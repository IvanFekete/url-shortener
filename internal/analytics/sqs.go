package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

type Producer interface {
	Send(context.Context, store.Event) error
}
type Queue struct {
	Client      *sqs.Client
	URL, DLQURL string
	config      config.Config
	metrics     *telemetry.Metrics
}

func New(ctx context.Context, c config.Config, m *telemetry.Metrics) (*Queue, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = c.MaxRequests + c.Workers
	transport.MaxConnsPerHost = c.MaxRequests + c.Workers
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region), awsconfig.WithHTTPClient(&http.Client{Transport: transport}), awsconfig.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 3; o.MaxBackoff = 200 * time.Millisecond })
	}))
	if err != nil {
		return nil, err
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if c.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(c.SQSEndpoint)
		}
	})
	return &Queue{Client: client, URL: c.QueueURL, DLQURL: c.DLQURL, config: c, metrics: m}, nil
}

// Resolve discovers pre-provisioned queues. It never creates infrastructure.
func (q *Queue) Resolve(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, q.config.RequestTimeout)
	defer cancel()
	for _, item := range []struct {
		name   string
		target *string
	}{{q.config.QueueName, &q.URL}, {q.config.DLQName, &q.DLQURL}} {
		if *item.target != "" {
			continue
		}
		out, err := q.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(item.name)})
		if err != nil {
			return err
		}
		*item.target = aws.ToString(out.QueueUrl)
	}
	return nil
}
func (q *Queue) Send(ctx context.Context, event store.Event) (err error) {
	start := time.Now()
	defer func() {
		q.metrics.Observe("sqs", "SendMessage", start, err, store.ErrorClass(err))
		if err != nil {
			q.metrics.Dropped.Inc()
			q.metrics.Events.WithLabelValues("send_failed").Inc()
		} else {
			q.metrics.Events.WithLabelValues("accepted").Inc()
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, q.config.EnqueueTimeout)
	defer cancel()
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// SDK retries reuse this body and therefore the application event ID.
	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(q.URL), MessageBody: aws.String(string(body))})
	return err
}
func (q *Queue) Receive(ctx context.Context, count int) (messages []types.Message, err error) {
	start := time.Now()
	defer func() { q.metrics.Observe("sqs", "ReceiveMessage", start, err, store.ErrorClass(err)) }()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	out, err := q.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q.URL), MaxNumberOfMessages: int32(min(count, 10)), WaitTimeSeconds: 20, VisibilityTimeout: int32(q.config.Visibility / time.Second), MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameSentTimestamp}})
	if err != nil {
		return nil, err
	}
	q.metrics.Batches.Inc()
	return out.Messages, nil
}
func (q *Queue) Delete(ctx context.Context, receipt *string) (err error) {
	start := time.Now()
	defer func() { q.metrics.Observe("sqs", "DeleteMessage", start, err, store.ErrorClass(err)) }()
	ctx, cancel := context.WithTimeout(ctx, q.config.RequestTimeout)
	defer cancel()
	_, err = q.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: receipt})
	if err == nil {
		q.metrics.Events.WithLabelValues("deleted").Inc()
	}
	return err
}
func (q *Queue) Retry(ctx context.Context, receipt *string, delay time.Duration) (err error) {
	start := time.Now()
	defer func() { q.metrics.Observe("sqs", "ChangeMessageVisibility", start, err, store.ErrorClass(err)) }()
	ctx, cancel := context.WithTimeout(ctx, q.config.RequestTimeout)
	defer cancel()
	_, err = q.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(q.URL), ReceiptHandle: receipt, VisibilityTimeout: int32(delay / time.Second)})
	return err
}
func (q *Queue) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, q.config.RequestTimeout)
	defer cancel()
	_, err := q.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(q.URL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	return err
}
func (q *Queue) Monitor(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		q.sample(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (q *Queue) sample(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, q.config.RequestTimeout)
	defer cancel()
	for _, item := range []struct {
		url string
		dlq bool
	}{{q.URL, false}, {q.DLQURL, true}} {
		start := time.Now()
		out, err := q.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(item.url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible, types.QueueAttributeNameApproximateNumberOfMessagesDelayed}})
		q.metrics.Observe("sqs", "GetQueueAttributes", start, err, store.ErrorClass(err))
		if err != nil {
			continue
		}
		value := func(k string) float64 { v, _ := strconv.ParseFloat(out.Attributes[k], 64); return v }
		visible := value("ApproximateNumberOfMessages")
		pending := value("ApproximateNumberOfMessagesNotVisible")
		delayed := value("ApproximateNumberOfMessagesDelayed")
		if item.dlq {
			q.metrics.DLQ.Set(visible + pending + delayed)
		} else {
			q.metrics.Queue.Set(visible)
			q.metrics.Pending.Set(pending)
			q.metrics.Delayed.Set(delayed)
		}
	}
}
func (q *Queue) Setup(ctx context.Context) error {
	seconds := func(d time.Duration) string { return strconv.FormatInt(int64(d/time.Second), 10) }
	dlq, err := q.Client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.config.DLQName), Attributes: map[string]string{"MessageRetentionPeriod": seconds(q.config.DLQRetention), "SqsManagedSseEnabled": "true"}})
	if err != nil {
		return err
	}
	q.DLQURL = aws.ToString(dlq.QueueUrl)
	attrs, err := q.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		return err
	}
	policy, _ := json.Marshal(map[string]any{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": q.config.MaxAttempts})
	out, err := q.Client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.config.QueueName), Attributes: map[string]string{"MessageRetentionPeriod": seconds(q.config.Retention), "VisibilityTimeout": seconds(q.config.Visibility), "ReceiveMessageWaitTimeSeconds": "20", "RedrivePolicy": string(policy), "SqsManagedSseEnabled": "true"}})
	if err != nil {
		return fmt.Errorf("create analytics queue: %w", err)
	}
	q.URL = aws.ToString(out.QueueUrl)
	return nil
}
