package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func queueFixture(fn func(string, map[string]any) (string, error)) *Queue {
	m := telemetry.New()
	client := sqs.New(sqs.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, Retryer: aws.NopRetryer{}, HTTPClient: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		var b map[string]any
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			return nil, err
		}
		body, err := fn(strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS."), b)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}})
	return &Queue{Client: client, URL: "https://sqs.us-east-1.amazonaws.com/123456789012/test", DLQURL: "https://sqs.us-east-1.amazonaws.com/123456789012/dlq", metrics: m, config: config.Config{RequestTimeout: time.Second, EnqueueTimeout: time.Second, Visibility: 30 * time.Second}}
}

type applyStub struct {
	calls     int
	duplicate bool
	err       error
}

func (s *applyStub) Create(context.Context, store.Link) error         { return nil }
func (s *applyStub) Get(context.Context, string) (*store.Link, error) { return nil, nil }
func (s *applyStub) Clicks(context.Context, string) (int64, error)    { return 0, nil }
func (s *applyStub) Ready(context.Context) error                      { return nil }
func (s *applyStub) Apply(context.Context, store.Event) (bool, error) {
	s.calls++
	return s.duplicate, s.err
}
func validEvent() store.Event {
	return store.Event{ID: "01234567-89ab-4cde-8012-3456789abcde", Code: "Abc0123456", At: time.Now().Add(-time.Second), Status: 302}
}
func TestWorkerAcknowledgesOnlyCommittedEvents(t *testing.T) {
	for _, tc := range []struct {
		name      string
		duplicate bool
		err       error
		operation string
		throttled bool
	}{{"commit", false, nil, "DeleteMessage", false}, {"duplicate", true, nil, "DeleteMessage", false}, {"failure", false, errors.New("offline"), "ChangeMessageVisibility", false}, {"throttle", false, &smithy.GenericAPIError{Code: "ThrottlingException"}, "ChangeMessageVisibility", true}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			q := queueFixture(func(op string, b map[string]any) (string, error) {
				calls++
				assert.Equal(t, tc.operation, op)
				assert.Equal(t, "receipt", b["ReceiptHandle"])
				if tc.err != nil {
					assert.GreaterOrEqual(t, b["VisibilityTimeout"].(float64), float64(4))
					assert.Less(t, b["VisibilityTimeout"].(float64), float64(8))
				}
				return `{}`, nil
			})
			s := &applyStub{duplicate: tc.duplicate, err: tc.err}
			w := &Worker{Queue: q, Store: s, Config: config.Config{RequestTimeout: time.Second, MaxAttempts: 3}, Metrics: q.metrics, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			body, err := json.Marshal(validEvent())
			require.NoError(t, err)
			throttled := w.process(types.Message{Body: aws.String(string(body)), ReceiptHandle: aws.String("receipt"), Attributes: map[string]string{"ApproximateReceiveCount": "3"}})
			assert.Equal(t, tc.throttled, throttled)
			assert.Equal(t, 1, s.calls)
			assert.Equal(t, 1, calls)
		})
	}
}
func TestWorkerRejectsMalformedEvents(t *testing.T) {
	for _, kind := range []string{"json", "uuid", "code", "status", "zero time", "future"} {
		t.Run(kind, func(t *testing.T) {
			e := validEvent()
			switch kind {
			case "uuid":
				e.ID = "bad"
			case "code":
				e.Code = "bad"
			case "status":
				e.Status = 200
			case "zero time":
				e.At = time.Time{}
			case "future":
				e.At = time.Now().Add(time.Hour)
			}
			body, err := json.Marshal(e)
			require.NoError(t, err)
			if kind == "json" {
				body = []byte("{")
			}
			calls := 0
			q := queueFixture(func(op string, b map[string]any) (string, error) {
				calls++
				assert.Equal(t, "ChangeMessageVisibility", op)
				assert.EqualValues(t, 1, b["VisibilityTimeout"])
				return `{}`, nil
			})
			s := &applyStub{}
			w := &Worker{Queue: q, Store: s, Config: config.Config{RequestTimeout: time.Second, MaxAttempts: 10}, Metrics: q.metrics, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			assert.False(t, w.process(types.Message{Body: aws.String(string(body)), ReceiptHandle: aws.String("receipt")}))
			assert.Zero(t, s.calls)
			assert.Equal(t, 1, calls)
		})
	}
}
func TestWorkerDeleteFailureLeavesCommittedEventForRedelivery(t *testing.T) {
	q := queueFixture(func(op string, _ map[string]any) (string, error) {
		assert.Equal(t, "DeleteMessage", op)
		return "", errors.New("delete unavailable")
	})
	s := &applyStub{}
	w := &Worker{Queue: q, Store: s, Config: config.Config{RequestTimeout: time.Second}, Metrics: q.metrics, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	body, err := json.Marshal(validEvent())
	require.NoError(t, err)
	msg := types.Message{Body: aws.String(string(body)), ReceiptHandle: aws.String("receipt")}
	assert.False(t, w.process(msg))
	s.duplicate = true
	assert.False(t, w.process(msg))
	assert.Equal(t, 2, s.calls)
}
func TestQueueSendAndReceive(t *testing.T) {
	e := validEvent()
	operations := []string{}
	q := queueFixture(func(op string, b map[string]any) (string, error) {
		operations = append(operations, op)
		switch op {
		case "SendMessage":
			var got store.Event
			require.NoError(t, json.Unmarshal([]byte(b["MessageBody"].(string)), &got))
			assert.Equal(t, e.ID, got.ID)
			assert.Equal(t, e.Code, got.Code)
			return `{"MessageId":"id"}`, nil
		case "ReceiveMessage":
			assert.EqualValues(t, 10, b["MaxNumberOfMessages"])
			assert.EqualValues(t, 20, b["WaitTimeSeconds"])
			assert.EqualValues(t, 30, b["VisibilityTimeout"])
			return `{"Messages":[{"Body":"payload","ReceiptHandle":"receipt"}]}`, nil
		}
		return `{}`, nil
	})
	require.NoError(t, q.Send(context.Background(), e))
	messages, err := q.Receive(context.Background(), 99)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "payload", aws.ToString(messages[0].Body))
	assert.Equal(t, []string{"SendMessage", "ReceiveMessage"}, operations)
}
func TestQueueResolveAndReadiness(t *testing.T) {
	q := queueFixture(func(op string, b map[string]any) (string, error) {
		switch op {
		case "GetQueueUrl":
			return `{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/` + b["QueueName"].(string) + `"}`, nil
		case "GetQueueAttributes":
			return `{"Attributes":{"QueueArn":"arn:test"}}`, nil
		default:
			t.Errorf("unexpected operation %s", op)
			return `{}`, nil
		}
	})
	q.URL = ""
	q.DLQURL = ""
	q.config.QueueName = "events"
	q.config.DLQName = "dead"
	require.NoError(t, q.Resolve(context.Background()))
	assert.Contains(t, q.URL, "/events")
	assert.Contains(t, q.DLQURL, "/dead")
	require.NoError(t, q.Ready(context.Background()))
}
func TestWorkerCanceledBeforePolling(t *testing.T) {
	q := queueFixture(func(string, map[string]any) (string, error) { t.Error("polled after cancellation"); return `{}`, nil })
	w := &Worker{Queue: q, Config: config.Config{Workers: 2, BatchSize: 2}, Metrics: q.metrics}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctx)
	assert.False(t, pause(ctx, time.Hour))
}
func TestWorkerRunStopsAfterCurrentBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body, err := json.Marshal(validEvent())
	require.NoError(t, err)
	response, err := json.Marshal(map[string]any{"Messages": []map[string]string{{"Body": string(body), "ReceiptHandle": "receipt"}}})
	require.NoError(t, err)
	var mu sync.Mutex
	ops := []string{}
	q := queueFixture(func(op string, b map[string]any) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, op)
		if op == "ReceiveMessage" {
			assert.EqualValues(t, 2, b["MaxNumberOfMessages"])
			cancel()
			return string(response), nil
		}
		return `{}`, nil
	})
	s := &applyStub{}
	w := &Worker{Queue: q, Store: s, Config: config.Config{Workers: 8, BatchSize: 2, RequestTimeout: time.Second}, Metrics: q.metrics, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w.Run(ctx)
	assert.Equal(t, 1, s.calls)
	assert.Equal(t, []string{"ReceiveMessage", "DeleteMessage"}, ops)
}
