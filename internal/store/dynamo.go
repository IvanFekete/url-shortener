package store

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"url-shortener/internal/config"
	"url-shortener/internal/telemetry"
)

const Shards = 16

var ErrCollision = errors.New("code collision")

type Link struct {
	Code      string    `json:"code"`
	URL       string    `json:"url"`
	Token     string    `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}
type Event struct {
	ID          string    `json:"event_id"`
	Code        string    `json:"code"`
	At          time.Time `json:"redirected_at"`
	Status      int       `json:"status"`
	TraceParent string    `json:"traceparent,omitempty"`
}
type Repository interface {
	Create(context.Context, Link) error
	Get(context.Context, string) (*Link, error)
	Clicks(context.Context, string) (int64, error)
	Apply(context.Context, Event) (bool, error)
	Ready(context.Context) error
}
type Dynamo struct {
	Client  *dynamodb.Client
	Config  config.Config
	Metrics *telemetry.Metrics
	slots   chan struct{}
}

func New(ctx context.Context, c config.Config, m *telemetry.Metrics) (*Dynamo, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = c.MaxRequests + c.Workers
	transport.MaxIdleConnsPerHost = c.MaxRequests + c.Workers
	transport.MaxConnsPerHost = c.MaxRequests + c.Workers
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region), awsconfig.WithHTTPClient(&http.Client{Transport: transport}), awsconfig.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 3; o.MaxBackoff = 200 * time.Millisecond })
	}))
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if c.DynamoEndpoint != "" {
			o.BaseEndpoint = aws.String(c.DynamoEndpoint)
		}
	})
	return &Dynamo{Client: client, Config: c, Metrics: m, slots: make(chan struct{}, c.MaxRequests+c.Workers)}, nil
}
func (d *Dynamo) begin(ctx context.Context) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, d.Config.RequestTimeout)
	select {
	case d.slots <- struct{}{}:
		return ctx, func() { <-d.slots; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return ctx, func() {}, ctx.Err()
	}
}
func ErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded":
			return "throttle"
		case "TransactionConflictException":
			return "conflict"
		case "ConditionalCheckFailedException":
			return "condition"
		case "ValidationException":
			return "validation"
		}
	}
	var canceled *types.TransactionCanceledException
	if errors.As(err, &canceled) {
		for _, reason := range canceled.CancellationReasons {
			switch aws.ToString(reason.Code) {
			case "ThrottlingError", "ProvisionedThroughputExceeded":
				return "throttle"
			case "TransactionConflict":
				return "conflict"
			case "ValidationError":
				return "validation"
			}
		}
		return "transaction"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "unavailable"
}
func (d *Dynamo) observe(op string, start time.Time, err error) {
	d.Metrics.Observe("dynamodb", op, start, err, ErrorClass(err))
}
func (d *Dynamo) capacity(c *types.ConsumedCapacity, kind string) {
	if c == nil {
		return
	}
	table := aws.ToString(c.TableName)
	if c.ReadCapacityUnits == nil && c.WriteCapacityUnits == nil {
		d.Metrics.Capacity.WithLabelValues(table, kind).Add(aws.ToFloat64(c.CapacityUnits))
		return
	}
	d.Metrics.Capacity.WithLabelValues(table, "read").Add(aws.ToFloat64(c.ReadCapacityUnits))
	d.Metrics.Capacity.WithLabelValues(table, "write").Add(aws.ToFloat64(c.WriteCapacityUnits))
}
func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func str(item map[string]types.AttributeValue, key string) string {
	if v, ok := item[key].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}
func key(name, value string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{name: s(value)}
}

func (d *Dynamo) Create(ctx context.Context, l Link) (err error) {
	start := time.Now()
	defer func() { d.observe("PutItem", start, err) }()
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	// Reserve part of the request budget for ambiguous-write verification.
	deadline, _ := ctx.Deadline()
	writeCtx, writeCancel := context.WithTimeout(ctx, time.Until(deadline)*2/3)
	out, err := d.Client.PutItem(writeCtx, &dynamodb.PutItemInput{TableName: aws.String(d.Config.LinksTable), ConditionExpression: aws.String("attribute_not_exists(code)"), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, Item: map[string]types.AttributeValue{
		"code": s(l.Code), "creation_token": s(l.Token), "target_url": s(l.URL), "created_at": s(l.CreatedAt.Format(time.RFC3339Nano)), "expires_at": s(l.ExpiresAt.Format(time.RFC3339Nano)), "ttl": n(l.ExpiresAt.Unix()),
	}})
	writeCancel()
	if err == nil {
		d.capacity(out.ConsumedCapacity, "write")
		return nil
	}
	// A retry may encounter the original successful write. Only its token proves ownership.
	existing, readErr := d.get(ctx, l.Code)
	if readErr == nil && existing != nil {
		if existing.Token == l.Token {
			return nil
		}
		return ErrCollision
	}
	return err
}
func (d *Dynamo) Get(ctx context.Context, code string) (l *Link, err error) {
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return d.get(ctx, code)
}
func (d *Dynamo) get(ctx context.Context, code string) (l *Link, err error) {
	start := time.Now()
	defer func() { d.observe("GetItem", start, err) }()
	out, err := d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(d.Config.LinksTable), Key: key("code", code), ConsistentRead: aws.Bool(true), ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	if err != nil {
		return nil, err
	}
	d.capacity(out.ConsumedCapacity, "read")
	if len(out.Item) == 0 {
		return nil, nil
	}
	expires, err := time.Parse(time.RFC3339Nano, str(out.Item, "expires_at"))
	if err != nil {
		return nil, fmt.Errorf("invalid stored expiry: %w", err)
	}
	created, err := time.Parse(time.RFC3339Nano, str(out.Item, "created_at"))
	if err != nil {
		return nil, err
	}
	return &Link{Code: code, URL: str(out.Item, "target_url"), Token: str(out.Item, "creation_token"), CreatedAt: created, ExpiresAt: expires}, nil
}
func (d *Dynamo) Clicks(ctx context.Context, code string) (total int64, err error) {
	start := time.Now()
	defer func() { d.observe("BatchGetItem", start, err) }()
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	keys := make([]map[string]types.AttributeValue, Shards)
	for i := range keys {
		keys[i] = key("code_shard", fmt.Sprintf("%s#%d", code, i))
	}
	request := map[string]types.KeysAndAttributes{d.Config.StatsTable: {Keys: keys, ConsistentRead: aws.Bool(true)}}
	for attempt := 0; attempt < 4; attempt++ {
		out, e := d.Client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: request, ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
		if e != nil {
			return 0, e
		}
		for _, c := range out.ConsumedCapacity {
			d.capacity(&c, "read")
		}
		for _, item := range out.Responses[d.Config.StatsTable] {
			if v, ok := item["clicks"].(*types.AttributeValueMemberN); ok {
				x, e := strconv.ParseInt(v.Value, 10, 64)
				if e != nil {
					return 0, e
				}
				total += x
			}
		}
		request = out.UnprocessedKeys
		if len(request) == 0 {
			return total, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(10+rand.IntN(20*(1<<attempt))) * time.Millisecond):
		}
	}
	return 0, errors.New("stats read left unprocessed keys")
}
func (d *Dynamo) Apply(ctx context.Context, e Event) (duplicate bool, err error) {
	start := time.Now()
	defer func() { d.observe("TransactWriteItems", start, err) }()
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	h := fnv.New32a()
	_, _ = h.Write([]byte(e.ID))
	shard := h.Sum32() % Shards
	out, err := d.Client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String(d.Config.EventsTable), ConditionExpression: aws.String("attribute_not_exists(event_id)"), Item: map[string]types.AttributeValue{"event_id": s(e.ID), "processed_at": s(time.Now().UTC().Format(time.RFC3339Nano)), "ttl": n(time.Now().Add(d.Config.DedupeTTL).Unix())}}},
		{Update: &types.Update{TableName: aws.String(d.Config.StatsTable), Key: key("code_shard", fmt.Sprintf("%s#%d", e.Code, shard)), UpdateExpression: aws.String("SET #code = :code, #shard = :shard, last_clicked_at = :at ADD clicks :one"), ExpressionAttributeNames: map[string]string{"#code": "code", "#shard": "shard"}, ExpressionAttributeValues: map[string]types.AttributeValue{":code": s(e.Code), ":shard": n(int64(shard)), ":at": s(e.At.Format(time.RFC3339Nano)), ":one": n(1)}}},
	}})
	if err == nil {
		for _, c := range out.ConsumedCapacity {
			d.capacity(&c, "write")
		}
		return false, nil
	}
	// Verification also resolves an ambiguous network response after a successful commit.
	check, checkErr := d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(d.Config.EventsTable), Key: key("event_id", e.ID), ConsistentRead: aws.Bool(true)})
	if checkErr == nil && len(check.Item) > 0 {
		return true, nil
	}
	return false, err
}
func (d *Dynamo) Ready(ctx context.Context) error {
	ctx, done, err := d.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	for _, table := range []string{d.Config.LinksTable, d.Config.StatsTable, d.Config.EventsTable} {
		out, err := d.Client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
		if err != nil {
			return err
		}
		if out.Table.TableStatus != types.TableStatusActive {
			return errors.New("table not active")
		}
	}
	return nil
}

// Setup is called only by the explicit provisioning command, never by API replicas.
func (d *Dynamo) Setup(ctx context.Context) error {
	for _, t := range []struct {
		name, key string
		ttl       bool
	}{{d.Config.LinksTable, "code", true}, {d.Config.StatsTable, "code_shard", false}, {d.Config.EventsTable, "event_id", true}} {
		_, err := d.Client.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String(t.name), BillingMode: types.BillingModePayPerRequest, AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String(t.key), AttributeType: types.ScalarAttributeTypeS}}, KeySchema: []types.KeySchemaElement{{AttributeName: aws.String(t.key), KeyType: types.KeyTypeHash}}})
		var exists *types.ResourceInUseException
		if err != nil && !errors.As(err, &exists) {
			return err
		}
		if err := dynamodb.NewTableExistsWaiter(d.Client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(t.name)}, 2*time.Minute); err != nil {
			return err
		}
		if t.ttl {
			out, err := d.Client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(t.name)})
			if err != nil {
				return err
			}
			if out.TimeToLiveDescription.TimeToLiveStatus == types.TimeToLiveStatusDisabled {
				_, err = d.Client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: aws.String(t.name), TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ttl"), Enabled: aws.Bool(true)}})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
