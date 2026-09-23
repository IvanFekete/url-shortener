package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"url-shortener/internal/config"
	"url-shortener/internal/telemetry"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type exchange struct {
	op     string
	status int
	body   string
	check  func(map[string]any)
}

func scriptedDynamo(t *testing.T, steps ...exchange) *Dynamo {
	t.Helper()
	calls := 0
	client := dynamodb.New(dynamodb.Options{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, Retryer: aws.NopRetryer{}, HTTPClient: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		require.Less(t, calls, len(steps), "unexpected request")
		step := steps[calls]
		calls++
		assert.Equal(t, "DynamoDB_20120810."+step.op, r.Header.Get("X-Amz-Target"))
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		if step.check != nil {
			step.check(body)
		}
		status := step.status
		if status == 0 {
			status = 200
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}}, Body: io.NopCloser(strings.NewReader(step.body))}, nil
	})}})
	t.Cleanup(func() { assert.Equal(t, len(steps), calls, "unused scripted requests") })
	return &Dynamo{Client: client, Config: config.Config{LinksTable: "Links", StatsTable: "Stats", EventsTable: "Events", RequestTimeout: time.Second, DedupeTTL: 45 * 24 * time.Hour}, Metrics: telemetry.New(), slots: make(chan struct{}, 2)}
}

const storedItem = `{"Item":{"target_url":{"S":"https://example.com"},"creation_token":{"S":"owner"},"created_at":{"S":"2026-01-01T00:00:00Z"},"expires_at":{"S":"2026-02-01T00:00:00Z"}}}`

func TestGet(t *testing.T) {
	for _, tc := range []struct {
		name, body   string
		missing, bad bool
	}{{"found", storedItem, false, false}, {"missing", `{}`, true, false}, {"corrupt", `{"Item":{"expires_at":{"S":"bad"}}}`, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			d := scriptedDynamo(t, exchange{op: "GetItem", body: tc.body, check: func(b map[string]any) {
				assert.Equal(t, true, b["ConsistentRead"])
				assert.Equal(t, "Links", b["TableName"])
			}})
			l, err := d.Get(context.Background(), "Abc0123456")
			if tc.bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.missing {
					assert.Nil(t, l)
				} else {
					require.NotNil(t, l)
					assert.Equal(t, "owner", l.Token)
					assert.Equal(t, "https://example.com", l.URL)
				}
			}
		})
	}
}
func TestCreateAmbiguousWrite(t *testing.T) {
	for _, token := range []string{"owner", "different"} {
		t.Run(token, func(t *testing.T) {
			d := scriptedDynamo(t, exchange{op: "PutItem", status: 400, body: `{"__type":"ConditionalCheckFailedException","message":"exists"}`, check: func(b map[string]any) { assert.Equal(t, "attribute_not_exists(code)", b["ConditionExpression"]) }}, exchange{op: "GetItem", body: storedItem})
			err := d.Create(context.Background(), Link{Code: "Abc0123456", Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)})
			if token == "owner" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrCollision)
			}
		})
	}
}
func TestClicksRetriesOnlyUnprocessedKeys(t *testing.T) {
	d := scriptedDynamo(t, exchange{op: "BatchGetItem", body: `{"Responses":{"Stats":[{"clicks":{"N":"7"}}]},"UnprocessedKeys":{"Stats":{"Keys":[{"code_shard":{"S":"Abc0123456#1"}}],"ConsistentRead":true}}}`, check: func(b map[string]any) {
		r := b["RequestItems"].(map[string]any)["Stats"].(map[string]any)
		assert.Len(t, r["Keys"], Shards)
		assert.Equal(t, true, r["ConsistentRead"])
	}}, exchange{op: "BatchGetItem", body: `{"Responses":{"Stats":[{"clicks":{"N":"5"}}]}}`, check: func(b map[string]any) {
		assert.Len(t, b["RequestItems"].(map[string]any)["Stats"].(map[string]any)["Keys"], 1)
	}})
	n, err := d.Clicks(context.Background(), "Abc0123456")
	require.NoError(t, err)
	assert.EqualValues(t, 12, n)
}
func TestClicksRejectsIncompleteOrCorruptResults(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			steps := []exchange{}
			if corrupt {
				steps = append(steps, exchange{op: "BatchGetItem", body: `{"Responses":{"Stats":[{"clicks":{"N":"oops"}}]}}`})
			} else {
				for i := 0; i < 4; i++ {
					steps = append(steps, exchange{op: "BatchGetItem", body: `{"UnprocessedKeys":{"Stats":{"Keys":[{"code_shard":{"S":"x"}}]}}}`})
				}
			}
			n, err := scriptedDynamo(t, steps...).Clicks(context.Background(), "Abc0123456")
			require.Error(t, err)
			assert.Zero(t, n)
		})
	}
}
func TestApplyDeduplication(t *testing.T) {
	for _, tc := range []struct {
		name, verification string
		duplicate, wantErr bool
	}{{"committed", "", false, false}, {"verified duplicate", `{"Item":{"event_id":{"S":"id"}}}`, true, false}, {"uncommitted", `{}`, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			step := exchange{op: "TransactWriteItems", body: `{}`, check: func(b map[string]any) {
				items := b["TransactItems"].([]any)
				require.Len(t, items, 2)
				put := items[0].(map[string]any)["Put"].(map[string]any)
				assert.Equal(t, "Events", put["TableName"])
				assert.Equal(t, "attribute_not_exists(event_id)", put["ConditionExpression"])
				update := items[1].(map[string]any)["Update"].(map[string]any)
				assert.Equal(t, "Stats", update["TableName"])
				assert.Contains(t, update["UpdateExpression"], "ADD clicks :one")
			}}
			steps := []exchange{step}
			if tc.verification != "" {
				steps[0].status = 400
				steps[0].body = `{"__type":"TransactionCanceledException","message":"failed"}`
				steps = append(steps, exchange{op: "GetItem", body: tc.verification, check: func(b map[string]any) {
					assert.Equal(t, true, b["ConsistentRead"])
					assert.Equal(t, "Events", b["TableName"])
				}})
			}
			dup, err := scriptedDynamo(t, steps...).Apply(context.Background(), Event{ID: "id", Code: "Abc0123456", At: time.Now(), Status: 302})
			assert.Equal(t, tc.duplicate, dup)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
func TestErrorClass(t *testing.T) {
	for _, tc := range []struct {
		err   error
		class string
	}{{nil, "none"}, {fmt.Errorf("wrapped: %w", context.DeadlineExceeded), "timeout"}, {errors.New("offline"), "unavailable"}, {&smithy.GenericAPIError{Code: "ThrottlingException"}, "throttle"}, {&smithy.GenericAPIError{Code: "TransactionConflictException"}, "conflict"}, {&smithy.GenericAPIError{Code: "ConditionalCheckFailedException"}, "condition"}, {&smithy.GenericAPIError{Code: "ValidationException"}, "validation"}, {&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("ThrottlingError")}}}, "throttle"}} {
		assert.Equal(t, tc.class, ErrorClass(tc.err))
	}
}
func TestBeginHonorsCancellationWhenSaturated(t *testing.T) {
	d := &Dynamo{Config: config.Config{RequestTimeout: time.Second}, slots: make(chan struct{}, 1)}
	_, done, err := d.begin(context.Background())
	require.NoError(t, err)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = d.begin(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReadinessRequiresAllTablesActive(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			steps := []exchange{}
			tables := []string{"Links", "Stats", "Events"}
			if !active {
				tables = tables[:1]
			}
			for _, table := range tables {
				status := "ACTIVE"
				if !active {
					status = "CREATING"
				}
				steps = append(steps, exchange{op: "DescribeTable", body: `{"Table":{"TableStatus":"` + status + `"}}`, check: func(b map[string]any) { assert.Equal(t, table, b["TableName"]) }})
			}
			err := scriptedDynamo(t, steps...).Ready(context.Background())
			if active {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
