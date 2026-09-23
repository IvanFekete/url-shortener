package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"url-shortener/internal/analytics"
	"url-shortener/internal/api"
	"url-shortener/internal/cache"
	"url-shortener/internal/config"
	"url-shortener/internal/identity"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

func TestIntegrationServices(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("opt in with RUN_INTEGRATION=1; see README")
	}
	endpoint := func(key string) string {
		v := os.Getenv(key)
		require.NotEmpty(t, v, "set %s to an isolated test service", key)
		return v
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "local")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	id, err := identity.Code()
	require.NoError(t, err)
	prefix := "test-" + id
	c := config.Config{Region: "us-east-1", DynamoEndpoint: endpoint("INTEGRATION_DYNAMODB_URL"), SQSEndpoint: endpoint("INTEGRATION_SQS_URL"), CacheURL: endpoint("INTEGRATION_REDIS_URL"), LinksTable: prefix + "-links", StatsTable: prefix + "-stats", EventsTable: prefix + "-events", QueueName: prefix, DLQName: prefix + "-dlq", BaseURL: "https://short.example", MaxRequests: 16, Workers: 2, BatchSize: 2, MaxAttempts: 3, MaxURLLength: 2048, RequestTimeout: 2 * time.Second, RedisTimeout: time.Second, EnqueueTimeout: 2 * time.Second, Visibility: 10 * time.Second, LinkTTL: time.Hour, Retention: time.Hour, DLQRetention: 2 * time.Hour, DedupeTTL: 24 * time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	m := telemetry.New()
	repo, err := store.New(ctx, c, m)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, name := range []string{c.LinksTable, c.StatsTable, c.EventsTable} {
			_, err := repo.Client.DeleteTable(cleanup, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
			assert.NoError(t, err)
		}
	})
	require.NoError(t, repo.Setup(ctx))
	require.NoError(t, repo.Ready(ctx))
	q, err := analytics.New(ctx, c, m)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, u := range []string{q.URL, q.DLQURL} {
			if u != "" {
				_, err := q.Client.DeleteQueue(cleanup, &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
				assert.NoError(t, err)
			}
		}
	})
	require.NoError(t, q.Setup(ctx))
	require.NoError(t, q.Ready(ctx))
	cached, err := cache.New(c, m)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cached.Close()) })
	options, err := redis.ParseURL(c.CacheURL)
	require.NoError(t, err)
	rawCache := redis.NewClient(options)
	t.Cleanup(func() { assert.NoError(t, rawCache.Close()) })
	require.NoError(t, rawCache.Ping(ctx).Err())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := api.New(c, repo, cached, q, m, logger)
	server := httptest.NewServer(a.Handler())
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Post(server.URL+"/shorten", "application/json", strings.NewReader(`{"url":"https://example.com/target"}`))
	require.NoError(t, err)
	var created struct {
		Code string `json:"code"`
	}
	err = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 201, resp.StatusCode)
	require.True(t, identity.ValidCode(created.Code))
	t.Cleanup(func() { assert.NoError(t, rawCache.Del(context.Background(), "link:"+created.Code).Err()) })
	link, err := repo.Get(ctx, created.Code)
	require.NoError(t, err)
	require.NotNil(t, link)
	require.NoError(t, repo.Create(ctx, *link), "same creation token confirms an already committed write")
	different := *link
	different.Token = "other"
	require.ErrorIs(t, repo.Create(ctx, different), store.ErrCollision)
	cached.Put(ctx, created.Code, nil)
	hit, ok := cached.Get(ctx, created.Code)
	require.True(t, ok)
	require.NotNil(t, hit, "negative cache cannot overwrite a positive link")
	require.NoError(t, rawCache.Set(ctx, "link:"+created.Code, "corrupt", time.Minute).Err())
	resp, err = client.Get(server.URL + "/" + created.Code)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 302, resp.StatusCode)
	assert.Equal(t, link.URL, resp.Header.Get("Location"))
	hit, ok = cached.Get(ctx, created.Code)
	require.True(t, ok)
	require.NotNil(t, hit, "database fallback repairs corrupt cache")
	// Send the same event twice; the worker must acknowledge both but increment once.
	eventID, err := identity.UUID()
	require.NoError(t, err)
	event := store.Event{ID: eventID, Code: created.Code, At: time.Now().UTC(), Status: 302}
	require.NoError(t, q.Send(ctx, event))
	require.NoError(t, q.Send(ctx, event))
	workerCtx, stopWorker := context.WithCancel(context.Background())
	done := make(chan struct{})
	worker := analytics.Worker{Queue: q, Store: repo, Config: c, Metrics: m, Logger: logger}
	go func() { defer close(done); worker.Run(workerCtx) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
	require.Eventually(t, func() bool {
		families, err := m.Registry.Gather()
		if err != nil {
			return false
		}
		for _, family := range families {
			if family.GetName() == "analytics_events_total" {
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "result" && label.GetValue() == "deleted" && metric.GetCounter().GetValue() >= 3 {
							return true
						}
					}
				}
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond, "worker should acknowledge all three deliveries")
	clicks, err := repo.Clicks(ctx, created.Code)
	require.NoError(t, err)
	assert.EqualValues(t, 2, clicks)
	resp, err = client.Get(server.URL + "/stats/" + created.Code)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	assert.JSONEq(t, `{"url":"https://example.com/target","clicks":2}`, string(body))
	// Absolute expiry is enforced before an analytics event is emitted.
	expiredCode, err := identity.Code()
	require.NoError(t, err)
	expired := store.Link{Code: expiredCode, Token: "expired", URL: "https://example.com", CreatedAt: time.Now().Add(-2 * time.Hour), ExpiresAt: time.Now().Add(-time.Hour)}
	require.NoError(t, repo.Create(ctx, expired))
	resp, err = client.Get(server.URL + "/" + expiredCode)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Contains(t, []int{404, 410}, resp.StatusCode, "TTL cleanup may already have removed the expired row")
}
