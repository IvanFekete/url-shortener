package config

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func cleanEnv(t *testing.T) {
	t.Helper()
	for _, k := range strings.Fields("LISTEN_ADDR BASE_URL AWS_REGION DYNAMODB_ENDPOINT LINKS_TABLE STATS_TABLE EVENTS_TABLE CACHE_REDIS_URL SQS_ENDPOINT SQS_QUEUE_URL SQS_QUEUE_NAME SQS_DLQ_NAME SQS_DLQ_URL LINK_TTL REQUEST_TIMEOUT REDIS_TIMEOUT SQS_ENQUEUE_TIMEOUT SQS_VISIBILITY_TIMEOUT SQS_RETENTION SQS_DLQ_RETENTION DEDUPE_TTL MAX_REQUESTS WORKER_CONCURRENCY WORKER_BATCH_SIZE MAX_EVENT_ATTEMPTS MAX_URL_LENGTH") {
		t.Setenv(k, "")
	}
}
func TestDefaults(t *testing.T) {
	cleanEnv(t)
	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 30*24*time.Hour, c.LinkTTL)
	assert.Equal(t, 256, c.MaxRequests)
	assert.Equal(t, 8, c.BatchSize)
	assert.Equal(t, "http://localhost:8080", c.BaseURL)
	assert.Greater(t, c.DedupeTTL, c.Retention+2*c.DLQRetention)
}
func TestInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ k, v string }{{"LINK_TTL", "30d"}, {"REQUEST_TIMEOUT", "0s"}, {"REDIS_TIMEOUT", "-1s"}, {"MAX_REQUESTS", "0"}, {"WORKER_CONCURRENCY", "100001"}, {"MAX_URL_LENGTH", "abc"}, {"WORKER_BATCH_SIZE", "11"}, {"SQS_VISIBILITY_TIMEOUT", "1s"}, {"SQS_VISIBILITY_TIMEOUT", "30.5s"}, {"SQS_VISIBILITY_TIMEOUT", "13h"}, {"SQS_RETENTION", "30s"}, {"SQS_RETENTION", "337h"}, {"SQS_DLQ_RETENTION", "1h"}, {"DEDUPE_TTL", "768h"}, {"BASE_URL", "ftp://example.com"}, {"BASE_URL", "https://u:p@example.com"}, {"BASE_URL", "https://example.com/path"}, {"BASE_URL", "https://example.com?q=1"}, {"BASE_URL", "https://example.com#x"}} {
		t.Run(tc.k+"/"+tc.v, func(t *testing.T) { cleanEnv(t); t.Setenv(tc.k, tc.v); _, err := Load(); require.Error(t, err) })
	}
}
func TestOverrides(t *testing.T) {
	cleanEnv(t)
	t.Setenv("BASE_URL", "https://short.example/")
	t.Setenv("LINK_TTL", "1h")
	t.Setenv("MAX_REQUESTS", "32")
	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "https://short.example/", c.BaseURL)
	assert.Equal(t, time.Hour, c.LinkTTL)
	assert.Equal(t, 32, c.MaxRequests)
}
