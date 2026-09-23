package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Listen, BaseURL, Region, DynamoEndpoint                                                               string
	LinksTable, StatsTable, EventsTable                                                                   string
	CacheURL, SQSEndpoint, QueueURL, QueueName, DLQName, DLQURL                                           string
	LinkTTL, RequestTimeout, RedisTimeout, EnqueueTimeout, Visibility, Retention, DLQRetention, DedupeTTL time.Duration
	MaxRequests, Workers, BatchSize, MaxAttempts, MaxURLLength                                            int
}

func Load() (Config, error) {
	c := Config{
		Listen: env("LISTEN_ADDR", ":8080"), BaseURL: env("BASE_URL", "http://localhost:8080"),
		Region: env("AWS_REGION", "us-east-1"), DynamoEndpoint: os.Getenv("DYNAMODB_ENDPOINT"),
		LinksTable: env("LINKS_TABLE", "Links"), StatsTable: env("STATS_TABLE", "LinkStats"), EventsTable: env("EVENTS_TABLE", "ProcessedEvents"),
		CacheURL: env("CACHE_REDIS_URL", "redis://localhost:6379"), SQSEndpoint: os.Getenv("SQS_ENDPOINT"), QueueURL: os.Getenv("SQS_QUEUE_URL"),
		QueueName: env("SQS_QUEUE_NAME", "redirects"), DLQName: env("SQS_DLQ_NAME", "redirects-dlq"), DLQURL: os.Getenv("SQS_DLQ_URL"),
	}
	for _, item := range []struct {
		name, fallback string
		target         *time.Duration
	}{
		{"LINK_TTL", "720h", &c.LinkTTL}, {"REQUEST_TIMEOUT", "2s", &c.RequestTimeout},
		{"REDIS_TIMEOUT", "50ms", &c.RedisTimeout}, {"SQS_ENQUEUE_TIMEOUT", "50ms", &c.EnqueueTimeout}, {"SQS_VISIBILITY_TIMEOUT", "30s", &c.Visibility},
		{"SQS_RETENTION", "96h", &c.Retention}, {"SQS_DLQ_RETENTION", "336h", &c.DLQRetention}, {"DEDUPE_TTL", "1080h", &c.DedupeTTL},
	} {
		v, err := time.ParseDuration(env(item.name, item.fallback))
		if err != nil || v <= 0 {
			return c, fmt.Errorf("invalid %s", item.name)
		}
		*item.target = v
	}
	for _, item := range []struct {
		name, fallback string
		target         *int
	}{
		{"MAX_REQUESTS", "256", &c.MaxRequests}, {"WORKER_CONCURRENCY", "8", &c.Workers},
		{"WORKER_BATCH_SIZE", "8", &c.BatchSize}, {"MAX_EVENT_ATTEMPTS", "10", &c.MaxAttempts}, {"MAX_URL_LENGTH", "2048", &c.MaxURLLength},
	} {
		v, err := strconv.Atoi(env(item.name, item.fallback))
		if err != nil || v <= 0 || v > 100000 {
			return c, fmt.Errorf("invalid %s", item.name)
		}
		*item.target = v
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return c, fmt.Errorf("BASE_URL must be an HTTP(S) origin")
	}
	if c.BatchSize > 10 || c.Visibility < 3*c.RequestTimeout || c.Visibility > 12*time.Hour || c.Visibility%time.Second != 0 {
		return c, fmt.Errorf("batch size must be <=10; visibility must be whole seconds, >=3*REQUEST_TIMEOUT and <=12h")
	}
	if c.Retention < time.Minute || c.Retention > 14*24*time.Hour || c.DLQRetention < c.Retention || c.DLQRetention > 14*24*time.Hour || c.DedupeTTL <= c.Retention+2*c.DLQRetention {
		return c, fmt.Errorf("SQS retention must be 1m..336h, DLQ retention >= queue retention and <=336h, dedupe TTL > queue retention + 2*DLQ retention")
	}
	return c, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
