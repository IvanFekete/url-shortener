package cache

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

type Cache interface {
	Get(context.Context, string) (*store.Link, bool)
	Put(context.Context, string, *store.Link)
}
type Redis struct {
	client  *redis.Client
	timeout time.Duration
	until   atomic.Int64
	metrics *telemetry.Metrics
}

func New(c config.Config, m *telemetry.Metrics) (*Redis, error) {
	o, err := redis.ParseURL(c.CacheURL)
	if err != nil {
		return nil, err
	}
	o.DialTimeout = c.RedisTimeout
	o.ReadTimeout = c.RedisTimeout
	o.WriteTimeout = c.RedisTimeout
	o.PoolTimeout = c.RedisTimeout
	o.ContextTimeoutEnabled = true
	o.MaxRetries = -1
	o.PoolSize = c.MaxRequests
	o.MaxActiveConns = c.MaxRequests
	return &Redis{client: redis.NewClient(o), timeout: c.RedisTimeout, metrics: m}, nil
}
func (r *Redis) Close() error { return r.client.Close() }
func (r *Redis) available() bool {
	if time.Now().UnixNano() < r.until.Load() {
		r.metrics.Cache.WithLabelValues("circuit_open").Inc()
		return false
	}
	return true
}
func (r *Redis) observe(op string, start time.Time, err error) {
	if err == redis.Nil {
		err = nil
	}
	r.metrics.Observe("redis", op, start, err, "unavailable")
	if err != nil {
		r.until.Store(time.Now().Add(2 * time.Second).UnixNano())
		r.metrics.Cache.WithLabelValues("error").Inc()
	}
}
func (r *Redis) Get(ctx context.Context, code string) (*store.Link, bool) {
	if !r.available() {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	start := time.Now()
	v, err := r.client.Get(ctx, "link:"+code).Bytes()
	r.observe("GET", start, err)
	if err != nil {
		r.metrics.Cache.WithLabelValues("miss").Inc()
		return nil, false
	}
	if string(v) == "null" {
		r.metrics.Cache.WithLabelValues("negative_hit").Inc()
		return nil, true
	}
	var link store.Link
	if json.Unmarshal(v, &link) != nil || link.Code != code || link.URL == "" || link.ExpiresAt.IsZero() {
		r.metrics.Cache.WithLabelValues("miss").Inc()
		return nil, false
	}
	r.metrics.Cache.WithLabelValues("hit").Inc()
	return &link, true
}
func (r *Redis) Put(ctx context.Context, code string, link *store.Link) {
	if !r.available() {
		return
	}
	ttl := 30 * time.Second
	if link != nil {
		ttl = time.Duration(float64(24*time.Hour) * (0.9 + rand.Float64()*0.1))
		ttl = min(ttl, time.Until(link.ExpiresAt))
		if ttl < time.Millisecond {
			return
		}
	}
	b, err := json.Marshal(link)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	start := time.Now()
	if link == nil {
		err = r.client.SetNX(ctx, "link:"+code, b, ttl).Err()
	} else {
		err = r.client.Set(ctx, "link:"+code, b, ttl).Err()
	}
	r.observe("SET", start, err)
}
