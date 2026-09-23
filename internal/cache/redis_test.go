package cache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

type commandHook struct{ run func(redis.Cmder) error }

func (h commandHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(c context.Context, n, a string) (net.Conn, error) { return next(c, n, a) }
}
func (h commandHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, c redis.Cmder) error { return h.run(c) }
}
func (h commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func cacheFixture(t *testing.T, run func(redis.Cmder) error) *Redis {
	t.Helper()
	r, err := New(config.Config{CacheURL: "redis://localhost:6379", RedisTimeout: time.Second, MaxRequests: 2}, telemetry.New())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	r.client.AddHook(commandHook{run})
	return r
}
func TestGetValidation(t *testing.T) {
	link := store.Link{Code: "Abc0123456", URL: "https://example.com", ExpiresAt: time.Now().Add(time.Hour)}
	b, err := json.Marshal(link)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, value   string
		err           error
		hit, positive bool
	}{{"positive", string(b), nil, true, true}, {"negative", "null", nil, true, false}, {"absent", "", redis.Nil, false, false}, {"corrupt", "{", nil, false, false}, {"wrong code", `{"code":"Other12345","url":"https://example.com","expires_at":"2027-01-01T00:00:00Z"}`, nil, false, false}, {"incomplete", `{"code":"Abc0123456"}`, nil, false, false}, {"outage", "", errors.New("offline"), false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			r := cacheFixture(t, func(c redis.Cmder) error {
				assert.Equal(t, []interface{}{"get", "link:Abc0123456"}, c.Args())
				c.(*redis.StringCmd).SetVal(tc.value)
				return tc.err
			})
			l, hit := r.Get(context.Background(), link.Code)
			assert.Equal(t, tc.hit, hit)
			assert.Equal(t, tc.positive, l != nil)
			assert.Equal(t, tc.err != nil && tc.err != redis.Nil, r.until.Load() > 0)
		})
	}
}
func TestPutTTLAndNegativeCache(t *testing.T) {
	for _, kind := range []string{"negative", "short", "long", "expired"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			r := cacheFixture(t, func(c redis.Cmder) error {
				calls++
				args := c.Args()
				assert.Equal(t, "set", args[0])
				assert.Equal(t, "link:Abc0123456", args[1])
				ttl := time.Duration(args[4].(int64))
				if args[3] == "px" {
					ttl *= time.Millisecond
				} else {
					assert.Equal(t, "ex", args[3])
					ttl *= time.Second
				}
				switch kind {
				case "negative":
					assert.Equal(t, 30*time.Second, ttl)
					assert.Contains(t, args, "nx")
				case "short":
					assert.Greater(t, ttl, time.Duration(0))
					assert.LessOrEqual(t, ttl, time.Minute)
				case "long":
					assert.GreaterOrEqual(t, ttl, 21*time.Hour)
					assert.LessOrEqual(t, ttl, 24*time.Hour)
				}
				return nil
			})
			var l *store.Link
			if kind != "negative" {
				expiry := time.Now().Add(time.Minute)
				if kind == "long" {
					expiry = time.Now().Add(48 * time.Hour)
				}
				if kind == "expired" {
					expiry = time.Now().Add(-time.Second)
				}
				l = &store.Link{Code: "Abc0123456", URL: "https://example.com", ExpiresAt: expiry}
			}
			r.Put(context.Background(), "Abc0123456", l)
			if kind == "expired" {
				assert.Zero(t, calls)
			} else {
				assert.Equal(t, 1, calls)
			}
		})
	}
}
func TestCircuitCooldown(t *testing.T) {
	calls := 0
	r := cacheFixture(t, func(redis.Cmder) error { calls++; return errors.New("offline") })
	r.Get(context.Background(), "Abc0123456")
	r.Get(context.Background(), "Abc0123456")
	r.Put(context.Background(), "Abc0123456", nil)
	assert.Equal(t, 1, calls)
	r.until.Store(time.Now().Add(-time.Second).UnixNano())
	r.Get(context.Background(), "Abc0123456")
	assert.Equal(t, 2, calls)
}
func TestInvalidRedisURL(t *testing.T) {
	_, err := New(config.Config{CacheURL: "http://example.com"}, telemetry.New())
	require.Error(t, err)
}
