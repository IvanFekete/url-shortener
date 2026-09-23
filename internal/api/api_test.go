package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"url-shortener/internal/config"
	"url-shortener/internal/identity"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

type repositoryStub struct {
	create   func(context.Context, store.Link) error
	get      func(context.Context, string) (*store.Link, error)
	clicks   func(context.Context, string) (int64, error)
	readyErr error
}

func (s *repositoryStub) Create(c context.Context, l store.Link) error {
	if s.create != nil {
		return s.create(c, l)
	}
	return nil
}
func (s *repositoryStub) Get(c context.Context, k string) (*store.Link, error) {
	if s.get != nil {
		return s.get(c, k)
	}
	return nil, nil
}
func (s *repositoryStub) Clicks(c context.Context, k string) (int64, error) {
	if s.clicks != nil {
		return s.clicks(c, k)
	}
	return 0, nil
}
func (s *repositoryStub) Apply(context.Context, store.Event) (bool, error) { return false, nil }
func (s *repositoryStub) Ready(context.Context) error                      { return s.readyErr }

type cacheStub struct {
	mu      sync.Mutex
	entries map[string]*store.Link
}

func (c *cacheStub) Get(_ context.Context, k string) (*store.Link, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.entries[k]
	return v, ok
}
func (c *cacheStub) Put(_ context.Context, k string, l *store.Link) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]*store.Link{}
	}
	c.entries[k] = l
}

type producerStub struct {
	mu     sync.Mutex
	events []store.Event
	err    error
}

func (p *producerStub) Send(_ context.Context, e store.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return p.err
}
func fixture() (*API, *repositoryStub, *cacheStub, *producerStub) {
	s, c, p := &repositoryStub{}, &cacheStub{}, &producerStub{}
	cfg := config.Config{BaseURL: "https://short.example/", MaxRequests: 8, MaxURLLength: 2048, RequestTimeout: time.Second, LinkTTL: 30 * 24 * time.Hour}
	return New(cfg, s, c, p, telemetry.New(), slog.New(slog.NewTextHandler(io.Discard, nil))), s, c, p
}
func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(method, path, strings.NewReader(body)))
	return r
}
func TestValidURL(t *testing.T) {
	for _, tc := range []struct {
		url   string
		valid bool
	}{
		{"https://example.com/a?q=1#fragment", true}, {"http://[::1]:8080/", true}, {"", false}, {"/relative", false}, {"ftp://example.com", false}, {"https://u:p@example.com", false}, {"https:///path", false}, {"https://example.com/a b", false}, {"https://example.com/\n", false}, {"https://example.com/\\a", false}, {"https://example.com/%zz", false},
	} {
		t.Run(tc.url, func(t *testing.T) { assert.Equal(t, tc.valid, validURL(tc.url, 2048)) })
	}
	assert.False(t, validURL("https://example.com", 5))
}
func TestShorten(t *testing.T) {
	a, s, c, _ := fixture()
	var saved store.Link
	s.create = func(ctx context.Context, l store.Link) error {
		_, ok := ctx.Deadline()
		assert.True(t, ok)
		saved = l
		return nil
	}
	r := httptest.NewRequest("POST", "/shorten", strings.NewReader(`{"url":"https://example.com/path"}`))
	r.Host = "attacker.example"
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, r)
	require.Equal(t, 201, w.Code)
	var body struct {
		Code      string    `json:"code"`
		ShortURL  string    `json:"short_url"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.True(t, identity.ValidCode(body.Code))
	assert.Equal(t, "https://short.example/"+body.Code, body.ShortURL)
	assert.Equal(t, a.Config.LinkTTL, saved.ExpiresAt.Sub(saved.CreatedAt))
	assert.Equal(t, saved.ExpiresAt, body.ExpiresAt)
	assert.NotEmpty(t, saved.Token)
	cached, ok := c.Get(context.Background(), body.Code)
	assert.True(t, ok)
	assert.Equal(t, saved, *cached)
	assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
}
func TestShortenRejectsInput(t *testing.T) {
	for _, body := range []string{`{`, `null`, `{}`, `{"url":42}`, `{"url":"/relative"}`, `{"url":"https://example.com","expiry":1}`, `{"url":"https://example.com"}{}`, strings.Repeat("x", 13000)} {
		t.Run(body[:min(len(body), 50)], func(t *testing.T) {
			a, s, _, _ := fixture()
			s.create = func(context.Context, store.Link) error { t.Error("invalid input reached store"); return nil }
			assert.Equal(t, 400, request(a.Handler(), "POST", "/shorten", body).Code)
		})
	}
}
func TestShortenRetriesCollisions(t *testing.T) {
	for _, n := range []int{2, 5} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			a, s, _, _ := fixture()
			calls := 0
			s.create = func(context.Context, store.Link) error {
				calls++
				if calls <= n {
					return store.ErrCollision
				}
				return nil
			}
			w := request(a.Handler(), "POST", "/shorten", `{"url":"https://example.com"}`)
			if n == 5 {
				assert.Equal(t, 503, w.Code)
				assert.Equal(t, 5, calls)
			} else {
				assert.Equal(t, 201, w.Code)
				assert.Equal(t, 3, calls)
			}
		})
	}
}
func TestRedirect(t *testing.T) {
	for _, tc := range []struct {
		name, method                                        string
		cached, missing, expired, storeFailure, sendFailure bool
		status, events                                      int
	}{
		{name: "cache hit", method: "GET", cached: true, status: 302, events: 1}, {name: "cache miss", method: "GET", status: 302, events: 1}, {name: "negative cache", method: "GET", cached: true, missing: true, status: 404}, {name: "unknown", method: "GET", missing: true, status: 404}, {name: "expired cache", method: "GET", cached: true, expired: true, status: 410}, {name: "database error", method: "GET", storeFailure: true, status: 503}, {name: "queue failure still redirects", method: "GET", sendFailure: true, status: 302, events: 1}, {name: "head has no analytics", method: "HEAD", status: 302},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, c, p := fixture()
			link := &store.Link{Code: "Abc0123456", URL: "https://example.com/target", ExpiresAt: time.Now().Add(time.Hour)}
			if tc.expired {
				link.ExpiresAt = time.Now().Add(-time.Second)
			}
			if tc.missing {
				link = nil
			}
			reads := 0
			s.get = func(context.Context, string) (*store.Link, error) {
				reads++
				if tc.storeFailure {
					return nil, errors.New("offline")
				}
				return link, nil
			}
			if tc.cached {
				c.Put(context.Background(), "Abc0123456", link)
			}
			if tc.sendFailure {
				p.err = errors.New("offline")
			}
			w := request(a.Handler(), tc.method, "/Abc0123456", "")
			assert.Equal(t, tc.status, w.Code)
			assert.Len(t, p.events, tc.events)
			if tc.cached {
				assert.Zero(t, reads)
			}
			if tc.status == 302 {
				assert.Equal(t, link.URL, w.Header().Get("Location"))
			}
			if tc.events > 0 {
				assert.Equal(t, "Abc0123456", p.events[0].Code)
				assert.Equal(t, 302, p.events[0].Status)
				assert.Equal(t, w.Header().Get("traceparent"), p.events[0].TraceParent)
			}
		})
	}
}
func TestStatsAndDependencyFailures(t *testing.T) {
	a, s, _, _ := fixture()
	s.get = func(context.Context, string) (*store.Link, error) {
		return &store.Link{URL: "https://example.com"}, nil
	}
	s.clicks = func(context.Context, string) (int64, error) { return 17, nil }
	w := request(a.Handler(), "GET", "/stats/Abc0123456", "")
	require.Equal(t, 200, w.Code)
	assert.JSONEq(t, `{"url":"https://example.com","clicks":17}`, w.Body.String())
	s.clicks = func(context.Context, string) (int64, error) { return 0, errors.New("offline") }
	assert.Equal(t, 503, request(a.Handler(), "GET", "/stats/Abc0123456", "").Code)
	s.create = func(context.Context, store.Link) error { return errors.New("offline") }
	assert.Equal(t, 503, request(a.Handler(), "POST", "/shorten", `{"url":"https://example.com"}`).Code)
	for _, path := range []string{"/bad", "/stats/bad"} {
		assert.Equal(t, 404, request(a.Handler(), "GET", path, "").Code)
	}
}
func TestAdmissionAndRecovery(t *testing.T) {
	a, s, _, _ := fixture()
	a.slots = make(chan struct{}, 1)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s.get = func(ctx context.Context, _ string) (*store.Link, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}
	h := a.Handler()
	go func() { defer close(done); request(h, "GET", "/Abc0123456", "") }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not enter")
	}
	w := request(h, "GET", "/Abc0123456", "")
	assert.Equal(t, 429, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
	once.Do(func() { close(release) })
	<-done
	s.get = nil
	assert.Equal(t, 404, request(h, "GET", "/Abc0123456", "").Code)
}
func TestTimeoutPanicAndDrain(t *testing.T) {
	a, s, _, _ := fixture()
	a.Config.RequestTimeout = 10 * time.Millisecond
	s.get = func(ctx context.Context, _ string) (*store.Link, error) { <-ctx.Done(); return nil, ctx.Err() }
	h := a.Handler()
	assert.Equal(t, 503, request(h, "GET", "/Abc0123456", "").Code)
	s.get = func(context.Context, string) (*store.Link, error) { panic("test") }
	assert.Equal(t, 500, request(h, "GET", "/Abc0123456", "").Code)
	assert.Empty(t, a.slots)
	assert.Equal(t, 200, request(h, "GET", "/health/ready", "").Code)
	s.readyErr = errors.New("offline")
	assert.Equal(t, 503, request(h, "GET", "/health/ready", "").Code)
	a.Drain()
	assert.Equal(t, 503, request(h, "GET", "/Abc0123456", "").Code)
	assert.Equal(t, 503, request(h, "GET", "/health/ready", "").Code)
	assert.Equal(t, 200, request(h, "GET", "/health/live", "").Code)
}
func TestTraceParent(t *testing.T) {
	id := "01234567-89ab-4cde-8012-3456789abcde"
	valid := "00-11111111111111111111111111111111-2222222222222222-01"
	assert.Equal(t, "00-11111111111111111111111111111111-0123456789ab4cde-01", traceParent(valid, id))
	for _, in := range []string{"", "garbage", "00-00000000000000000000000000000000-2222222222222222-01", "00-11111111111111111111111111111111-0000000000000000-01"} {
		assert.Equal(t, "00-0123456789ab4cde80123456789abcde-0123456789ab4cde-00", traceParent(in, id))
	}
}

func TestBrowserClientFlow(t *testing.T) {
	a, _, _, _ := fixture()
	h := a.Handler()
	page := request(h, "GET", "/", "")
	require.Equal(t, http.StatusOK, page.Code)
	assert.Equal(t, "text/html; charset=utf-8", page.Header().Get("Content-Type"))
	assert.Contains(t, page.Body.String(), `id="shorten-form"`)
	assert.Equal(t, http.StatusNotFound, request(h, "GET", "/unknown/path", "").Code)

	created := request(h, "POST", "/shorten", `{"url":"https://example.com/path?foo=bar"}`)
	require.Equal(t, http.StatusCreated, created.Code)
	var body struct {
		ShortURL string `json:"short_url"`
	}
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &body))
	require.NotEmpty(t, body.ShortURL)
	redirect := request(h, "GET", body.ShortURL, "")
	assert.Equal(t, http.StatusFound, redirect.Code)
	assert.Equal(t, "https://example.com/path?foo=bar", redirect.Header().Get("Location"))
}
