package api

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"url-shortener/internal/analytics"
	"url-shortener/internal/cache"
	"url-shortener/internal/config"
	"url-shortener/internal/identity"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

//go:embed index.html
var clientPage string

type API struct {
	Config   config.Config
	Store    store.Repository
	Cache    cache.Cache
	Producer analytics.Producer
	Metrics  *telemetry.Metrics
	Logger   *slog.Logger
	draining atomic.Bool
	slots    chan struct{}
}

func New(c config.Config, s store.Repository, cache cache.Cache, p analytics.Producer, m *telemetry.Metrics, l *slog.Logger) *API {
	return &API{Config: c, Store: s, Cache: cache, Producer: p, Metrics: m, Logger: l, slots: make(chan struct{}, c.MaxRequests)}
}
func (a *API) Drain() { a.draining.Store(true) }
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = io.WriteString(w, clientPage)
	})
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.Handle("GET /metrics", a.Metrics.Handler())
	mux.Handle("POST /shorten", a.wrap("/shorten", a.shorten))
	mux.Handle("GET /stats/{code}", a.wrap("/stats/{code}", a.stats))
	mux.Handle("GET /{code}", a.wrap("/{code}", a.redirect))
	return mux
}
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		problem(w, 503, "shutting down")
		return
	}
	if err := a.Store.Ready(r.Context()); err != nil {
		problem(w, 503, "database unavailable")
		return
	}
	w.WriteHeader(200)
}

type response struct {
	http.ResponseWriter
	status int
}

func (w *response) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *response) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (a *API) wrap(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &response{ResponseWriter: w}
		requestID, err := identity.UUID()
		if err != nil {
			problem(rw, 503, "random source unavailable")
			return
		}
		trace := traceParent(r.Header.Get("traceparent"), requestID)
		rw.Header().Set("X-Request-ID", requestID)
		rw.Header().Set("traceparent", trace)
		rw.Header().Set("Cache-Control", "no-store")
		defer func() {
			if recover() != nil {
				problem(rw, 500, "internal error")
				a.Logger.Error("request panic", "request_id", requestID)
			}
			status := rw.status
			if status == 0 {
				status = 200
			}
			method := r.Method
			if method != "GET" && method != "HEAD" && method != "POST" {
				method = "other"
			}
			a.Metrics.Requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
			a.Metrics.Latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
			if status >= 400 || route != "/{code}" || rand.IntN(100) == 0 {
				a.Logger.Info("http request", "request_id", requestID, "trace_id", strings.Split(trace, "-")[1], "route", route, "status", status, "duration_ms", time.Since(start).Milliseconds())
			}
		}()
		if a.draining.Load() {
			problem(rw, 503, "shutting down")
			return
		}
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
		default:
			a.Metrics.Rejected.Inc()
			rw.Header().Set("Retry-After", "1")
			problem(rw, 429, "too many requests")
			return
		}
		a.Metrics.Inflight.Inc()
		defer a.Metrics.Inflight.Dec()
		ctx, cancel := context.WithTimeout(r.Context(), a.Config.RequestTimeout)
		defer cancel()
		next(rw, r.WithContext(ctx))
	})
}
func traceParent(in, uuid string) string {
	id := strings.ReplaceAll(uuid, "-", "")
	trace := id
	flags := "00"
	p := strings.Split(in, "-")
	if len(p) == 4 && p[0] == "00" && len(p[1]) == 32 && len(p[2]) == 16 && len(p[3]) == 2 {
		_, e1 := hex.DecodeString(p[1])
		_, e2 := hex.DecodeString(p[2])
		_, e3 := hex.DecodeString(p[3])
		if e1 == nil && e2 == nil && e3 == nil && p[1] != strings.Repeat("0", 32) && p[2] != strings.Repeat("0", 16) {
			trace = p[1]
			flags = p[3]
		}
	}
	return "00-" + trace + "-" + id[:16] + "-" + flags
}
func (a *API) shorten(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(a.Config.MaxURLLength)*6+128)
	var input struct {
		URL string `json:"url"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF || !validURL(input.URL, a.Config.MaxURLLength) {
		problem(w, 400, "provide an absolute HTTP(S) URL without credentials")
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		code, err := identity.Code()
		if err != nil {
			a.unavailable(w, err)
			return
		}
		token, err := identity.UUID()
		if err != nil {
			a.unavailable(w, err)
			return
		}
		now := time.Now().UTC()
		link := store.Link{Code: code, URL: input.URL, Token: token, CreatedAt: now, ExpiresAt: now.Add(a.Config.LinkTTL)}
		err = a.Store.Create(r.Context(), link)
		if errors.Is(err, store.ErrCollision) {
			continue
		}
		if err != nil {
			a.unavailable(w, err)
			return
		}
		a.Cache.Put(r.Context(), code, &link)
		jsonResponse(w, 201, map[string]any{"code": code, "short_url": strings.TrimRight(a.Config.BaseURL, "/") + "/" + code, "expires_at": link.ExpiresAt.Format(time.RFC3339Nano)})
		return
	}
	problem(w, 503, "code allocation exhausted")
}
func validURL(raw string, maxLength int) bool {
	if len(raw) == 0 || len(raw) > maxLength || strings.ContainsAny(raw, "\r\n\t\\ ") {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.Hostname() != "" && u.User == nil && u.Opaque == ""
}
func (a *API) lookup(ctx context.Context, code string) (*store.Link, error) {
	if link, ok := a.Cache.Get(ctx, code); ok {
		return link, nil
	}
	link, err := a.Store.Get(ctx, code)
	if err == nil {
		a.Cache.Put(ctx, code, link)
	}
	return link, err
}
func (a *API) redirect(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !identity.ValidCode(code) {
		problem(w, 404, "unknown code")
		return
	}
	link, err := a.lookup(r.Context(), code)
	if err != nil {
		a.unavailable(w, err)
		return
	}
	if link == nil {
		problem(w, 404, "unknown code")
		return
	}
	if !time.Now().Before(link.ExpiresAt) {
		problem(w, 410, "link expired")
		return
	}
	if r.Method == http.MethodGet {
		id, err := identity.UUID()
		if err != nil {
			a.Metrics.Dropped.Inc()
		} else {
			if err := a.Producer.Send(r.Context(), store.Event{ID: id, Code: code, At: time.Now().UTC(), Status: 302, TraceParent: w.Header().Get("traceparent")}); err != nil {
				a.Logger.Error("analytics enqueue failed", "request_id", w.Header().Get("X-Request-ID"), "traceparent", w.Header().Get("traceparent"), "error_class", store.ErrorClass(err))
			}
		}
	}
	w.Header().Set("Location", link.URL)
	w.WriteHeader(http.StatusFound)
}
func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if !identity.ValidCode(code) {
		problem(w, 404, "unknown code")
		return
	}
	link, err := a.Store.Get(r.Context(), code)
	if err != nil {
		a.unavailable(w, err)
		return
	}
	if link == nil {
		problem(w, 404, "unknown code")
		return
	}
	clicks, err := a.Store.Clicks(r.Context(), code)
	if err != nil {
		a.unavailable(w, err)
		return
	}
	jsonResponse(w, 200, map[string]any{"url": link.URL, "clicks": clicks})
}
func (a *API) unavailable(w http.ResponseWriter, err error) {
	a.Logger.Error("dependency request failed", "request_id", w.Header().Get("X-Request-ID"), "traceparent", w.Header().Get("traceparent"), "error_class", store.ErrorClass(err))
	problem(w, 503, "dependency unavailable")
}
func problem(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": message})
}
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
