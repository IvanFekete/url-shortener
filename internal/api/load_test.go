package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"url-shortener/internal/store"
)

func TestLoadMixedHTTP(t *testing.T) {
	if os.Getenv("RUN_LOAD") != "1" {
		t.Skip("opt in with RUN_LOAD=1")
	}
	duration := 5 * time.Second
	if raw := os.Getenv("LOAD_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		require.NoError(t, err)
		require.Greater(t, duration, time.Duration(0))
	}
	concurrency := 8
	if raw := os.Getenv("LOAD_CONCURRENCY"); raw != "" {
		var err error
		concurrency, err = strconv.Atoi(raw)
		require.NoError(t, err)
		require.Greater(t, concurrency, 0)
		require.LessOrEqual(t, concurrency, 1024)
	}
	base := strings.TrimRight(os.Getenv("LOAD_BASE_URL"), "/")
	label := "external application"
	var p *producerStub
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if base == "" {
		a, server, producer := memoryServer(t)
		a.Config.MaxRequests = concurrency
		a.slots = make(chan struct{}, concurrency)
		base, p, label = server.URL, producer, "in-memory dependencies"
		client = noRedirectClient(server)
		client.Timeout = 3 * time.Second
	}
	// Keep enough connections for every worker instead of churning local TCP ports.
	transport := http.DefaultTransport.(*http.Transport)
	if client.Transport != nil {
		transport = client.Transport.(*http.Transport)
	}
	transport = transport.Clone()
	transport.MaxIdleConns = concurrency
	transport.MaxIdleConnsPerHost = concurrency
	transport.MaxConnsPerHost = concurrency
	client.Transport = transport
	t.Cleanup(client.CloseIdleConnections)
	codes := make([]string, 32)
	for i := range codes {
		codes[i] = createHTTP(t, client, base)
	}
	var completed, redirects, failed atomic.Int64
	var firstFailure string
	var failureOnce sync.Once
	recordFailure := func(message string) {
		failed.Add(1)
		failureOnce.Do(func() { firstFailure = message })
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := []time.Duration{}
	deadline := time.Now().Add(duration)
	start := time.Now()
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			local := []time.Duration{}
			defer func() { mu.Lock(); latencies = append(latencies, local...); mu.Unlock() }()
			for i := 0; time.Now().Before(deadline); i++ {
				code := codes[(worker+i)%len(codes)]
				method, path, want := "GET", "/"+code, 302
				switch i % 10 {
				case 0:
					method = "HEAD"
				case 1, 2:
					path = "/stats/" + code
					want = 200
				}
				req, err := http.NewRequest(method, base+path, nil)
				if err != nil {
					recordFailure(err.Error())
					continue
				}
				begin := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					recordFailure(err.Error())
					continue
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				local = append(local, time.Since(begin))
				if readErr != nil || resp.StatusCode != want {
					recordFailure(fmt.Sprintf("%s %s: status=%d want=%d read error=%v", method, path, resp.StatusCode, want, readErr))
					continue
				}
				completed.Add(1)
				if method == "GET" && want == 302 {
					redirects.Add(1)
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start)
	require.Zero(t, failed.Load(), "unexpected responses or transport errors; first failure: %s", firstFailure)
	require.Positive(t, completed.Load())
	if p != nil {
		p.mu.Lock()
		assert.EqualValues(t, redirects.Load(), len(p.events))
		p.mu.Unlock()
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration { return latencies[min(len(latencies)-1, int(float64(len(latencies))*p))] }
	t.Logf("%s: requests=%d concurrency=%d elapsed=%s req/s=%.1f p50=%s p95=%s p99=%s", label, completed.Load(), concurrency, elapsed, float64(completed.Load())/elapsed.Seconds(), percentile(.50), percentile(.95), percentile(.99))
}

func TestStressSaturationAndRecovery(t *testing.T) {
	if os.Getenv("RUN_STRESS") != "1" {
		t.Skip("opt in with RUN_STRESS=1")
	}
	for _, limit := range []int{1, 4, 16, 64} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			a, s, _, _ := fixture()
			a.slots = make(chan struct{}, limit)
			a.Config.RequestTimeout = 10 * time.Second
			entered := make(chan struct{}, limit)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var active, peak atomic.Int64
			s.get = func(ctx context.Context, _ string) (*store.Link, error) {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				entered <- struct{}{}
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			server := httptest.NewServer(a.Handler())
			defer server.Close()
			client := noRedirectClient(server)
			client.Timeout = 12 * time.Second
			var wg sync.WaitGroup
			statuses := make(chan int, limit)
			for i := 0; i < limit; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r, err := client.Get(server.URL + "/Abc0123456")
					if err != nil {
						statuses <- 0
						return
					}
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
					statuses <- r.StatusCode
				}()
			}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			for i := 0; i < limit; i++ {
				select {
				case <-entered:
				case <-timer.C:
					once.Do(func() { close(release) })
					wg.Wait()
					t.Fatal("failed to occupy admission slots")
				}
			}
			var overload sync.WaitGroup
			results := make(chan int, limit*4)
			for i := 0; i < limit*4; i++ {
				overload.Add(1)
				go func() {
					defer overload.Done()
					r, err := client.Get(server.URL + "/Abc0123456")
					if err != nil {
						results <- 0
						return
					}
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
					results <- r.StatusCode
				}()
			}
			overload.Wait()
			close(results)
			for status := range results {
				assert.Equal(t, 429, status)
			}
			assert.EqualValues(t, limit, peak.Load())
			once.Do(func() { close(release) })
			wg.Wait()
			close(statuses)
			for status := range statuses {
				assert.Equal(t, 404, status)
			}
			assert.Zero(t, active.Load())
			r, err := client.Get(server.URL + "/Abc0123456")
			require.NoError(t, err)
			r.Body.Close()
			assert.Equal(t, 404, r.StatusCode)
			assert.Empty(t, a.slots)
		})
	}
}
