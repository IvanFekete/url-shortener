package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"url-shortener/internal/store"
)

// This fixture crosses a real HTTP socket with concurrent, in-memory dependencies.
// The separate service suite exercises DynamoDB, Redis, SQS and the worker.
func memoryServer(t *testing.T) (*API, *httptest.Server, *producerStub) {
	t.Helper()
	a, s, _, p := fixture()
	var mu sync.Mutex
	links := map[string]store.Link{}
	s.create = func(_ context.Context, l store.Link) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := links[l.Code]; ok {
			return store.ErrCollision
		}
		links[l.Code] = l
		return nil
	}
	s.get = func(_ context.Context, code string) (*store.Link, error) {
		mu.Lock()
		defer mu.Unlock()
		l, ok := links[code]
		if !ok {
			return nil, nil
		}
		return &l, nil
	}
	server := httptest.NewServer(a.Handler())
	t.Cleanup(server.Close)
	return a, server, p
}
func noRedirectClient(server *httptest.Server) *http.Client {
	c := server.Client()
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}
func createHTTP(t *testing.T, c *http.Client, base string) string {
	t.Helper()
	resp, err := c.Post(base+"/shorten", "application/json", strings.NewReader(`{"url":"https://example.com/target"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 201, resp.StatusCode)
	var result struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Code, 10)
	return result.Code
}
func TestIntegrationHTTPLifecycle(t *testing.T) {
	a, server, p := memoryServer(t)
	c := noRedirectClient(server)
	code := createHTTP(t, c, server.URL)
	for _, method := range []string{"GET", "HEAD"} {
		req, err := http.NewRequest(method, server.URL+"/"+code, nil)
		require.NoError(t, err)
		resp, err := c.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, 302, resp.StatusCode)
		assert.Equal(t, "https://example.com/target", resp.Header.Get("Location"))
	}
	p.mu.Lock()
	assert.Len(t, p.events, 1)
	p.mu.Unlock()
	resp, err := c.Get(server.URL + "/stats/" + code)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.JSONEq(t, `{"url":"https://example.com/target","clicks":0}`, string(body))
	a.Drain()
	resp, err = c.Get(server.URL + "/" + code)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, 503, resp.StatusCode)
}
