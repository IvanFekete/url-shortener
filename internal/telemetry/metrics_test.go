package telemetry

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetricsIsolationAndObservation(t *testing.T) {
	m, other := New(), New()
	m.Requests.WithLabelValues("/{code}", "GET", "302").Inc()
	m.Observe("sqs", "SendMessage", time.Now().Add(-time.Millisecond), errors.New("offline"), "unavailable")
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	require.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), `http_requests_total{method="GET",route="/{code}",status="302"} 1`)
	assert.Contains(t, w.Body.String(), `dependency_errors_total{class="unavailable",dependency="sqs",operation="SendMessage"} 1`)
	assert.Contains(t, w.Body.String(), `dependency_duration_seconds_count{dependency="sqs",operation="SendMessage"} 1`)
	w = httptest.NewRecorder()
	other.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	assert.NotContains(t, w.Body.String(), "http_requests_total{")
}
