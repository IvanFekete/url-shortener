package analytics

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"url-shortener/internal/config"
	"url-shortener/internal/identity"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type Worker struct {
	Queue   *Queue
	Store   store.Repository
	Config  config.Config
	Metrics *telemetry.Metrics
	Logger  *slog.Logger
}

// Run receives only as much work as it can execute immediately. Cancellation
// interrupts polling; a received batch gets a bounded opportunity to finish.
func (w *Worker) Run(ctx context.Context) {
	limit := min(w.Config.Workers, w.Config.BatchSize)
	healthy := 0
	for ctx.Err() == nil {
		w.Metrics.Concurrency.Set(float64(limit))
		messages, err := w.Queue.Receive(ctx, limit)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.Logger.Error("analytics receive failed", "error_class", store.ErrorClass(err))
			if !pause(ctx, time.Second+time.Duration(rand.IntN(1000))*time.Millisecond) {
				return
			}
			continue
		}
		if len(messages) == 0 {
			continue
		}
		var throttled atomic.Bool
		var wg sync.WaitGroup
		for _, msg := range messages {
			wg.Add(1)
			go func(msg types.Message) {
				defer wg.Done()
				if w.process(msg) {
					throttled.Store(true)
				}
			}(msg)
		}
		wg.Wait()
		if throttled.Load() {
			limit = max(1, limit/2)
			healthy = 0
			if !pause(ctx, time.Second) {
				return
			}
		} else {
			healthy++
			if healthy >= 10 {
				limit = min(limit+1, min(w.Config.Workers, w.Config.BatchSize))
				healthy = 0
			}
		}
	}
}
func (w *Worker) process(msg types.Message) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*w.Config.RequestTimeout)
	defer cancel()
	var event store.Event
	attempt, _ := strconv.Atoi(msg.Attributes["ApproximateReceiveCount"])
	attempt = max(attempt, 1)
	err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &event)
	valid := err == nil && uuidPattern.MatchString(event.ID) && identity.ValidCode(event.Code) && event.Status == 302 && !event.At.IsZero() && event.At.Before(time.Now().Add(time.Minute))
	class := "invalid_event"
	if valid {
		w.Metrics.EventAge.Observe(max(0, time.Since(event.At).Seconds()))
		duplicate, applyErr := w.Store.Apply(ctx, event)
		err = applyErr
		if err == nil {
			result := "processed"
			if duplicate {
				result = "duplicate"
			}
			w.Metrics.Events.WithLabelValues(result).Inc()
			if rand.IntN(100) == 0 {
				parts := strings.Split(event.TraceParent, "-")
				if len(parts) == 4 {
					w.Logger.Info("analytics applied", "result", result, "trace_id", parts[1])
				}
			}
			if err := w.Queue.Delete(ctx, msg.ReceiptHandle); err != nil {
				w.Logger.Error("analytics delete failed", "error_class", store.ErrorClass(err))
			}
			return false
		}
		class = store.ErrorClass(err)
	}
	w.Metrics.Events.WithLabelValues("failed").Inc()
	w.Metrics.Events.WithLabelValues("retry").Inc()
	if attempt >= w.Config.MaxAttempts {
		w.Metrics.DeadLetters.Inc()
	}
	w.Logger.Error("analytics processing failed", "error_class", class, "receive_count", attempt)
	// SQS's redrive policy owns DLQ movement. Never delete a failed transaction.
	delay := time.Duration(1<<min(attempt, 8)) * time.Second
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
	if err := w.Queue.Retry(ctx, msg.ReceiptHandle, delay); err != nil {
		w.Logger.Error("analytics visibility update failed", "error_class", store.ErrorClass(err))
	}
	return class == "throttle"
}
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
