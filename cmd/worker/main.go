package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"url-shortener/internal/analytics"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := config.Load()
	if err != nil {
		return err
	}
	m := telemetry.New()
	db, err := store.New(ctx, c, m)
	if err != nil {
		return err
	}
	q, err := analytics.New(ctx, c, m)
	if err != nil {
		return err
	}
	if err = q.Resolve(ctx); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), c.RequestTimeout)
		defer cancel()
		if ctx.Err() != nil || db.Ready(check) != nil || q.Ready(check) != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: c.Listen, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	worker := &analytics.Worker{Queue: q, Store: db, Config: c, Metrics: m, Logger: logger}
	done := make(chan struct{})
	go func() { defer close(done); worker.Run(ctx) }()
	go q.Monitor(ctx)
	logger.Info("analytics worker started", "concurrency", c.Workers)
	var serveErr error
	select {
	case serveErr = <-errs:
		stop()
	case <-ctx.Done():
	}
	drain, cancel := context.WithTimeout(context.Background(), 3*c.RequestTimeout+5*time.Second)
	defer cancel()
	if err := server.Shutdown(drain); err != nil {
		_ = server.Close()
		return err
	}
	select {
	case <-done:
	case <-drain.Done():
		return drain.Err()
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}
