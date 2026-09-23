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
	"url-shortener/internal/api"
	"url-shortener/internal/cache"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("API stopped", "error", err)
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
	redis, err := cache.New(c, m)
	if err != nil {
		return err
	}
	defer redis.Close()
	queue, err := analytics.New(ctx, c, m)
	if err != nil {
		return err
	}
	if err = queue.Resolve(ctx); err != nil {
		return err
	}
	app := api.New(c, db, redis, queue, m, logger)
	server := &http.Server{Addr: c.Listen, Handler: app.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: c.RequestTimeout + time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	logger.Info("API listening", "address", c.Listen)
	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	app.Drain()
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(drain); err != nil {
		_ = server.Close()
		return err
	}
	return nil
}
