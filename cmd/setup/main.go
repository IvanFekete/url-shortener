// setup provisions local resources explicitly. Production uses CloudFormation.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"url-shortener/internal/analytics"
	"url-shortener/internal/config"
	"url-shortener/internal/store"
	"url-shortener/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("setup failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	c, err := config.Load()
	if err != nil {
		return err
	}
	if c.DynamoEndpoint == "" || c.SQSEndpoint == "" {
		return fmt.Errorf("setup requires explicit local DYNAMODB_ENDPOINT and SQS_ENDPOINT; use infrastructure-as-code for AWS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	m := telemetry.New()
	d, err := store.New(ctx, c, m)
	if err != nil {
		return err
	}
	if err = d.Setup(ctx); err != nil {
		return err
	}
	q, err := analytics.New(ctx, c, m)
	if err != nil {
		return err
	}
	if err = q.Setup(ctx); err != nil {
		return err
	}
	slog.Info("local resources ready", "queue_url", q.URL, "dlq_url", q.DLQURL)
	return nil
}
