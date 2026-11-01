package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"financial-streaming-platform/services/materializer/internal/consumer"
	"financial-streaming-platform/services/materializer/internal/data"
)

type config struct {
	databaseURL string
	brokers     []string
	groupID     string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A canceled database operation can return a driver error during shutdown.
	if err := run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("materializer failed", "error", err)
		os.Exit(1)
	}
	slog.Info("materializer stopped")
}

func run(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	store, err := data.NewPostgres(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()

	c, err := consumer.New(cfg.brokers, cfg.groupID, store, slog.Default())
	if err != nil {
		return err
	}
	slog.Info("starting materializer", "brokers", cfg.brokers, "group_id", cfg.groupID)
	// Run closes the consumer before the deferred database close.
	return c.Run(ctx)
}

func loadConfig() (config, error) {
	cfg := config{databaseURL: os.Getenv("MATERIALIZER_DATABASE_URL")}
	if strings.TrimSpace(cfg.databaseURL) == "" {
		return config{}, fmt.Errorf("MATERIALIZER_DATABASE_URL must be set")
	}
	brokers, ok := os.LookupEnv("MATERIALIZER_BROKERS")
	if !ok {
		brokers = "localhost:19092"
	}
	cfg.brokers = strings.Split(brokers, ",")
	for i := range cfg.brokers {
		cfg.brokers[i] = strings.TrimSpace(cfg.brokers[i])
		if cfg.brokers[i] == "" {
			return config{}, fmt.Errorf("MATERIALIZER_BROKERS must contain nonempty, comma-separated broker addresses")
		}
	}
	cfg.groupID, ok = os.LookupEnv("MATERIALIZER_GROUP_ID")
	if !ok {
		cfg.groupID = "materializer"
	}
	if cfg.groupID == "" || cfg.groupID != strings.TrimSpace(cfg.groupID) {
		return config{}, fmt.Errorf("MATERIALIZER_GROUP_ID must be nonempty and trimmed")
	}
	return cfg, nil
}
