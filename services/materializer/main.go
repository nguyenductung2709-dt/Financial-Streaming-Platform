package main

import (
	"context"
	_ "embed"
	"flag"
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

//go:embed migrations/001_create_read_models.up.sql
var initialMigration string

func main() {
	initDB := flag.Bool("init-db", false, "Create the initial materializer tables if absent, then exit")
	resetDB := flag.Bool("reset-db", false, "Clear materializer tables, then exit")
	flag.Parse()
	if flag.NArg() != 0 || (*initDB && *resetDB) {
		slog.Error("use at most one of -init-db or -reset-db, without positional arguments")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A canceled database operation can return a driver error during shutdown.
	if err := run(ctx, *initDB, *resetDB); err != nil && ctx.Err() == nil {
		slog.Error("materializer failed", "error", err)
		os.Exit(1)
	}
	slog.Info("materializer stopped")
}

func run(ctx context.Context, initDB, resetDB bool) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	store, err := data.NewPostgres(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if initDB || resetDB {
		if err := store.InitSchema(ctx, initialMigration); err != nil {
			return err
		}
		if resetDB {
			return store.ResetReadModels(ctx)
		}
		return nil
	}

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
