package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

const databaseTimeout = 10 * time.Second

type Postgres struct {
	db *sql.DB
}

// NewPostgres opens and verifies a pool. The caller must close it when finished.
func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	ctx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("PostgreSQL connection string is empty")
	}
	cfg, err := pq.NewConfig(databaseURL)
	if err != nil {
		// Driver parse errors can contain the connection string and password.
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = databaseTimeout
	}
	connector, err := pq.NewConnectorConfig(cfg)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return &Postgres{db: db}, nil
}

func (s *Postgres) Close() {
	_ = s.db.Close()
}

func (s *Postgres) transaction(ctx context.Context, write func(context.Context, *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin database transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := write(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database transaction: %w", err)
	}
	return nil
}
