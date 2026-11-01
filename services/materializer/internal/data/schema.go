package data

import (
	"context"
	"database/sql"
	"fmt"
)

// InitSchema applies the initial migration only when all read-model tables are absent.
// Existing tables and data are preserved; a partial schema needs manual repair.
func (s *Postgres) InitSchema(ctx context.Context, migration string) error {
	ctx, cancel := context.WithTimeout(ctx, databaseTimeout)
	defer cancel()
	var tables int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema()
		AND table_name IN ('symbols', 'ema_windows', 'trade_advisories')`).Scan(&tables); err != nil {
		return fmt.Errorf("inspect materializer schema: %w", err)
	}
	if tables == 3 {
		return nil
	}
	if tables != 0 {
		return fmt.Errorf("materializer schema is incomplete (%d of 3 tables); repair it before starting", tables)
	}
	if _, err := s.db.ExecContext(ctx, migration); err != nil {
		return fmt.Errorf("initialize materializer schema: %w", err)
	}
	return nil
}

// ResetReadModels atomically removes stored results without dropping the schema.
func (s *Postgres) ResetReadModels(ctx context.Context) error {
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `TRUNCATE trade_advisories, ema_windows, symbols`); err != nil {
			return fmt.Errorf("reset materializer read models: %w", err)
		}
		return nil
	})
}
