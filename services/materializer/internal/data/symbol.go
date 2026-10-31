package data

import (
	"context"
	"database/sql"
	"fmt"
)

type Symbol struct {
	Symbol string
}

func upsertSymbol(ctx context.Context, tx *sql.Tx, symbol string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO symbols (symbol) VALUES ($1) ON CONFLICT DO NOTHING`, symbol); err != nil {
		return fmt.Errorf("upsert symbol %q: %w", symbol, err)
	}
	return nil
}
