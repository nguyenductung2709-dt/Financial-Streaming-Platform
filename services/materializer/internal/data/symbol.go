package data

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

type Symbol struct {
	Symbol string
}

func upsertSymbol(ctx context.Context, tx *sql.Tx, symbol string) error {
	return upsertSymbols(ctx, tx, []string{symbol})
}

func upsertSymbols(ctx context.Context, tx *sql.Tx, symbols []string) error {
	for rows := range slices.Chunk(symbols, maxUpsertRows) {
		values := make([]string, len(rows))
		args := make([]any, len(rows))
		for i, symbol := range rows {
			values[i] = fmt.Sprintf("($%d)", i+1)
			args[i] = symbol
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO symbols (symbol) VALUES `+
			strings.Join(values, ",")+` ON CONFLICT DO NOTHING`, args...); err != nil {
			return fmt.Errorf("upsert symbols: %w", err)
		}
	}
	return nil
}
