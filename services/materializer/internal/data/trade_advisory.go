package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type AdvisoryAction string

const (
	AdvisoryActionBuy  AdvisoryAction = "BUY"
	AdvisoryActionSell AdvisoryAction = "SELL"
)

type TradeAdvisory struct {
	EmaWindow      *EmaWindow
	Action         AdvisoryAction
	PreviousEma38  string
	PreviousEma100 string
}

// SaveTradeAdvisory also saves its embedded window, regardless of topic arrival order.
func (s *Postgres) SaveTradeAdvisory(ctx context.Context, advisory *TradeAdvisory) error {
	if err := advisory.Validate(); err != nil {
		return err
	}
	return s.transaction(ctx, advisory.save)
}

func (advisory *TradeAdvisory) save(ctx context.Context, tx *sql.Tx) error {
	window := advisory.EmaWindow
	if err := window.save(ctx, tx); err != nil {
		return err
	}
	return upsertAdvisories(ctx, tx, []*TradeAdvisory{advisory})
}

func upsertAdvisories(ctx context.Context, tx *sql.Tx, advisories []*TradeAdvisory) error {
	for rows := range slices.Chunk(advisories, maxUpsertRows) {
		values := make([]string, len(rows))
		args := make([]any, 0, 5*len(rows))
		for i, advisory := range rows {
			n := len(args)
			values[i] = fmt.Sprintf("($%d,$%d,$%d,$%d::text::numeric,$%d::text::numeric)", n+1, n+2, n+3, n+4, n+5)
			args = append(args, advisory.EmaWindow.Symbol, advisory.EmaWindow.WindowStart,
				string(advisory.Action), advisory.PreviousEma38, advisory.PreviousEma100)
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO trade_advisories (symbol, window_start, action, previous_ema38, previous_ema100)
			VALUES `+strings.Join(values, ",")+`
			ON CONFLICT (symbol, window_start) DO UPDATE SET
			    action = EXCLUDED.action,
			    previous_ema38 = EXCLUDED.previous_ema38,
			    previous_ema100 = EXCLUDED.previous_ema100`, args...)
		if err != nil {
			return fmt.Errorf("upsert trade advisories: %w", err)
		}
	}
	return nil
}

// Validate checks the advisory and its embedded EMA window.
func (advisory *TradeAdvisory) Validate() error {
	if advisory == nil {
		return errors.New("advisory is nil")
	}
	if err := advisory.EmaWindow.Validate(); err != nil {
		return fmt.Errorf("advisory window: %w", err)
	}
	switch advisory.Action {
	case AdvisoryActionBuy, AdvisoryActionSell:
	default:
		return fmt.Errorf("unsupported advisory action %q", advisory.Action)
	}
	for name, value := range map[string]string{
		"previous_ema38":  advisory.PreviousEma38,
		"previous_ema100": advisory.PreviousEma100,
	} {
		if !decimalPattern.MatchString(value) {
			return fmt.Errorf("%s must be a nonnegative decimal without exponent notation", name)
		}
	}
	return nil
}
