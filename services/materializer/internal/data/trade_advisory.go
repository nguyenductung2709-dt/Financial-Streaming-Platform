package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	if err := validateTradeAdvisory(advisory); err != nil {
		return err
	}
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		window := advisory.EmaWindow
		if err := upsertWindow(ctx, tx, window); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO trade_advisories (symbol, window_start, action, previous_ema38, previous_ema100)
			VALUES ($1, $2, $3, $4::text::numeric, $5::text::numeric)
			ON CONFLICT (symbol, window_start) DO UPDATE SET
			    action = EXCLUDED.action,
			    previous_ema38 = EXCLUDED.previous_ema38,
			    previous_ema100 = EXCLUDED.previous_ema100`,
			window.Symbol, window.WindowStart, string(advisory.Action),
			advisory.PreviousEma38, advisory.PreviousEma100)
		if err != nil {
			return fmt.Errorf("upsert advisory for %q: %w", window.Symbol, err)
		}
		return nil
	})
}

func validateTradeAdvisory(advisory *TradeAdvisory) error {
	if advisory == nil {
		return errors.New("advisory is nil")
	}
	if err := validateWindow(advisory.EmaWindow); err != nil {
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
