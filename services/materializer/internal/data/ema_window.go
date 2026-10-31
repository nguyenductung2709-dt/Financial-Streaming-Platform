package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var decimalPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// EmaWindow is a completed five-minute result. EMA decimals are in price units.
type EmaWindow struct {
	Symbol             string
	WindowStart        time.Time
	WindowEnd          time.Time
	ClosingPriceMicros uint64
	Ema38              string
	Ema100             string
}

func (s *Postgres) SaveEmaWindow(ctx context.Context, window *EmaWindow) error {
	if err := validateWindow(window); err != nil {
		return err
	}
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return upsertWindow(ctx, tx, window)
	})
}

func upsertWindow(ctx context.Context, tx *sql.Tx, window *EmaWindow) error {
	if err := upsertSymbol(ctx, tx, window.Symbol); err != nil {
		return err
	}
	// Decimal text and uint64 text are cast by PostgreSQL without a float conversion.
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ema_windows (symbol, window_start, window_end, closing_price_micros, ema38, ema100)
		VALUES ($1, $2, $3, $4::text::numeric, $5::text::numeric, $6::text::numeric)
		ON CONFLICT (symbol, window_start) DO UPDATE SET
		    window_end = EXCLUDED.window_end,
		    closing_price_micros = EXCLUDED.closing_price_micros,
		    ema38 = EXCLUDED.ema38,
		    ema100 = EXCLUDED.ema100`,
		window.Symbol, window.WindowStart, window.WindowEnd,
		strconv.FormatUint(window.ClosingPriceMicros, 10), window.Ema38, window.Ema100)
	if err != nil {
		return fmt.Errorf("upsert EMA window for %q: %w", window.Symbol, err)
	}
	return nil
}

func validateWindow(window *EmaWindow) error {
	if window == nil {
		return errors.New("EMA window is nil")
	}
	if symbol := window.Symbol; symbol == "" || symbol != strings.TrimSpace(symbol) {
		return errors.New("symbol must be nonempty and trimmed")
	}
	start, end := window.WindowStart, window.WindowEnd
	if start.IsZero() || start.Year() < 1 || start.Year() > 9999 {
		return errors.New("invalid window_start")
	}
	if end.IsZero() || end.Year() < 1 || end.Year() > 9999 {
		return errors.New("invalid window_end")
	}
	if start.Nanosecond() != 0 || start.Unix()%300 != 0 || !end.Equal(start.Add(5*time.Minute)) {
		return errors.New("window must align to five-minute boundaries and last exactly five minutes")
	}
	if window.ClosingPriceMicros == 0 {
		return errors.New("closing price must be positive")
	}
	if !decimalPattern.MatchString(window.Ema38) || !decimalPattern.MatchString(window.Ema100) {
		return errors.New("EMA values must be nonnegative decimals without exponent notation")
	}
	return nil
}
