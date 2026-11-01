package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// Keep each statement comfortably below PostgreSQL's bind-parameter limit.
// The consumer's 100-record batches fit in one statement per table.
const maxUpsertRows = 1000

// Result is an EMA window or trade advisory that can be saved in a batch.
// The private save method keeps transaction handling inside the data package.
type Result interface {
	Validate() error
	save(context.Context, *sql.Tx) error
}

// BatchError identifies the zero-based input result that failed validation.
// Multi-row SQL errors and commit failures do not identify one input result.
type BatchError struct {
	Index int
	Err   error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("batch result %d: %v", e.Index, e.Err)
}

func (e *BatchError) Unwrap() error { return e.Err }

// SaveBatch validates all results, then saves them with multi-row upserts.
// Repeated keys retain the last input value, including embedded advisory windows.
// A write failure rolls back all results, including symbols and embedded windows.
// An empty batch does not open a transaction.
func (s *Postgres) SaveBatch(ctx context.Context, results []Result) error {
	if len(results) == 0 {
		return nil
	}
	batch, err := prepareBatch(results)
	if err != nil {
		return err
	}
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := upsertSymbols(ctx, tx, batch.symbols); err != nil {
			return err
		}
		if err := upsertWindows(ctx, tx, batch.windows); err != nil {
			return err
		}
		return upsertAdvisories(ctx, tx, batch.advisories)
	})
}

type windowKey struct {
	symbol string
	start  int64
}

type preparedBatch struct {
	symbols    []string
	windows    []*EmaWindow
	advisories []*TradeAdvisory
}

func prepareBatch(results []Result) (*preparedBatch, error) {
	symbols := make(map[string]struct{})
	windows := make(map[windowKey]*EmaWindow)
	advisories := make(map[windowKey]*TradeAdvisory)
	for i, result := range results {
		if result == nil {
			return nil, &BatchError{Index: i, Err: errors.New("result is nil")}
		}
		// Validate before deduplicating so an overwritten bad input still fails.
		if err := result.Validate(); err != nil {
			return nil, &BatchError{Index: i, Err: err}
		}
		var window *EmaWindow
		switch result := result.(type) {
		case *EmaWindow:
			window = result
		case *TradeAdvisory:
			window = result.EmaWindow
			advisories[windowKey{window.Symbol, window.WindowStart.Unix()}] = result
		default:
			return nil, &BatchError{Index: i, Err: fmt.Errorf("unsupported result type %T", result)}
		}
		// Valid windows have whole-second boundaries; Unix normalizes time zones.
		windows[windowKey{window.Symbol, window.WindowStart.Unix()}] = window
		symbols[window.Symbol] = struct{}{}
	}
	batch := &preparedBatch{}
	for symbol := range symbols {
		batch.symbols = append(batch.symbols, symbol)
	}
	sort.Strings(batch.symbols)
	keys := make([]windowKey, 0, len(windows))
	for key := range windows {
		keys = append(keys, key)
	}
	// Acquire row locks consistently across batches. Winners were chosen above.
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].symbol != keys[j].symbol {
			return keys[i].symbol < keys[j].symbol
		}
		return keys[i].start < keys[j].start
	})
	for _, key := range keys {
		batch.windows = append(batch.windows, windows[key])
		if advisory, ok := advisories[key]; ok {
			batch.advisories = append(batch.advisories, advisory)
		}
	}
	return batch, nil
}
