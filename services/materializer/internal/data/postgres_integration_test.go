//go:build integration

package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
)

func integrationStore(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("MATERIALIZER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("set MATERIALIZER_TEST_DATABASE_URL to a PostgreSQL URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	schema := fmt.Sprintf("materializer_test_%d", time.Now().UnixNano())
	identifier := pq.QuoteIdentifier(schema)
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("remove test schema: %v", err)
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("MATERIALIZER_TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	s, err := NewPostgres(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	migration, err := os.ReadFile("../../migrations/001_create_read_models.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPostgresDeliveryOrderAndDuplicates(t *testing.T) {
	for _, advisoryFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("advisoryFirst=%t", advisoryFirst), func(t *testing.T) {
			s := integrationStore(t)
			ctx := context.Background()
			window := testWindow()
			advice := &TradeAdvisory{EmaWindow: window, Action: AdvisoryActionBuy,
				PreviousEma38: "0", PreviousEma100: "0.1234567890123456789012345678901234"}
			writeWindow := func() error { return s.SaveEmaWindow(ctx, window) }
			writeAdvice := func() error { return s.SaveTradeAdvisory(ctx, advice) }
			writes := []func() error{writeWindow, writeAdvice, writeWindow, writeAdvice}
			if advisoryFirst {
				writes = []func() error{writeAdvice, writeWindow, writeAdvice, writeWindow}
			}
			for _, write := range writes {
				if err := write(); err != nil {
					t.Fatal(err)
				}
			}
			var symbols, windows, advisories int
			if err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM symbols), (SELECT count(*) FROM ema_windows), (SELECT count(*) FROM trade_advisories)`).Scan(&symbols, &windows, &advisories); err != nil {
				t.Fatal(err)
			}
			if symbols != 1 || windows != 1 || advisories != 1 {
				t.Fatalf("row counts: symbols=%d windows=%d advisories=%d", symbols, windows, advisories)
			}
			var price, short, long, action, previousShort, previousLong string
			var start, end time.Time
			if err := s.db.QueryRowContext(ctx, `SELECT e.closing_price_micros::text, e.ema38::text, e.ema100::text, e.window_start, e.window_end,
				a.action, a.previous_ema38::text, a.previous_ema100::text FROM ema_windows e JOIN trade_advisories a USING (symbol, window_start)`).Scan(
				&price, &short, &long, &start, &end, &action, &previousShort, &previousLong); err != nil {
				t.Fatal(err)
			}
			if price != "18446744073709551615" || short != window.Ema38 || long != window.Ema100 ||
				action != "BUY" || previousShort != advice.PreviousEma38 || previousLong != advice.PreviousEma100 ||
				!start.Equal(window.WindowStart) || !end.Equal(window.WindowEnd) {
				t.Fatalf("stored fields differ: %s %s %s %s %s %s %s %s", price, short, long, start, end, action, previousShort, previousLong)
			}
		})
	}
}

func TestPostgresAdvisoryFailureRollsBackAllWrites(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE trade_advisories ADD CONSTRAINT test_sell_only CHECK (action = 'SELL')`); err != nil {
		t.Fatal(err)
	}
	advice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}
	if err := s.SaveTradeAdvisory(ctx, advice); err == nil {
		t.Fatal("expected database constraint failure")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM symbols) + (SELECT count(*) FROM ema_windows) + (SELECT count(*) FROM trade_advisories)`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial writes remain after failure: count=%d err=%v", count, err)
	}
	advice.Action = AdvisoryActionSell
	if err := s.SaveTradeAdvisory(ctx, advice); err != nil {
		t.Fatalf("pool should remain usable after rollback: %v", err)
	}
}

func TestPostgresWindowsAndSymbolsStayIndependent(t *testing.T) {
	s := integrationStore(t)
	first := testWindow()
	second := testWindow()
	second.Symbol = "RUI.ETR"
	third := testWindow()
	third.WindowStart = third.WindowStart.Add(5 * time.Minute)
	third.WindowEnd = third.WindowEnd.Add(5 * time.Minute)
	third.Ema38 = "2.5"
	for _, window := range []*EmaWindow{first, second, third} {
		if err := s.SaveEmaWindow(context.Background(), window); err != nil {
			t.Fatal(err)
		}
	}
	var symbols, windows int
	if err := s.db.QueryRowContext(context.Background(), `SELECT (SELECT count(*) FROM symbols), (SELECT count(*) FROM ema_windows)`).Scan(&symbols, &windows); err != nil || symbols != 2 || windows != 3 {
		t.Fatalf("symbols=%d windows=%d err=%v", symbols, windows, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.SaveEmaWindow(ctx, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write: %v", err)
	}
}

func TestPostgresMigrationDownAndUp(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	if err := s.SaveTradeAdvisory(ctx, &TradeAdvisory{
		EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0",
	}); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/001_create_read_models.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	var tables int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name IN ('symbols', 'ema_windows', 'trade_advisories')`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("tables after rollback=%d err=%v", tables, err)
	}
	up, err := os.ReadFile("../../migrations/001_create_read_models.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, string(up)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEmaWindow(ctx, testWindow()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCancelsBlockedWrite(t *testing.T) {
	s := integrationStore(t)
	blocker, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec(`LOCK TABLE symbols IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		writeErr = s.SaveEmaWindow(ctx, testWindow())
	}()
	defer func() {
		cancel()
		_ = blocker.Rollback()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("write did not stop after releasing its lock")
		}
	}()

	// Confirm the write reached PostgreSQL and is waiting on this transaction.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := blocker.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE relation = 'symbols'::regclass AND NOT granted
		)`).Scan(&blocked)
		if err != nil {
			t.Fatalf("observe blocked write: %v", err)
		}
		if blocked {
			break
		}
		select {
		case <-done:
			t.Fatalf("write returned before waiting on the lock: %v", writeErr)
		case <-ctx.Done():
			t.Fatal("write never waited on the database lock")
		case <-ticker.C:
		}
	}

	deadline, _ := ctx.Deadline()
	timer := time.NewTimer(time.Until(deadline) + 300*time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
		if ctx.Err() != context.DeadlineExceeded || writeErr == nil {
			t.Fatalf("blocked write error = %v, context error = %v", writeErr, ctx.Err())
		}
	case <-timer.C:
		t.Fatal("write stayed blocked beyond its deadline while the lock was held")
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM symbols`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("canceled write left rows: count=%d err=%v", count, err)
	}
	if err := s.SaveEmaWindow(context.Background(), testWindow()); err != nil {
		t.Fatalf("pool should remain usable after cancellation: %v", err)
	}
}
