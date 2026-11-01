//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSaveBatchMixedResultsPreserveOrderAndReplay(t *testing.T) {
	for _, advisoryFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("advisoryFirst=%t", advisoryFirst), func(t *testing.T) {
			s := integrationStore(t)
			window := testWindow()
			updated := testWindow()
			updated.ClosingPriceMicros = 1<<63 + 42
			updated.Ema38 = "2.50"
			other := testWindow()
			other.Symbol = "OTHER.FR"
			advice := &TradeAdvisory{EmaWindow: window, Action: AdvisoryActionBuy,
				PreviousEma38: "0", PreviousEma100: "0.1234567890123456789012345678901234"}
			results := []Result{updated, other, advice}
			want := window
			if advisoryFirst {
				results = []Result{advice, other, updated}
				want = updated
			}
			for range 2 { // Replay the same batch after an uncertain broker commit.
				if err := s.SaveBatch(context.Background(), results); err != nil {
					t.Fatal(err)
				}
			}
			assertBatchRowCounts(t, s, 2, 2, 1)
			var price, short, long, action, previousShort, previousLong string
			var start, end time.Time
			if err := s.db.QueryRow(`SELECT e.closing_price_micros::text, e.ema38::text, e.ema100::text,
				e.window_start, e.window_end, a.action, a.previous_ema38::text, a.previous_ema100::text
				FROM ema_windows e JOIN trade_advisories a USING (symbol, window_start)
				WHERE e.symbol = $1`, window.Symbol).Scan(
				&price, &short, &long, &start, &end, &action, &previousShort, &previousLong); err != nil {
				t.Fatal(err)
			}
			if price != strconv.FormatUint(want.ClosingPriceMicros, 10) || short != want.Ema38 || long != want.Ema100 ||
				!start.Equal(want.WindowStart) || !end.Equal(want.WindowEnd) || action != "BUY" ||
				previousShort != advice.PreviousEma38 || previousLong != advice.PreviousEma100 {
				t.Fatalf("batch changed order or precision: %s %s %s %s %s %s %s %s",
					price, short, long, start, end, action, previousShort, previousLong)
			}
		})
	}
}

func TestSaveBatchLaterFailureRollsBackInsertsAndUpdates(t *testing.T) {
	s := integrationStore(t)
	original := testWindow()
	if err := s.SaveEmaWindow(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`ALTER TABLE trade_advisories ADD CONSTRAINT test_sell_only CHECK (action = 'SELL')`); err != nil {
		t.Fatal(err)
	}
	updated := testWindow()
	updated.Ema38 = "999"
	newWindow := testWindow()
	newWindow.Symbol = "NEW.FR"
	advisoryWindow := testWindow()
	advisoryWindow.Symbol = "FAILED.FR"
	advice := &TradeAdvisory{EmaWindow: advisoryWindow, Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}
	results := []Result{updated, newWindow, advice}
	err := s.SaveBatch(context.Background(), results)
	var batchErr *BatchError
	if err == nil || errors.As(err, &batchErr) || !strings.Contains(err.Error(), "upsert trade advisories") {
		t.Fatalf("error = %v; want batch-level advisory write failure", err)
	}
	assertBatchRowCounts(t, s, 1, 1, 0)
	var short string
	if err := s.db.QueryRow(`SELECT ema38::text FROM ema_windows WHERE symbol = $1`, original.Symbol).Scan(&short); err != nil || short != original.Ema38 {
		t.Fatalf("earlier update was not rolled back: EMA=%s error=%v", short, err)
	}
	advice.Action = AdvisoryActionSell
	if err := s.SaveBatch(context.Background(), results); err != nil {
		t.Fatalf("pool should remain usable after rollback: %v", err)
	}
	assertBatchRowCounts(t, s, 3, 3, 1)
}

func TestSaveBatchCommitFailureRollsBackWholeBatch(t *testing.T) {
	s := integrationStore(t)
	if _, err := s.db.Exec(`
		CREATE FUNCTION reject_batch_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test batch commit failure'; END;
		$$;
		CREATE CONSTRAINT TRIGGER reject_batch_commit AFTER INSERT ON trade_advisories
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_batch_commit();`); err != nil {
		t.Fatal(err)
	}
	window := testWindow()
	adviceWindow := testWindow()
	adviceWindow.Symbol = "ADVICE.FR"
	results := []Result{window, &TradeAdvisory{EmaWindow: adviceWindow, Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}}
	err := s.SaveBatch(context.Background(), results)
	var batchErr *BatchError
	if err == nil || errors.As(err, &batchErr) {
		t.Fatalf("error = %v; want transaction-level commit error", err)
	}
	assertBatchRowCounts(t, s, 0, 0, 0)
	if _, err := s.db.Exec(`DROP TRIGGER reject_batch_commit ON trade_advisories`); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveBatch(context.Background(), results); err != nil {
		t.Fatalf("pool should remain usable after commit failure: %v", err)
	}
	assertBatchRowCounts(t, s, 2, 2, 1)
}

func TestSaveBatchCancellationRollsBackEarlierResults(t *testing.T) {
	s := integrationStore(t)
	blocker, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec(`LOCK TABLE trade_advisories IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	window := testWindow()
	advice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- s.SaveBatch(ctx, []Result{window, advice})
	}()
	defer func() {
		cancel()
		_ = blocker.Rollback()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("batch did not stop after releasing its lock")
		}
	}()
	// Reaching this lock means the first result and advisory's window have
	// already been written inside the transaction.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := blocker.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE relation = 'trade_advisories'::regclass AND NOT granted
		)`).Scan(&blocked); err != nil {
			t.Fatalf("observe blocked advisory: %v", err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("batch returned before the advisory waited on the lock: %v", err)
		case <-ctx.Done():
			t.Fatal("batch never reached the advisory write")
		case <-ticker.C:
		}
	}
	deadline, _ := ctx.Deadline()
	timer := time.NewTimer(time.Until(deadline) + 300*time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		if err == nil || ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("canceled batch error=%v context=%v", err, ctx.Err())
		}
	case <-timer.C:
		t.Fatal("batch stayed blocked after cancellation")
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertBatchRowCounts(t, s, 0, 0, 0)
}

func assertBatchRowCounts(t *testing.T, s *Postgres, wantSymbols, wantWindows, wantAdvisories int) {
	t.Helper()
	var symbols, windows, advisories int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM symbols),
		(SELECT count(*) FROM ema_windows), (SELECT count(*) FROM trade_advisories)`).Scan(&symbols, &windows, &advisories); err != nil {
		t.Fatal(err)
	}
	if symbols != wantSymbols || windows != wantWindows || advisories != wantAdvisories {
		t.Fatalf("counts symbols=%d windows=%d advisories=%d; want %d %d %d",
			symbols, windows, advisories, wantSymbols, wantWindows, wantAdvisories)
	}
}

func TestSaveBatchDeduplicatesWindowsAndAdvisories(t *testing.T) {
	s := integrationStore(t)
	firstWindow := testWindow()
	firstWindow.Symbol = "O'RLY.FR" // Symbols remain SQL parameters.
	firstAdvice := &TradeAdvisory{EmaWindow: firstWindow, Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0.1234567890123456789012345678901234"}
	secondWindow := *firstWindow
	secondWindow.Ema38 = "2.50"
	secondWindow.WindowStart = secondWindow.WindowStart.In(time.FixedZone("UTC+02", 2*3600))
	secondWindow.WindowEnd = secondWindow.WindowEnd.In(time.FixedZone("UTC+02", 2*3600))
	secondAdvice := &TradeAdvisory{EmaWindow: &secondWindow, Action: AdvisoryActionSell,
		PreviousEma38: "0.0000000000000000000000000000000010", PreviousEma100: "3.4000"}
	lastWindow := *firstWindow
	lastWindow.Ema38 = "9.876543210987654321098765432109876"
	lastWindow.Ema100 = "7.0000"
	results := []Result{firstWindow, firstAdvice, &secondWindow, secondAdvice, &lastWindow}
	for range 2 {
		if err := s.SaveBatch(context.Background(), results); err != nil {
			t.Fatal(err)
		}
	}
	assertBatchRowCounts(t, s, 1, 1, 1)
	var symbol, price, short, long, action, previousShort, previousLong string
	if err := s.db.QueryRow(`SELECT e.symbol, e.closing_price_micros::text, e.ema38::text, e.ema100::text,
		a.action, a.previous_ema38::text, a.previous_ema100::text
		FROM ema_windows e JOIN trade_advisories a USING (symbol, window_start)`).Scan(
		&symbol, &price, &short, &long, &action, &previousShort, &previousLong); err != nil {
		t.Fatal(err)
	}
	if symbol != firstWindow.Symbol || price != "18446744073709551615" || short != lastWindow.Ema38 ||
		long != lastWindow.Ema100 || action != "SELL" || previousShort != secondAdvice.PreviousEma38 ||
		previousLong != secondAdvice.PreviousEma100 {
		t.Fatalf("deduplication changed final values or precision: %s %s %s %s %s %s %s",
			symbol, price, short, long, action, previousShort, previousLong)
	}
}

func TestSaveBatchStatementChunksShareOneTransaction(t *testing.T) {
	s := integrationStore(t)
	if _, err := s.db.Exec(`ALTER TABLE trade_advisories ADD CONSTRAINT test_sell_only CHECK (action = 'SELL')`); err != nil {
		t.Fatal(err)
	}
	var results []Result
	var last *TradeAdvisory
	for i := range maxUpsertRows + 1 {
		window := testWindow()
		window.Symbol = fmt.Sprintf("BATCH_%04d.FR", i)
		last = &TradeAdvisory{EmaWindow: window, Action: AdvisoryActionSell, PreviousEma38: "0", PreviousEma100: "0"}
		results = append(results, last)
	}
	// The first advisory statement succeeds; the last row in the second fails.
	last.Action = AdvisoryActionBuy
	if err := s.SaveBatch(context.Background(), results); err == nil {
		t.Fatal("expected failure in the second advisory statement")
	}
	assertBatchRowCounts(t, s, 0, 0, 0)
	last.Action = AdvisoryActionSell
	if err := s.SaveBatch(context.Background(), results); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	assertBatchRowCounts(t, s, maxUpsertRows+1, maxUpsertRows+1, maxUpsertRows+1)
}
