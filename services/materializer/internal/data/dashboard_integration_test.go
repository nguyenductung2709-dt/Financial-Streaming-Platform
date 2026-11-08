//go:build integration

package data

import (
	"context"
	"os"
	"testing"
)

func TestDashboardMigrationsPreserveMaterializerWritesAndReset(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	advice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}
	if err := s.SaveBatch(ctx, []Result{advice}); err != nil {
		t.Fatal(err)
	}
	apply := func(name string) {
		t.Helper()
		sql, err := os.ReadFile("../../migrations/" + name + ".sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	apply("002_create_notifications.up")
	apply("003_notify_new_symbols.up")
	apply("004_result_revisions.up")
	assertBatchRowCounts(t, s, 1, 1, 1)
	var before, after int64
	if err := s.db.QueryRowContext(ctx, "SELECT revision FROM trade_advisories").Scan(&before); err != nil {
		t.Fatal(err)
	}
	advice.Action = AdvisoryActionSell
	if err := s.SaveBatch(ctx, []Result{advice}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT revision FROM trade_advisories").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("revision did not advance: before %d, after %d", before, after)
	}
	if err := s.ResetReadModels(ctx); err != nil {
		t.Fatal(err)
	}
	assertBatchRowCounts(t, s, 0, 0, 0)
	if err := s.SaveBatch(ctx, []Result{advice}); err != nil {
		t.Fatal(err)
	}
	apply("004_result_revisions.down")
	apply("003_notify_new_symbols.down")
	apply("002_create_notifications.down")
	if err := s.SaveBatch(ctx, []Result{advice}); err != nil {
		t.Fatalf("writes after migration rollback: %v", err)
	}
}
