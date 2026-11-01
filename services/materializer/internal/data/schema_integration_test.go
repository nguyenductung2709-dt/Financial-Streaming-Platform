//go:build integration

package data

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestInitSchemaCreatesMissingTablesAndPreservesExistingData(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	if _, err := s.db.Exec(`DROP TABLE trade_advisories, ema_windows, symbols`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../migrations/001_create_read_models.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InitSchema(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEmaWindow(ctx, testWindow()); err != nil {
		t.Fatal(err)
	}
	if err := s.InitSchema(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	assertBatchRowCounts(t, s, 1, 1, 0)
}

func TestInitSchemaRejectsPartialSchema(t *testing.T) {
	s := integrationStore(t)
	if _, err := s.db.Exec(`DROP TABLE trade_advisories`); err != nil {
		t.Fatal(err)
	}
	err := s.InitSchema(context.Background(), "invalid SQL must not execute")
	if err == nil || !strings.Contains(err.Error(), "schema is incomplete") {
		t.Fatalf("error = %v; want incomplete schema error", err)
	}
}

func TestResetReadModelsClearsAllTablesAndAllowsReuse(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	advice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "0", PreviousEma100: "0"}
	if err := s.SaveTradeAdvisory(ctx, advice); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetReadModels(ctx); err != nil {
		t.Fatal(err)
	}
	assertBatchRowCounts(t, s, 0, 0, 0)
	if err := s.SaveTradeAdvisory(ctx, advice); err != nil {
		t.Fatalf("schema should remain usable after reset: %v", err)
	}
	assertBatchRowCounts(t, s, 1, 1, 1)
}
