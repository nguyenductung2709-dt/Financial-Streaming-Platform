package data

import (
	"context"
	"errors"
	"testing"
)

func TestSaveBatchValidatesAllResultsBeforeWriting(t *testing.T) {
	invalidWindow := testWindow()
	invalidWindow.ClosingPriceMicros = 0
	for name, invalid := range map[string]Result{
		"nil result":     nil,
		"nil window":     (*EmaWindow)(nil),
		"nil advisory":   (*TradeAdvisory)(nil),
		"invalid window": invalidWindow,
		"invalid advisory": &TradeAdvisory{
			EmaWindow: testWindow(), Action: "UNKNOWN", PreviousEma38: "0", PreviousEma100: "0",
		},
	} {
		t.Run(name, func(t *testing.T) {
			// A valid first result must not start a transaction before a later
			// result is checked. This store intentionally has no database pool.
			s := &Postgres{}
			err := s.SaveBatch(context.Background(), []Result{testWindow(), invalid})
			var batchErr *BatchError
			if !errors.As(err, &batchErr) || batchErr.Index != 1 {
				t.Fatalf("error = %v; want batch error at index 1", err)
			}
		})
	}
}

func TestSaveBatchEmptyDoesNotOpenTransaction(t *testing.T) {
	s := &Postgres{}
	if err := s.SaveBatch(context.Background(), nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

func TestSaveBatchRejectsInvalidOverwrittenResults(t *testing.T) {
	invalidWindow := testWindow()
	invalidWindow.ClosingPriceMicros = 0
	invalidAdvice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionBuy,
		PreviousEma38: "NaN", PreviousEma100: "0"}
	validAdvice := &TradeAdvisory{EmaWindow: testWindow(), Action: AdvisoryActionSell,
		PreviousEma38: "0", PreviousEma100: "0"}
	for name, results := range map[string][]Result{
		"window":   {invalidWindow, testWindow()},
		"advisory": {invalidAdvice, validAdvice},
	} {
		t.Run(name, func(t *testing.T) {
			s := &Postgres{}
			err := s.SaveBatch(context.Background(), results)
			var batchErr *BatchError
			if !errors.As(err, &batchErr) || batchErr.Index != 0 {
				t.Fatalf("error = %v; want overwritten input rejected at index 0", err)
			}
		})
	}
}
