package data

import (
	"context"
	"strings"
	"testing"
	"time"
)

func testWindow() *EmaWindow {
	start := time.Date(2021, 11, 12, 6, 0, 0, 0, time.UTC)
	return &EmaWindow{
		Symbol: "RUI.FR", WindowStart: start, WindowEnd: start.Add(5 * time.Minute),
		ClosingPriceMicros: ^uint64(0), Ema38: "1.234567890123456789012345678901234", Ema100: "0.000000000000000000000000000000001",
	}
}

func TestValidateWindow(t *testing.T) {
	if err := validateWindow(testWindow()); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*EmaWindow){
		"empty symbol":      func(w *EmaWindow) { w.Symbol = "" },
		"untrimmed symbol":  func(w *EmaWindow) { w.Symbol = " RUI.FR" },
		"missing start":     func(w *EmaWindow) { w.WindowStart = time.Time{} },
		"missing end":       func(w *EmaWindow) { w.WindowEnd = time.Time{} },
		"invalid timestamp": func(w *EmaWindow) { w.WindowStart = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"wrong duration":    func(w *EmaWindow) { w.WindowEnd = w.WindowEnd.Add(time.Second) },
		"unaligned window": func(w *EmaWindow) {
			w.WindowStart = w.WindowStart.Add(time.Second)
			w.WindowEnd = w.WindowEnd.Add(time.Second)
		},
		"zero price":   func(w *EmaWindow) { w.ClosingPriceMicros = 0 },
		"empty EMA":    func(w *EmaWindow) { w.Ema38 = "" },
		"NaN":          func(w *EmaWindow) { w.Ema38 = "NaN" },
		"infinity":     func(w *EmaWindow) { w.Ema100 = "Infinity" },
		"negative EMA": func(w *EmaWindow) { w.Ema100 = "-1" },
		"exponent":     func(w *EmaWindow) { w.Ema38 = "1e3" },
	} {
		t.Run(name, func(t *testing.T) {
			w := testWindow()
			change(w)
			if err := validateWindow(w); err == nil {
				t.Fatal("expected invalid window to fail")
			}
		})
	}
}

func TestInvalidMessagesDoNotStartDatabaseWrites(t *testing.T) {
	s := &Postgres{}
	if err := s.SaveEmaWindow(context.Background(), nil); err == nil {
		t.Fatal("nil window should fail before using the pool")
	}
	for _, a := range []*TradeAdvisory{
		nil, {},
		{EmaWindow: testWindow()},
		{EmaWindow: testWindow(), Action: "UNKNOWN"},
		{EmaWindow: testWindow(), Action: AdvisoryActionBuy, PreviousEma38: "NaN", PreviousEma100: "0"},
		{EmaWindow: testWindow(), Action: AdvisoryActionSell, PreviousEma38: "0", PreviousEma100: "-1"},
	} {
		if err := s.SaveTradeAdvisory(context.Background(), a); err == nil {
			t.Fatal("invalid advisory should fail before using the pool")
		}
	}
}

func TestNewPostgresRejectsInvalidConfiguration(t *testing.T) {
	for _, connection := range []string{"", " ", "postgres://%"} {
		if s, err := NewPostgres(context.Background(), connection); err == nil || s != nil {
			t.Fatal("expected invalid connection configuration to fail")
		}
	}
}

func TestNewPostgresDoesNotExposeCredentials(t *testing.T) {
	const password = "do-not-log-this-password"
	for _, dsn := range []string{
		"postgres://financial:" + password + "@127.0.0.1:invalid/financial_streaming",
		"postgres://financial:" + password + "@127.0.0.1/%zz",
	} {
		s, err := NewPostgres(context.Background(), dsn)
		if s != nil {
			s.Close()
		}
		if err == nil {
			t.Fatal("expected invalid connection configuration")
		}
		if strings.Contains(err.Error(), password) || strings.Contains(err.Error(), dsn) {
			t.Fatal("connection error exposed the password or connection string")
		}
	}
}
