package mapper_test

import (
	"reflect"
	"strings"
	"testing"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"
	"financial-streaming-platform/services/materializer/internal/mapper"
)

func advisoryMessage() *marketv1.TradeAdvisory {
	return &marketv1.TradeAdvisory{
		EmaWindow:      windowMessage(),
		Action:         marketv1.AdvisoryAction_ADVISORY_ACTION_BUY,
		PreviousEma38:  "0",
		PreviousEma100: "0.1234567890123456789012345678901234",
	}
}

func TestDecodeTradeAdvisoryPreservesFields(t *testing.T) {
	for _, action := range []struct {
		proto marketv1.AdvisoryAction
		model data.AdvisoryAction
	}{
		{marketv1.AdvisoryAction_ADVISORY_ACTION_BUY, data.AdvisoryActionBuy},
		{marketv1.AdvisoryAction_ADVISORY_ACTION_SELL, data.AdvisoryActionSell},
	} {
		t.Run(string(action.model), func(t *testing.T) {
			message := advisoryMessage()
			message.Action = action.proto
			payload := append(marshal(t, message), 0xa0, 0x06, 0x01)
			got, err := mapper.DecodeTradeAdvisory(payload)
			if err != nil {
				t.Fatal(err)
			}
			want := &data.TradeAdvisory{
				EmaWindow: expectedWindow(), Action: action.model,
				PreviousEma38: "0", PreviousEma100: "0.1234567890123456789012345678901234",
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded advisory = %+v; want %+v", got, want)
			}
		})
	}
}

func TestDecodeTradeAdvisoryRejectsInvalidPayloads(t *testing.T) {
	for name, payload := range map[string][]byte{
		"tombstone": nil,
		"empty":     {},
		"bad tag":   {0x00},
		"truncated": {0x0a, 0x02, 'A'},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := mapper.DecodeTradeAdvisory(payload); err == nil || got != nil {
				t.Fatalf("advisory decoder returned model=%+v error=%v", got, err)
			}
		})
	}
}

func TestDecodeTradeAdvisoryRejectsInvalidFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*marketv1.TradeAdvisory)
		field  string
	}{
		{"missing window", func(a *marketv1.TradeAdvisory) { a.EmaWindow = nil }, "advisory window"},
		{"invalid embedded window", func(a *marketv1.TradeAdvisory) { a.EmaWindow.ClosingPriceMicros = 0 }, "closing price"},
		{"missing embedded timestamp", func(a *marketv1.TradeAdvisory) { a.EmaWindow.WindowStart = nil }, "window_start"},
		{"unspecified action", func(a *marketv1.TradeAdvisory) { a.Action = marketv1.AdvisoryAction_ADVISORY_ACTION_UNSPECIFIED }, "action"},
		{"unknown action", func(a *marketv1.TradeAdvisory) { a.Action = 99 }, "action"},
		{"missing previous short EMA", func(a *marketv1.TradeAdvisory) { a.PreviousEma38 = "" }, "previous_ema38"},
		{"invalid previous long EMA", func(a *marketv1.TradeAdvisory) { a.PreviousEma100 = "Infinity" }, "previous_ema100"},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := advisoryMessage()
			test.change(message)
			got, err := mapper.DecodeTradeAdvisory(marshal(t, message))
			if err == nil || got != nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("model=%+v error=%v; want rejection mentioning %s", got, err, test.field)
			}
		})
	}
}
