package mapper_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"
	"financial-streaming-platform/services/materializer/internal/mapper"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func windowMessage() *marketv1.EmaWindow {
	start := time.Date(2021, 11, 12, 6, 0, 0, 0, time.UTC)
	return &marketv1.EmaWindow{
		Symbol:             "RUI.FR",
		WindowStart:        timestamppb.New(start),
		WindowEnd:          timestamppb.New(start.Add(5 * time.Minute)),
		ClosingPriceMicros: ^uint64(0),
		Ema38:              "1.234567890123456789012345678901234",
		Ema100:             "0.0000000000000000000000000000000010",
	}
}

func marshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func expectedWindow() *data.EmaWindow {
	return &data.EmaWindow{
		Symbol:             "RUI.FR",
		WindowStart:        time.Date(2021, 11, 12, 6, 0, 0, 0, time.UTC),
		WindowEnd:          time.Date(2021, 11, 12, 6, 5, 0, 0, time.UTC),
		ClosingPriceMicros: ^uint64(0),
		Ema38:              "1.234567890123456789012345678901234",
		Ema100:             "0.0000000000000000000000000000000010",
	}
}

func TestDecodeEmaWindowPreservesFields(t *testing.T) {
	payload := marshal(t, windowMessage())
	// A future producer may add fields; valid known fields must still decode.
	payload = append(payload, 0xa0, 0x06, 0x01) // Field 100, varint 1.
	got, err := mapper.DecodeEmaWindow(payload)
	if err != nil {
		t.Fatal(err)
	}
	if want := expectedWindow(); !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded window = %+v; want %+v", got, want)
	}
}

func TestDecodeEmaWindowRejectsInvalidPayloads(t *testing.T) {
	for name, payload := range map[string][]byte{
		"tombstone": nil,
		"empty":     {},
		"bad tag":   {0x00},
		"truncated": {0x0a, 0x02, 'A'},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := mapper.DecodeEmaWindow(payload); err == nil || got != nil {
				t.Fatalf("EMA decoder returned model=%+v error=%v", got, err)
			}
		})
	}
}

func TestDecodeEmaWindowRejectsInvalidFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*marketv1.EmaWindow)
		field  string
	}{
		{"missing start", func(w *marketv1.EmaWindow) { w.WindowStart = nil }, "window_start"},
		{"missing end", func(w *marketv1.EmaWindow) { w.WindowEnd = nil }, "window_end"},
		{"start before range", func(w *marketv1.EmaWindow) { w.WindowStart.Seconds = -62135596801 }, "window_start"},
		{"end after range", func(w *marketv1.EmaWindow) { w.WindowEnd.Seconds = 253402300800 }, "window_end"},
		{"negative nanos", func(w *marketv1.EmaWindow) { w.WindowStart.Nanos = -1 }, "window_start"},
		{"overflow nanos", func(w *marketv1.EmaWindow) { w.WindowEnd.Nanos = 1000000000 }, "window_end"},
		{"empty symbol", func(w *marketv1.EmaWindow) { w.Symbol = "" }, "symbol"},
		{"untrimmed symbol", func(w *marketv1.EmaWindow) { w.Symbol = " RUI.FR" }, "symbol"},
		{"wrong duration", func(w *marketv1.EmaWindow) { w.WindowEnd.Seconds++ }, "window"},
		{"unaligned window", func(w *marketv1.EmaWindow) {
			w.WindowStart.Seconds++
			w.WindowEnd.Seconds++
		}, "window"},
		{"fractional boundaries", func(w *marketv1.EmaWindow) {
			w.WindowStart.Nanos = 1
			w.WindowEnd.Nanos = 1
		}, "window"},
		{"zero price", func(w *marketv1.EmaWindow) { w.ClosingPriceMicros = 0 }, "closing price"},
		{"missing short EMA", func(w *marketv1.EmaWindow) { w.Ema38 = "" }, "EMA"},
		{"invalid long EMA", func(w *marketv1.EmaWindow) { w.Ema100 = "NaN" }, "EMA"},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := windowMessage()
			test.change(message)
			got, err := mapper.DecodeEmaWindow(marshal(t, message))
			if err == nil || got != nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("model=%+v error=%v; want rejection mentioning %s", got, err, test.field)
			}
		})
	}
}
