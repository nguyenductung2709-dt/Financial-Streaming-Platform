package mapper

import (
	"errors"
	"fmt"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"

	"google.golang.org/protobuf/proto"
)

// DecodeEmaWindow converts a raw ema-windows Protobuf value into a validated model.
func DecodeEmaWindow(payload []byte) (*data.EmaWindow, error) {
	if len(payload) == 0 {
		return nil, errors.New("EMA window payload is empty")
	}
	var message marketv1.EmaWindow
	if err := proto.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("decode EMA window: %w", err)
	}
	return emaWindowFromProto(&message)
}

func emaWindowFromProto(message *marketv1.EmaWindow) (*data.EmaWindow, error) {
	if message == nil {
		return nil, errors.New("EMA window is missing")
	}
	// Check before AsTime, which normalizes invalid seconds and nanoseconds.
	if err := message.WindowStart.CheckValid(); err != nil {
		return nil, fmt.Errorf("window_start: %w", err)
	}
	if err := message.WindowEnd.CheckValid(); err != nil {
		return nil, fmt.Errorf("window_end: %w", err)
	}
	window := &data.EmaWindow{
		Symbol:             message.Symbol,
		WindowStart:        message.WindowStart.AsTime(),
		WindowEnd:          message.WindowEnd.AsTime(),
		ClosingPriceMicros: message.ClosingPriceMicros,
		Ema38:              message.Ema38,
		Ema100:             message.Ema100,
	}
	if err := window.Validate(); err != nil {
		return nil, fmt.Errorf("EMA window: %w", err)
	}
	return window, nil
}
