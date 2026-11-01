package mapper

import (
	"errors"
	"fmt"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"

	"google.golang.org/protobuf/proto"
)

// DecodeTradeAdvisory converts a raw trade-advisories Protobuf value into a validated model.
func DecodeTradeAdvisory(payload []byte) (*data.TradeAdvisory, error) {
	if len(payload) == 0 {
		return nil, errors.New("trade advisory payload is empty")
	}
	var message marketv1.TradeAdvisory
	if err := proto.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("decode trade advisory: %w", err)
	}
	window, err := emaWindowFromProto(message.EmaWindow)
	if err != nil {
		return nil, fmt.Errorf("advisory window: %w", err)
	}
	var action data.AdvisoryAction
	switch message.Action {
	case marketv1.AdvisoryAction_ADVISORY_ACTION_BUY:
		action = data.AdvisoryActionBuy
	case marketv1.AdvisoryAction_ADVISORY_ACTION_SELL:
		action = data.AdvisoryActionSell
	default:
		return nil, fmt.Errorf("unsupported advisory action %d", message.Action)
	}
	advisory := &data.TradeAdvisory{
		EmaWindow:      window,
		Action:         action,
		PreviousEma38:  message.PreviousEma38,
		PreviousEma100: message.PreviousEma100,
	}
	if err := advisory.Validate(); err != nil {
		return nil, fmt.Errorf("trade advisory: %w", err)
	}
	return advisory, nil
}
