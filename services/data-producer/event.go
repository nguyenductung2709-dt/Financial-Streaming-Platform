package main

import (
	"errors"
	marketv1 "financial-streaming-platform/gen/market/v1"
	"fmt"
	"strconv"
	"strings"
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Reject rows only when neither date column supplies a date for the price update.
var errMissingTradingDate = errors.New("Trading date and Date are empty")

func rowToPriceEvent(row SourceRow) (*marketv1.PriceEvent, bool, error) {
	columnHeaders := row.Columns
	rowValues := row.Values
	line := row.Line

	// Read Last using column map, if empty or zero -> skip, parse positive price into millionths
	lastPriceText := strings.TrimSpace(rowValues[columnHeaders["Last"]])
	if lastPriceText == "" {
		return nil, true, nil
	}
	lastPriceMicros, err := parsePriceMicros(lastPriceText)
	if err != nil {
		return nil, false, fmt.Errorf("parse Last %q: %w", lastPriceText, err)
	}
	if lastPriceMicros == 0 {
		return nil, true, nil
	}

	// Read and trim ID
	symbol := strings.TrimSpace(rowValues[columnHeaders["ID"]])
	if symbol == "" {
		return nil, false, fmt.Errorf("ID is empty")
	}

	// Read and map SecType
	secTypeText := strings.TrimSpace(rowValues[columnHeaders["SecType"]])
	secType, err := parseSecurityType(secTypeText)
	if err != nil {
		return nil, false, fmt.Errorf("parse secType %q: %w", secTypeText, err)
	}

	// Read trading date and trading time and convert to Protobuf timestamp
	tradingTime := strings.TrimSpace(rowValues[columnHeaders["Trading time"]])
	tradingDate := strings.TrimSpace(rowValues[columnHeaders["Trading date"]])
	// Index updates often omit Trading date. Use the row's system Date as a
	// fallback for these price updates, keeping the existing Trading time.
	if tradingDate == "" {
		if index, ok := columnHeaders["Date"]; ok && index >= 0 && index < len(rowValues) {
			tradingDate = strings.TrimSpace(rowValues[index])
		}
	}
	if tradingDate == "" {
		return nil, false, fmt.Errorf("parse trading date %q and time %q: %w", tradingDate, tradingTime, errMissingTradingDate)
	}
	tradingTimeEvent, err := parseTradeTime(tradingDate, tradingTime)
	if err != nil {
		return nil, false, fmt.Errorf("parse trading date %q and time %q: %w", tradingDate, tradingTime, err)
	}

	// Build the event
	priceEvent := marketv1.PriceEvent{
		Symbol:          symbol,
		SecurityType:    secType,
		LastPriceMicros: lastPriceMicros,
		TradeTime:       tradingTimeEvent,
		SourceRow:       line,
	}

	return &priceEvent, false, nil
}

func parsePriceMicros(priceText string) (uint64, error) {
	const scale uint64 = 1_000_000
	priceText = strings.TrimSpace(priceText)
	wholeText, fractionText, hasDecimal := strings.Cut(priceText, ".")
	if wholeText == "" || (hasDecimal && fractionText == "") {
		return 0, fmt.Errorf("price must contain digits before and after any decimal point")
	}
	for _, digit := range wholeText + fractionText {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("price must be a nonnegative decimal number")
		}
	}
	if len(fractionText) > 6 {
		return 0, fmt.Errorf("price has more than six decimal places")
	}

	whole, err := strconv.ParseUint(wholeText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse whole price: %w", err)
	}
	// Pad the fractional digits to six places: "45" becomes "450000".
	fractionText += strings.Repeat("0", 6-len(fractionText))
	fraction, err := strconv.ParseUint(fractionText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse fractional price: %w", err)
	}

	// Check before multiplying or adding so an oversized price cannot wrap.
	if whole > (^uint64(0)-fraction)/scale {
		return 0, fmt.Errorf("price in millionths exceeds uint64 range")
	}
	return whole*scale + fraction, nil
}

func parseTradeTime(dateText string, timeText string) (*timestamppb.Timestamp, error) {
	layout := "02-01-2006 15:04:05.000"
	location := time.FixedZone("CEST", 2*60*60)

	// convert dateText and timeText to time.Time with proper layout
	tradingTime, err := time.ParseInLocation(layout, dateText+" "+timeText, location)
	if err != nil {
		return nil, fmt.Errorf("parse trade time: %w", err)
	}

	// convert time.Time into timestamppb for Protobuf
	timestamp := timestamppb.New(tradingTime)
	if err := timestamp.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid trade timestamp: %w", err)
	}

	return timestamp, nil
}

func parseSecurityType(secTypeText string) (marketv1.SecurityType, error) {
	switch secTypeText {
	case "E":
		return marketv1.SecurityType_SECURITY_TYPE_EQUITY, nil
	case "I":
		return marketv1.SecurityType_SECURITY_TYPE_INDEX, nil
	default:
		return marketv1.SecurityType_SECURITY_TYPE_UNSPECIFIED, fmt.Errorf("invalid SecType: %q", secTypeText)
	}
}
