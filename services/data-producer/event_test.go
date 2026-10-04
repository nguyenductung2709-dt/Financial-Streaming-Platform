package main

import (
	marketv1 "financial-streaming-platform/gen/market/v1"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParsePriceMicros(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{"0", 0},
		{"0.000000", 0},
		{"1", 1_000_000},
		{"123.45", 123_450_000},
		{"57.117900", 57_117_900},
		{"0.000001", 1},
		{"1.000001", 1_000_001},
		{"00057.117900", 57_117_900},
		{" 57.117900\t", 57_117_900},
		{"18446744073709.551615", ^uint64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parsePriceMicros(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("parsePriceMicros(%q) = %d; want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParsePriceMicrosRejectsInvalidPrices(t *testing.T) {
	for _, input := range []string{
		"", " ", "-1", "-0.000001", "+1", "NaN", "Inf", "1e3",
		".5", "1.", "1.2.3", "1,000", "1 2", "1.0000001",
		"18446744073709.551616", "18446744073710",
		"18446744073709551616",
	} {
		t.Run(input, func(t *testing.T) {
			if got, err := parsePriceMicros(input); err == nil {
				t.Fatalf("parsePriceMicros(%q) = %d; want an error", input, got)
			}
		})
	}
}

func TestParseTradeTime(t *testing.T) {
	tests := []struct{ name, date, clock, utc string }{
		{"milliseconds and CEST offset", "12-11-2021", "07:00:00.123", "2021-11-12T05:00:00.123Z"},
		{"previous UTC day", "12-11-2021", "00:30:00.000", "2021-11-11T22:30:00Z"},
		{"leap day", "29-02-2020", "23:59:59.999", "2020-02-29T21:59:59.999Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTradeTime(tt.date, tt.clock)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("expected a timestamp")
			}
			if err := got.CheckValid(); err != nil {
				t.Fatal(err)
			}
			if utc := got.AsTime().Format(time.RFC3339Nano); utc != tt.utc {
				t.Fatalf("UTC timestamp = %q; want %q", utc, tt.utc)
			}
		})
	}
}

func TestParseTradeTimeRejectsInvalidInput(t *testing.T) {
	tests := []struct{ name, date, clock string }{
		{"empty date", "", "07:00:00.000"},
		{"empty time", "12-11-2021", ""},
		{"wrong date format", "2021-11-12", "07:00:00.000"},
		{"invalid calendar date", "31-11-2021", "07:00:00.000"},
		{"non leap year", "29-02-2021", "07:00:00.000"},
		{"invalid hour", "12-11-2021", "24:00:00.000"},
		{"invalid minute", "12-11-2021", "07:60:00.000"},
		{"invalid second", "12-11-2021", "07:00:60.000"},
		{"missing milliseconds", "12-11-2021", "07:00:00"},
		{"outside protobuf range", "01-01-0000", "07:00:00.000"},
		{"UTC instant before protobuf range", "01-01-0001", "00:00:00.000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTradeTime(tt.date, tt.clock)
			if err == nil || got != nil {
				t.Fatalf("got timestamp %v, error %v; want nil timestamp and error", got, err)
			}
		})
	}
}

func TestParseSecurityType(t *testing.T) {
	tests := []struct {
		input     string
		want      marketv1.SecurityType
		wantError bool
	}{
		{"E", marketv1.SecurityType_SECURITY_TYPE_EQUITY, false},
		{"I", marketv1.SecurityType_SECURITY_TYPE_INDEX, false},
		{"", marketv1.SecurityType_SECURITY_TYPE_UNSPECIFIED, true},
		{"X", marketv1.SecurityType_SECURITY_TYPE_UNSPECIFIED, true},
		{"e", marketv1.SecurityType_SECURITY_TYPE_UNSPECIFIED, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSecurityType(tt.input)
			if got != tt.want || (err != nil) != tt.wantError {
				t.Fatalf("got (%v, %v); want type %v, error %v", got, err, tt.want, tt.wantError)
			}
		})
	}
}

// Reordered columns ensure conversion uses the header map, not fixed offsets.
func validEventSourceRow() SourceRow {
	return SourceRow{
		Line:    71436,
		Values:  []string{" 12-11-2021 ", " 57.117900 ", " SAMPLE.ETR ", " 07:00:00.123 ", " E "},
		Columns: map[string]int{"Trading date": 0, "Last": 1, "ID": 2, "Trading time": 3, "SecType": 4},
	}
}

func TestRowToPriceEvent(t *testing.T) {
	for _, security := range []struct {
		text  string
		value marketv1.SecurityType
	}{
		{" E ", marketv1.SecurityType_SECURITY_TYPE_EQUITY},
		{" I ", marketv1.SecurityType_SECURITY_TYPE_INDEX},
	} {
		t.Run(security.text, func(t *testing.T) {
			row := validEventSourceRow()
			row.Values[row.Columns["SecType"]] = security.text
			got, skip, err := rowToPriceEvent(row)
			if err != nil || skip || got == nil {
				t.Fatalf("got event %v, skip %v, error %v; want event without skip or error", got, skip, err)
			}
			want := &marketv1.PriceEvent{
				Symbol: "SAMPLE.ETR", SecurityType: security.value,
				LastPriceMicros: 57_117_900, SourceRow: 71436,
				TradeTime: timestamppb.New(time.Date(2021, 11, 12, 5, 0, 0, 123_000_000, time.UTC)),
			}
			if !proto.Equal(got, want) {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}

func TestRowToPriceEventSkipsNonPriceRows(t *testing.T) {
	for _, price := range []string{"", " ", "0", "0.000000"} {
		t.Run(price, func(t *testing.T) {
			row := validEventSourceRow()
			row.Values[row.Columns["Last"]] = price
			// Non-price rows may have no symbol, security type, or trade timestamp.
			for _, field := range []string{"ID", "SecType", "Trading date", "Trading time"} {
				row.Values[row.Columns[field]] = ""
			}
			got, skip, err := rowToPriceEvent(row)
			if got != nil || !skip || err != nil {
				t.Fatalf("got event %v, skip %v, error %v; want nil, true, nil", got, skip, err)
			}
		})
	}
}

func TestRowToPriceEventRejectsInvalidFields(t *testing.T) {
	tests := []struct{ name, field, value, errorText string }{
		{"malformed price", "Last", "abc", "parse Last"},
		{"negative price", "Last", "-1", "parse Last"},
		{"overflow price", "Last", "18446744073709.551616", "parse Last"},
		{"empty symbol", "ID", " ", "ID is empty"},
		{"empty security type", "SecType", "", "parse secType"},
		{"unknown security type", "SecType", "X", "parse secType"},
		{"empty date", "Trading date", "", "parse trading date"},
		{"invalid date", "Trading date", "31-11-2021", "parse trading date"},
		{"empty time", "Trading time", "", "parse trading date"},
		{"invalid time", "Trading time", "24:00:00.000", "parse trading date"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := validEventSourceRow()
			row.Values[row.Columns[tt.field]] = tt.value
			got, skip, err := rowToPriceEvent(row)
			if got != nil || skip || err == nil {
				t.Fatalf("got event %v, skip %v, error %v; want nil, false, error", got, skip, err)
			}
			if !strings.Contains(err.Error(), tt.errorText) {
				t.Fatalf("error %q does not identify %q", err, tt.errorText)
			}
		})
	}
}

// These complete rows were copied unchanged from debs2022-gc-trading-day-12-11-21.csv.
// Tests use these literals and never open the dataset file.
func TestRealCSVRows(t *testing.T) {
	columns := map[string]int{
		"ID":                  0,
		"SecType":             1,
		"Date":                2,
		"Time":                3,
		"Ask":                 4,
		"Ask volume":          5,
		"Bid":                 6,
		"Bid volume":          7,
		"Ask time":            8,
		"Day's high ask":      9,
		"Close":               10,
		"Currency":            11,
		"Day's high ask time": 12,
		"Day's high":          13,
		"ISIN":                14,
		"Auction price":       15,
		"Day's low ask":       16,
		"Day's low":           17,
		"Day's low ask time":  18,
		"Open":                19,
		"Nominal value":       20,
		"Last":                21,
		"Last volume":         22,
		"Trading time":        23,
		"Total volume":        24,
		"Mid price":           25,
		"Trading date":        26,
		"Profit":              27,
		"Current price":       28,
		"Related indices":     29,
		"Day high bid time":   30,
		"Day low bid time":    31,
		"Open Time":           32,
		"Last trade time":     33,
		"Close Time":          34,
		"Day high Time":       35,
		"Day low Time":        36,
		"Bid time":            37,
		"Auction Time":        38,
	}
	tests := []struct {
		name      string
		row       SourceRow
		price     uint64
		security  marketv1.SecurityType
		wantSkip  bool
		wantError string
		wantUTC   string
	}{
		{
			name: "empty_line_14",
			row: SourceRow{Line: 14, Columns: columns, Values: []string{
				"A0HN5C.ETR", "E", "12-11-2021", "01:01:24.605", "", "", "", "",
				"", "", "", "", "", "", "", "",
				"", "", "", "", "", "", "", "",
				"", "", "", "", "", "", "", "",
				"", "", "", "", "", "", "", "",
			}},
			price: 0, security: marketv1.SecurityType_SECURITY_TYPE_EQUITY,
			wantSkip: true,
		},
		{
			name: "zero_line_376",
			row: SourceRow{Line: 376, Columns: columns, Values: []string{
				"FREN4.FR", "I", "12-11-2021", "00:00:00.000", "", "", "", "",
				"", "", "0.000000", "", "", "0.000000", "", "",
				"", "0.000000", "", "0.000000", "", "0.000000", "", "00:00:00.000",
				"", "", "", "", "", "", "", "",
				"00:00:00.000", "00:00:00.000", "00:00:00.000", "00:00:00.000", "00:00:00.000", "", "", "",
			}},
			price: 0, security: marketv1.SecurityType_SECURITY_TYPE_INDEX,
			wantSkip: true,
		},
		{
			name: "missing_date_line_71436",
			row: SourceRow{Line: 71436, Columns: columns, Values: []string{
				"IIIFL.FR", "I", "12-11-2021", "07:00:01.000", "", "", "", "",
				"", "", "", "", "", "57.117900", "", "",
				"", "57.117900", "", "57.117900", "", "57.117900", "", "07:00:01.014",
				"", "", "", "", "", "", "", "",
				"", "07:00:01.000", "", "", "", "", "", "",
			}},
			price: 57117900, security: marketv1.SecurityType_SECURITY_TYPE_INDEX,
			wantError: "parse trading date",
		},
		{
			name: "E_line_728610",
			row: SourceRow{Line: 728610, Columns: columns, Values: []string{
				"RUI.FR", "E", "12-11-2021", "09:00:00.000", "", "", "", "",
				"", "", "", "", "", "28.040000", "", "",
				"", "28.040000", "", "28.040000", "", "28.040000", "27", "09:00:00.179",
				"27", "", "12-11-2021", "", "", "", "", "",
				"", "09:00:00.000", "", "", "", "", "", "",
			}},
			price: 28040000, security: marketv1.SecurityType_SECURITY_TYPE_EQUITY,
			wantUTC: "2021-11-12T07:00:00.179Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := tt.row.Values
			security, err := parseSecurityType(values[columns["SecType"]])
			if err != nil || security != tt.security {
				t.Fatalf("security = %v, error = %v; want %v", security, err, tt.security)
			}
			if priceText := values[columns["Last"]]; priceText != "" {
				price, err := parsePriceMicros(priceText)
				if err != nil || price != tt.price {
					t.Fatalf("price = %d, error = %v; want %d", price, err, tt.price)
				}
			}
			if tt.wantUTC != "" {
				timestamp, err := parseTradeTime(values[columns["Trading date"]], values[columns["Trading time"]])
				if err != nil {
					t.Fatal(err)
				}
				if got := timestamp.AsTime().Format(time.RFC3339Nano); got != tt.wantUTC {
					t.Fatalf("UTC timestamp = %q; want %q", got, tt.wantUTC)
				}
			}
			got, skip, err := rowToPriceEvent(tt.row)
			if tt.wantError != "" {
				if got != nil || skip || err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("got event %v, skip %v, error %v; want nil, false, error containing %q", got, skip, err, tt.wantError)
				}
				return
			}
			if err != nil || skip != tt.wantSkip {
				t.Fatalf("skip = %v, error = %v; want skip %v and no error", skip, err, tt.wantSkip)
			}
			if tt.wantSkip {
				if got != nil {
					t.Fatalf("skipped row produced event %v", got)
				}
				return
			}
			want := &marketv1.PriceEvent{
				Symbol: "RUI.FR", SecurityType: tt.security, LastPriceMicros: tt.price,
				SourceRow: tt.row.Line,
				TradeTime: timestamppb.New(time.Date(2021, 11, 12, 7, 0, 0, 179_000_000, time.UTC)),
			}
			if !proto.Equal(got, want) {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}
