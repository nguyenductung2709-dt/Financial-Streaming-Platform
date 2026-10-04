package main

import (
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const testCSVHeader = "ID,SecType,Last,Trading time,Trading date"

func writeCSVFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func csvContents(header, data string) string {
	return strings.Repeat("# metadata\n", 11) + header + "\n# descriptions\n" + data
}

func TestStreamCSVReadsDataRows(t *testing.T) {
	// Reordered columns, padded header names, a quoted comma, and a trailing
	// empty field exercise the dataset format without needing the full CSV.
	header := "Trading date, SecType , Last ,Trading time, ID "
	path := writeCSVFile(t, csvContents(header,
		"12-11-2021,E,57.117900,09:00:00.000,\"ABC,INC\",\n\n"+
			"12-11-2021,I,,09:01:00.000,DEF,\n"))
	var rows []SourceRow
	err := StreamCSV(path, func(row SourceRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]int{
		"Trading date": 0, "SecType": 1, "Last": 2, "Trading time": 3, "ID": 4,
	}
	want := []SourceRow{
		{Line: 14, Values: []string{"12-11-2021", "E", "57.117900", "09:00:00.000", "ABC,INC", ""}, Columns: columns},
		{Line: 16, Values: []string{"12-11-2021", "I", "", "09:01:00.000", "DEF", ""}, Columns: columns},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v; want %#v", rows, want)
	}
}

func TestStreamCSVTracksMultilineRecords(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"\"ABC\nINC\",E,1,09:00:00.000,12-11-2021\n"+
			"DEF,I,2,09:01:00.000,12-11-2021\n"))
	var lines []uint64
	err := StreamCSV(path, func(row SourceRow) error {
		lines = append(lines, row.Line)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint64{14, 16}; !reflect.DeepEqual(lines, want) {
		t.Fatalf("lines = %v; want %v", lines, want)
	}
}

func TestStreamCSVRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		message  string
		cause    error
	}{
		{name: "empty file", message: "CSV header is missing"},
		{
			name:     "header not on line 12",
			contents: csvContents("", "ABC,E,1,09:00:00.000,12-11-2021\n"),
			message:  "CSV header is missing at line 12",
		},
		{
			name:     "required column missing",
			contents: csvContents("ID,SecType,Trading time,Trading date", ""),
			message:  "missing column \"Last\"",
		},
		{
			name:     "short data row",
			contents: csvContents(testCSVHeader, "ABC,E,1\nDEF,I,2,09:01:00.000,12-11-2021\n"),
			message:  "CSV line 14 is missing required fields",
		},
		{
			name:     "invalid quoting",
			contents: csvContents(testCSVHeader, "ABC\"bad,E,1,09:00:00.000,12-11-2021\n"),
			message:  "read CSV",
			cause:    csv.ErrBareQuote,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := StreamCSV(writeCSVFile(t, tt.contents), func(SourceRow) error {
				calls++
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("error = %v; want error containing %q", err, tt.message)
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Fatalf("error = %v; want wrapped %v", err, tt.cause)
			}
			if calls != 0 {
				t.Fatalf("handler called %d times for invalid input", calls)
			}
		})
	}
}

func TestStreamCSVStopsOnHandlerError(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"ABC,E,1,09:00:00.000,12-11-2021\nDEF,I,2,09:01:00.000,12-11-2021\n"))
	sentinel := errors.New("handler failed")
	calls := 0
	err := StreamCSV(path, func(SourceRow) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v; want wrapped handler error", err)
	}
	if !strings.Contains(err.Error(), "CSV line 14") {
		t.Fatalf("error has no source line: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times; want 1", calls)
	}
}

func TestStreamCSVMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.csv")
	err := StreamCSV(path, func(SourceRow) error {
		t.Fatal("handler called for missing file")
		return nil
	})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v; want wrapped os.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error has no file path: %v", err)
	}
}

func TestStreamCSVHeaderWithoutData(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, ""))
	err := StreamCSV(path, func(SourceRow) error {
		t.Fatal("handler called when no data rows exist")
		return nil
	})
	if err != nil {
		t.Fatalf("error = %v; want success at EOF", err)
	}
}
