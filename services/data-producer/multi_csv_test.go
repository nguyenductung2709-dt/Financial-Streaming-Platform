package main

import (
	"bytes"
	"context"
	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func runCSVInputs(t *testing.T, args ...string) ([]*marketv1.PriceEvent, string, error) {
	t.Helper()
	cfg, err := parseConfig(append([]string{"-topic=trade-events"}, args...), io.Discard)
	if err != nil {
		return nil, "", err
	}
	client := &fakeRunClient{}
	var output bytes.Buffer
	err = runWithClient(context.Background(), cfg, client, &output)
	var events []*marketv1.PriceEvent
	for _, record := range client.records {
		event := &marketv1.PriceEvent{}
		if decodeErr := proto.Unmarshal(record.Value, event); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		events = append(events, event)
	}
	return events, output.String(), err
}

func TestMultipleCSVFlagsPreserveOrderAndReadOwnHeaders(t *testing.T) {
	first := writeCSVFile(t, csvContents(testCSVHeader,
		"ABC,E,1,09:00:00.000,12-11-2021\n"))
	second := writeCSVFile(t, csvContents("Last,ID,Trading date,Trading time,SecType",
		"2,ABC,12-11-2021,09:00:00.000,E\n"))
	events, summary, err := runCSVInputs(t, "-csvPath="+first, "-csvPath="+second)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%v; want both files", events)
	}
	if events[0].LastPriceMicros != 1_000_000 || events[1].LastPriceMicros != 2_000_000 ||
		events[0].SourceRow != 14 || events[1].SourceRow != 28 {
		t.Fatalf("file ordering/headers/source rows lost: %v", events)
	}
	if !strings.Contains(summary, "published=2") {
		t.Fatalf("summary=%q", summary)
	}
}

func TestCSVDirectoryIsAlphabeticalAndNonrecursive(t *testing.T) {
	dir := t.TempDir()
	for name, symbol := range map[string]string{"b.csv": "BBB", "a.csv": "AAA", "ignored.txt": "TXT"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(csvContents(testCSVHeader,
			symbol+",E,1,09:00:00.000,12-11-2021\n")), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "nested.csv"), 0700); err != nil {
		t.Fatal(err)
	}
	events, _, err := runCSVInputs(t, "-csvPath="+dir)
	if err != nil {
		t.Fatal(err)
	}
	var symbols []string
	for _, event := range events {
		symbols = append(symbols, event.Symbol)
	}
	if !reflect.DeepEqual(symbols, []string{"AAA", "BBB"}) {
		t.Fatalf("symbols=%v; want alphabetical CSV files only", symbols)
	}
}

func TestMultiCSVLimitAndRateContinueAcrossFiles(t *testing.T) {
	first := writeCSVFile(t, csvContents(testCSVHeader, validRunRow))
	second := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+validRunRow))
	cfg, err := parseConfig([]string{"-csvPath=" + first, "-csvPath=" + second, "-maxEvents=2", "-rate=50"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeRunClient{}
	var output bytes.Buffer
	if err := runWithClient(context.Background(), cfg, client, &output); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 2 || !strings.Contains(output.String(), "limit reached: published=2") {
		t.Fatalf("records=%d summary=%q", len(client.records), output.String())
	}
	var secondEvent marketv1.PriceEvent
	if err := proto.Unmarshal(client.records[1].Value, &secondEvent); err != nil {
		t.Fatal(err)
	}
	if secondEvent.SourceRow != 28 {
		t.Fatalf("limit should span files; second source_row=%d", secondEvent.SourceRow)
	}
	if gap := client.produceTimes[1].Sub(client.produceTimes[0]); gap < 20*time.Millisecond {
		t.Fatalf("rate reset at file boundary: %s", gap)
	}
}

func TestMultiCSVMissingLaterPathPublishesNothing(t *testing.T) {
	first := writeCSVFile(t, csvContents(testCSVHeader, validRunRow))
	missing := filepath.Join(t.TempDir(), "missing.csv")
	events, _, err := runCSVInputs(t, "-csvPath="+first, "-csvPath="+missing)
	if err == nil || !strings.Contains(err.Error(), missing) || len(events) != 0 {
		t.Fatalf("events=%v error=%v; want missing path before any publication", events, err)
	}
}

func TestMultiCSVRejectsEmptyDirectoryAndEmptyArguments(t *testing.T) {
	for _, args := range [][]string{
		{"-csvPath=" + t.TempDir()},
		{"-csvPath=", "-csvPath=" + writeCSVFile(t, csvContents(testCSVHeader, validRunRow))},
	} {
		events, _, err := runCSVInputs(t, args...)
		if err == nil || len(events) != 0 {
			t.Fatalf("args=%v events=%v error=%v", args, events, err)
		}
	}
}

func TestCSVSourceRowsIncludeMultilineAndTrailingBlankLines(t *testing.T) {
	first := writeCSVFile(t, csvContents(testCSVHeader,
		"\"ABC\nINC\",E,1,09:00:00.000,12-11-2021\n\n"))
	second := writeCSVFile(t, csvContents(testCSVHeader, validRunRow))
	events, _, err := runCSVInputs(t, "-csvPath="+first, "-csvPath="+second)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].SourceRow != 14 || events[1].SourceRow != 30 {
		t.Fatalf("wrong cumulative physical rows: %v", events)
	}
}

func TestCSVDirectoryIncludesDotCSVFilename(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".csv"), []byte(csvContents(testCSVHeader, validRunRow)), 0600); err != nil {
		t.Fatal(err)
	}
	events, _, err := runCSVInputs(t, "-csvPath="+dir)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%v error=%v; want .csv file included", events, err)
	}
}
