package main

import (
	"bytes"
	"context"
	"errors"
	marketv1 "financial-streaming-platform/gen/market/v1"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want config
	}{
		{"defaults", nil, config{
			csvPath: "dataset/debs2022-gc-trading-day-12-11-21.csv",
			brokers: []string{"localhost:19092"}, topic: "trade-events",
		}},
		{"custom flags and trimmed addresses", []string{"-csvPath=/tmp/my data.csv", "-brokers= broker1:9092, broker2:9092 ", "-topic= prices "}, config{
			csvPath: "/tmp/my data.csv", brokers: []string{"broker1:9092", "broker2:9092"}, topic: "prices",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(tt.args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("config = %+v; want %+v", got, tt.want)
			}
		})
	}
}

func TestParseConfigRejectsInvalidFlags(t *testing.T) {
	for _, arg := range []string{"-csvPath=", "-csvPath= ", "-topic=", "-topic= ", "-brokers=", "-brokers= ", "-brokers=broker:9092,", "-brokers=,broker:9092", "-unknown=value", "-rate=-1", "-rate=1000000001", "-rate=abc", "-max-events=-1", "-max-events=abc"} {
		t.Run(arg, func(t *testing.T) {
			if _, err := parseConfig([]string{arg}, io.Discard); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestParseConfigHelp(t *testing.T) {
	var output bytes.Buffer
	_, err := parseConfig([]string{"-h"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("error = %v; want flag.ErrHelp", err)
	}
	for _, name := range []string{"-csvPath", "-brokers", "-topic", "-rate", "-max-events"} {
		if !strings.Contains(output.String(), name) {
			t.Fatalf("help omits %s", name)
		}
	}
}

type fakeRunClient struct {
	records         []*kgo.Record
	pingErr         error
	produceErr      error
	pingCalls       int
	closeCalls      int
	pingHasDeadline bool
	afterProduce    func()
	produceTimes    []time.Time
}

func (f *fakeRunClient) Ping(ctx context.Context) error {
	f.pingCalls++
	_, f.pingHasDeadline = ctx.Deadline()
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.pingErr
}
func (f *fakeRunClient) Close() { f.closeCalls++ }
func (f *fakeRunClient) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.produceTimes = append(f.produceTimes, time.Now())
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: f.produceErr}
	}
	if f.afterProduce != nil {
		f.afterProduce()
	}
	return results
}

const validRunRow = "RUI.FR,E,28.040000,09:00:00.179,12-11-2021\n"

func TestRunWithClientPublishesAndSummarizes(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+"A0HN5C.ETR,E,,,\n"+"FREN4.FR,I,0.000000,00:00:00.000,\n"))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if client.pingCalls != 1 || !client.pingHasDeadline || client.closeCalls != 1 {
		t.Fatalf("ping calls=%d, deadline=%v, close calls=%d", client.pingCalls, client.pingHasDeadline, client.closeCalls)
	}
	if len(client.records) != 1 {
		t.Fatalf("published %d records; want 1", len(client.records))
	}
	record := client.records[0]
	if record.Topic != "trade-events" || string(record.Key) != "RUI.FR" {
		t.Fatalf("unexpected record: %+v", record)
	}
	var event marketv1.PriceEvent
	if err := proto.Unmarshal(record.Value, &event); err != nil {
		t.Fatal(err)
	}
	if event.SourceRow != 14 || event.LastPriceMicros != 28_040_000 {
		t.Fatalf("unexpected event: %v", &event)
	}
	want := "CSV processing completed: published=1 skipped=2 rejected=0 topic=\"trade-events\"\n"
	if output.String() != want {
		t.Fatalf("summary = %q; want %q", output.String(), want)
	}
}

func TestRunWithClientStopsOnConversionError(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+"IIIFL.FR,I,57.117900,07:00:01.014,31-11-2021\n"+validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if err == nil || !strings.Contains(err.Error(), "CSV line 15") || !strings.Contains(err.Error(), "parse trading date") {
		t.Fatalf("expected conversion error with physical line; got %v", err)
	}
	if len(client.records) != 1 || client.closeCalls != 1 {
		t.Fatalf("records=%d, close calls=%d; want 1 each", len(client.records), client.closeCalls)
	}
	want := "CSV processing stopped: published=1 skipped=0 rejected=0 topic=\"trade-events\"\n"
	if output.String() != want {
		t.Fatalf("summary = %q; want %q", output.String(), want)
	}
}

func TestRunWithClientReturnsPublishError(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+validRunRow))
	failure := errors.New("broker rejected record")
	client := &fakeRunClient{produceErr: failure}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v; want wrapped publish error", err)
	}
	if len(client.records) != 1 || client.closeCalls != 1 {
		t.Fatalf("records=%d, close calls=%d; want 1 each", len(client.records), client.closeCalls)
	}
	if !strings.Contains(output.String(), "stopped: published=0 skipped=0") {
		t.Fatalf("failed record counted as published: %s", &output)
	}
}

func TestRunWithClientReturnsPingError(t *testing.T) {
	failure := errors.New("broker unavailable")
	client := &fakeRunClient{pingErr: failure}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: "unused.csv"}, client, &output)
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "connect to Redpanda") {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.closeCalls != 1 || len(client.records) != 0 || output.Len() != 0 {
		t.Fatal("ping failure must close client without processing CSV")
	}
}

func TestRunWithClientReturnsFileError(t *testing.T) {
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: filepath.Join(t.TempDir(), "missing.csv"), topic: "trade-events"}, client, &output)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v; want missing file error", err)
	}
	if client.closeCalls != 1 || len(client.records) != 0 {
		t.Fatal("file failure must close client without publishing")
	}
	if !strings.Contains(output.String(), "stopped: published=0 skipped=0") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientCancellationStopsBeforeNextRow(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+validRunRow))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeRunClient{afterProduce: cancel}
	var output bytes.Buffer
	err := runWithClient(ctx, config{csvPath: path, topic: "trade-events"}, client, &output)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; want context.Canceled", err)
	}
	if len(client.records) != 1 || client.closeCalls != 1 {
		t.Fatalf("records=%d, close calls=%d; want 1 each", len(client.records), client.closeCalls)
	}
	if !strings.Contains(output.String(), "stopped: published=1 skipped=0") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientHeaderWithoutData(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, ""))
	client := &fakeRunClient{}
	var output bytes.Buffer
	if err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 0 || client.closeCalls != 1 {
		t.Fatal("empty CSV must close client without publishing")
	}
	if !strings.Contains(output.String(), "completed: published=0 skipped=0") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientRejectsMissingTradingDateAndContinues(t *testing.T) {
	// Relevant columns copied from real CSV line 71436; Trading date is empty.
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"IIIFL.FR,I,57.117900,07:00:01.014,\n"+validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 || string(client.records[0].Key) != "RUI.FR" {
		t.Fatalf("expected only the following valid row to be published; got %v", client.records)
	}
	if client.closeCalls != 1 {
		t.Fatal("client was not closed")
	}
	if !strings.Contains(output.String(), "Rejected CSV line 14:") {
		t.Fatalf("missing rejection warning: %s", &output)
	}
	if !strings.Contains(output.String(), "completed: published=1 skipped=0 rejected=1") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientLimitsMissingDateWarnings(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader,
		strings.Repeat("IIIFL.FR,I,57.117900,07:00:01.014,\n", 8)+validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "Rejected CSV line") != 5 {
		t.Fatalf("expected five warning examples: %s", &output)
	}
	if !strings.Contains(output.String(), "published=1 skipped=0 rejected=8") {
		t.Fatalf("summary must count all rejected rows: %s", &output)
	}
}

func TestRunWithClientPublishesIndexUsingSystemDate(t *testing.T) {
	// Relevant fields copied from real index row 71436 and equity row 728610.
	path := writeCSVFile(t, csvContents(testCSVHeader+",Date",
		"IIIFL.FR,I,57.117900,07:00:01.014,,12-11-2021\n"+
			"RUI.FR,E,28.040000,09:00:00.179,12-11-2021,12-11-2021\n"))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events"}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 2 {
		t.Fatalf("published %d records; want 2", len(client.records))
	}
	var event marketv1.PriceEvent
	if err := proto.Unmarshal(client.records[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	if event.Symbol != "IIIFL.FR" || event.SecurityType != marketv1.SecurityType_SECURITY_TYPE_INDEX {
		t.Fatalf("unexpected index event: %v", &event)
	}
	if got := event.TradeTime.AsTime().Format("2006-01-02T15:04:05.000Z"); got != "2021-11-12T05:00:01.014Z" {
		t.Fatalf("unexpected fallback timestamp: %s", got)
	}
	want := "CSV processing completed: published=2 skipped=0 rejected=0 topic=\"trade-events\"\n"
	if output.String() != want {
		t.Fatalf("summary = %q; want %q", output.String(), want)
	}
}

func TestParseConfigRate(t *testing.T) {
	cfg, err := parseConfig([]string{"-rate=1000"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.rate != 1000 {
		t.Fatalf("rate=%d; want 1000", cfg.rate)
	}
}

func TestRunWithClientPacesPublishedEvents(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader,
		validRunRow+"A0HN5C.ETR,E,,,\n"+validRunRow+validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events", rate: 50}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.produceTimes) != 3 {
		t.Fatalf("got %d sends; want 3", len(client.produceTimes))
	}
	// The fake captures times a few instructions after each scheduled send starts.
	// Allow 1 ms of measurement slack; the expected spacing is 20 ms.
	for i := 1; i < len(client.produceTimes); i++ {
		if gap := client.produceTimes[i].Sub(client.produceTimes[i-1]); gap < 19*time.Millisecond {
			t.Fatalf("send gap=%v; want approximately 20 ms or longer", gap)
		}
	}
	if !strings.Contains(output.String(), "published=3 skipped=1 rejected=0") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientCancellationDuringRateWait(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+validRunRow))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan struct{})
	client := &fakeRunClient{afterProduce: func() {
		// Cancel while the second event is waiting for its one-second rate slot.
		go func() {
			timer := time.NewTimer(20 * time.Millisecond)
			defer timer.Stop()
			<-timer.C
			cancel()
			close(canceled)
		}()
	}}
	var output bytes.Buffer
	started := time.Now()
	err := runWithClient(ctx, config{csvPath: path, topic: "trade-events", rate: 1}, client, &output)
	<-canceled
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v; want context.Canceled", err)
	}
	if len(client.records) != 1 || client.closeCalls != 1 {
		t.Fatal("canceled wait should close client without sending second event")
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("cancellation waited for rate slot: %v", elapsed)
	}
	if !strings.Contains(output.String(), "stopped: published=1 skipped=0") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestWaitForReplayAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForReplay(ctx, time.Time{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v; want context.Canceled", err)
	}
}

func TestParseConfigMaxEvents(t *testing.T) {
	cfg, err := parseConfig([]string{"-max-events=100", "-rate=50"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxEvents != 100 || cfg.rate != 50 {
		t.Fatalf("unexpected configuration: %+v", cfg)
	}
}

func TestRunWithClientStopsAtMaxEvents(t *testing.T) {
	// Non-price and rejected rows do not consume the event limit. The invalid
	// row after the first valid event must never be processed.
	path := writeCSVFile(t, csvContents(testCSVHeader,
		"A0HN5C.ETR,E,,,\n"+
			"IIIFL.FR,I,57.117900,07:00:01.014,\n"+
			validRunRow+
			"RUI.FR,E,invalid,09:00:00.179,12-11-2021\n"))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events", maxEvents: 1}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 || client.closeCalls != 1 {
		t.Fatal("expected one published event and client cleanup")
	}
	if !strings.Contains(output.String(), "limit reached: published=1 skipped=1 rejected=1") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestRunWithClientMaxEventsAboveFileSize(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	if err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events", maxEvents: 10}, client, &output); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 || !strings.Contains(output.String(), "completed: published=1") {
		t.Fatalf("unexpected result: %s", &output)
	}
}

func TestRunWithClientRateAndMaxEvents(t *testing.T) {
	path := writeCSVFile(t, csvContents(testCSVHeader, validRunRow+validRunRow+validRunRow))
	client := &fakeRunClient{}
	var output bytes.Buffer
	err := runWithClient(context.Background(), config{csvPath: path, topic: "trade-events", rate: 50, maxEvents: 2}, client, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 2 {
		t.Fatalf("got %d sends; want 2", len(client.records))
	}
	if gap := client.produceTimes[1].Sub(client.produceTimes[0]); gap < 19*time.Millisecond {
		t.Fatalf("rate limit not applied: gap=%v", gap)
	}
	if !strings.Contains(output.String(), "limit reached: published=2") {
		t.Fatalf("unexpected summary: %s", &output)
	}
}

func TestParseConfigRejectsUnexpectedArguments(t *testing.T) {
	tests := [][]string{
		{"input.csv"},
		{"input.csv", "-rate=10", "-max-events=100"},
		{"-rate=10", "extra", "-max-events=100"},
		{"--", "-max-events=100"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_, err := parseConfig(args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
				t.Fatalf("args %v: error = %v; want unexpected-arguments error", args, err)
			}
		})
	}
}
