package main

import (
	"context"
	"errors"
	marketv1 "financial-streaming-platform/gen/market/v1"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This fake captures the record and supplies the broker result.
// It never connects to Redpanda.
type fakePriceEventProducer struct {
	ctx     context.Context
	records []*kgo.Record
	err     error
	calls   int
}

func (f *fakePriceEventProducer) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.calls++
	f.ctx = ctx
	f.records = records
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: f.err}
	}
	return results
}

func TestNewRedpandaClient(t *testing.T) {
	brokers := []string{"localhost:19092", "localhost:29092"}
	client, err := newRedpandaClient(brokers)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("expected a client")
	}
	t.Cleanup(client.Close)
	if got := client.OptValue(kgo.SeedBrokers); !reflect.DeepEqual(got, brokers) {
		t.Fatalf("seed brokers = %v; want %v", got, brokers)
	}
}

func TestNewRedpandaClientRejectsEmptyBrokers(t *testing.T) {
	client, err := newRedpandaClient(nil)
	if client != nil {
		client.Close()
		t.Fatal("expected nil client")
	}
	if err == nil || !strings.Contains(err.Error(), "initialize Redpanda client") {
		t.Fatalf("expected initialization error; got %v", err)
	}
}

func TestPublishPriceEvent(t *testing.T) {
	producer := &fakePriceEventProducer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event := &marketv1.PriceEvent{
		Symbol:          "RUI.FR",
		SecurityType:    marketv1.SecurityType_SECURITY_TYPE_EQUITY,
		LastPriceMicros: 28_040_000,
		TradeTime:       timestamppb.New(time.Date(2021, 11, 12, 7, 0, 0, 179_000_000, time.UTC)),
		SourceRow:       728610,
	}
	if err := publishPriceEvent(ctx, producer, "trade-events", event); err != nil {
		t.Fatal(err)
	}
	if producer.calls != 1 || len(producer.records) != 1 {
		t.Fatalf("got %d calls and %d records; want one of each", producer.calls, len(producer.records))
	}
	if producer.ctx != ctx {
		t.Fatal("caller context was not forwarded")
	}
	record := producer.records[0]
	if record.Topic != "trade-events" {
		t.Fatalf("topic = %q; want trade-events", record.Topic)
	}
	if string(record.Key) != event.Symbol {
		t.Fatalf("key = %q; want %q", record.Key, event.Symbol)
	}
	var decoded marketv1.PriceEvent
	if err := proto.Unmarshal(record.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&decoded, event) {
		t.Fatalf("decoded event = %v; want %v", &decoded, event)
	}
}

func TestPublishPriceEventReturnsSerializationError(t *testing.T) {
	producer := &fakePriceEventProducer{}
	// Protobuf string fields must contain valid UTF-8.
	event := &marketv1.PriceEvent{Symbol: string([]byte{0xff})}
	err := publishPriceEvent(context.Background(), producer, "trade-events", event)
	if err == nil {
		t.Fatal("expected a serialization error")
	}
	if producer.calls != 0 {
		t.Fatal("serialization failure must not publish a record")
	}
}

func TestPublishPriceEventReturnsProduceError(t *testing.T) {
	for _, produceErr := range []error{errors.New("broker rejected record"), context.Canceled, context.DeadlineExceeded} {
		t.Run(produceErr.Error(), func(t *testing.T) {
			producer := &fakePriceEventProducer{err: produceErr}
			err := publishPriceEvent(context.Background(), producer, "trade-events", &marketv1.PriceEvent{Symbol: "RUI.FR"})
			if !errors.Is(err, produceErr) {
				t.Fatalf("error = %v; want wrapped %v", err, produceErr)
			}
			if !strings.Contains(err.Error(), "trade-events") {
				t.Fatalf("error omits topic: %v", err)
			}
			if producer.calls != 1 {
				t.Fatalf("got %d produce calls; want 1", producer.calls)
			}
		})
	}
}
