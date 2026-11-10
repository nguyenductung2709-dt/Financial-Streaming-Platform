package main

import (
	"context"
	"errors"
	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This fake captures the records and answers each one through its promise,
// at once or, with ackDelay, later from another goroutine like the real client.
// It never connects to Redpanda.
type fakeProducer struct {
	mu       sync.Mutex
	ctxs     []context.Context
	records  []*kgo.Record
	err      error
	ackDelay time.Duration
}

func (f *fakeProducer) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	f.ctxs = append(f.ctxs, ctx)
	f.records = append(f.records, record)
	f.mu.Unlock()
	if f.ackDelay == 0 {
		promise(record, f.err)
		return
	}
	go func() {
		time.Sleep(f.ackDelay)
		promise(record, f.err)
	}()
}

func TestNewRedpandaClient(t *testing.T) {
	brokers := []string{"localhost:19092", "localhost:29092"}
	client, err := newRedpandaClient(brokers, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("expected a client")
	}
	t.Cleanup(client.Close)
	if got := client.OptValue(kgo.RecordDeliveryTimeout); got != publishTimeout {
		t.Fatalf("delivery timeout = %v; want %v", got, publishTimeout)
	}
	if got := client.OptValue(kgo.AllowIdempotentProduceCancellation); got != true {
		t.Fatalf("in-flight cancellation = %v; want true", got)
	}
	if got := client.OptValue(kgo.DisableIdempotentWrite); got != false {
		t.Fatalf("idempotent writes disabled = %v; want false", got)
	}
	if got := client.OptValue(kgo.MaxBufferedRecords); got != int64(maxBufferedRecords) {
		t.Fatalf("max buffered records = %v; want %d", got, maxBufferedRecords)
	}
	if got := client.OptValue(kgo.ProducerLinger); got != 5*time.Millisecond {
		t.Fatalf("linger = %v; want 5ms", got)
	}
	if got := client.OptValue(kgo.SeedBrokers); !reflect.DeepEqual(got, brokers) {
		t.Fatalf("seed brokers = %v; want %v", got, brokers)
	}
}

func TestNewRedpandaClientRejectsEmptyBrokers(t *testing.T) {
	client, err := newRedpandaClient(nil, 0)
	if client != nil {
		client.Close()
		t.Fatal("expected nil client")
	}
	if err == nil || !strings.Contains(err.Error(), "initialize Redpanda client") {
		t.Fatalf("expected initialization error; got %v", err)
	}
}

func TestPriceEventRecord(t *testing.T) {
	event := &marketv1.PriceEvent{
		Symbol:          "RUI.FR",
		SecurityType:    marketv1.SecurityType_SECURITY_TYPE_EQUITY,
		LastPriceMicros: 28_040_000,
		TradeTime:       timestamppb.New(time.Date(2021, 11, 12, 7, 0, 0, 179_000_000, time.UTC)),
		SourceRow:       728610,
	}
	record, err := priceEventRecord("trade-events", event)
	if err != nil {
		t.Fatal(err)
	}
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

func TestPriceEventRecordReturnsSerializationError(t *testing.T) {
	// Protobuf string fields must contain valid UTF-8.
	if _, err := priceEventRecord("trade-events", &marketv1.PriceEvent{Symbol: string([]byte{0xff})}); err == nil {
		t.Fatal("expected a serialization error")
	}
}

func TestRejectedRowRecord(t *testing.T) {
	event := &marketv1.RejectedRow{SourceRow: ^uint64(0), Values: []string{"IIIFL.FR", "invalid"},
		ColumnIndices: map[string]int32{"ID": 0, "Last": 1}, ErrorReason: "invalid price"}
	record, err := rejectedRowRecord("trade-events-dlq", event)
	if err != nil {
		t.Fatal(err)
	}
	if record.Topic != "trade-events-dlq" || string(record.Key) != "18446744073709551615" {
		t.Fatalf("unexpected topic or decimal row key: %+v", record)
	}
	var decoded marketv1.RejectedRow
	if err := proto.Unmarshal(record.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&decoded, event) {
		t.Fatalf("decoded %v; want %v", &decoded, event)
	}
}

func TestRejectedRowRecordReturnsSerializationError(t *testing.T) {
	invalid := string([]byte{0xff})
	for _, event := range []*marketv1.RejectedRow{
		{Values: []string{invalid}}, {ColumnIndices: map[string]int32{invalid: 0}}, {ErrorReason: invalid},
	} {
		if _, err := rejectedRowRecord("trade-events-dlq", event); err == nil {
			t.Fatalf("expected a serialization error for %v", event)
		}
	}
}

// callerKey marks the caller's context, so the test can tell it reached the client.
type callerKey struct{}

func TestPublisherCountsAcknowledgedRecords(t *testing.T) {
	producer := &fakeProducer{ackDelay: 10 * time.Millisecond}
	p := &publisher{client: producer}
	ctx := context.WithValue(context.Background(), callerKey{}, "caller")
	p.publish(ctx, &kgo.Record{Topic: "trade-events"}, false)
	p.publish(ctx, &kgo.Record{Topic: "trade-events"}, false)
	p.publish(ctx, &kgo.Record{Topic: "trade-events-dlq"}, true)
	published, rejected, err := p.wait()
	if err != nil {
		t.Fatal(err)
	}
	if published != 2 || rejected != 1 {
		t.Fatalf("published=%d rejected=%d; want 2 and 1", published, rejected)
	}
	for _, got := range producer.ctxs {
		if got != ctx {
			t.Fatal("the caller's context must reach the client, so cancelling it cancels the records")
		}
	}
}

func TestPublisherKeepsFirstFailure(t *testing.T) {
	for _, failure := range []error{errors.New("broker rejected record"), context.Canceled, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			p := &publisher{client: &fakeProducer{err: failure, ackDelay: time.Millisecond}}
			p.publish(context.Background(), &kgo.Record{Topic: "trade-events"}, false)
			p.publish(context.Background(), &kgo.Record{Topic: "trade-events-dlq"}, true)
			published, rejected, err := p.wait()
			if !errors.Is(err, failure) {
				t.Fatalf("error = %v; want wrapped %v", err, failure)
			}
			if !strings.Contains(err.Error(), "trade-events") {
				t.Fatalf("error omits topic: %v", err)
			}
			if published != 0 || rejected != 0 {
				t.Fatalf("failed records counted: published=%d rejected=%d", published, rejected)
			}
			if !errors.Is(p.failed(), failure) {
				t.Fatalf("failed() = %v; want %v", p.failed(), failure)
			}
		})
	}
}

func TestPublisherFailedIsNilUntilAFailureArrives(t *testing.T) {
	p := &publisher{client: &fakeProducer{}}
	p.publish(context.Background(), &kgo.Record{Topic: "trade-events"}, false)
	if err := p.failed(); err != nil {
		t.Fatalf("failed() = %v; want nil", err)
	}
}
