package stamps

import (
	"context"
	"errors"
	"testing"
	"time"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type fakeProducer struct {
	produceErr error
	produced   [][]*kgo.Record
	produce    func(context.Context) error
	closed     bool
}

func (f *fakeProducer) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.produced = append(f.produced, records)
	results := make(kgo.ProduceResults, len(records))
	err := f.produceErr
	if f.produce != nil {
		err = f.produce(ctx)
	}
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: err}
	}
	return results
}

func (f *fakeProducer) Close() { f.closed = true }

var (
	windowStart = time.Date(2021, 11, 12, 6, 0, 0, 0, time.UTC)
	windowEnd   = windowStart.Add(5 * time.Minute)
	stampAt     = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
)

func emaWindow(symbol string) *data.EmaWindow {
	return &data.EmaWindow{
		Symbol: symbol, WindowStart: windowStart, WindowEnd: windowEnd,
		ClosingPriceMicros: 418210000, Ema38: "1.5", Ema100: "1.2",
	}
}

func advisory(symbol string) *data.TradeAdvisory {
	return &data.TradeAdvisory{
		EmaWindow: emaWindow(symbol), Action: data.AdvisoryActionBuy,
		PreviousEma38: "1.4", PreviousEma100: "1.2",
	}
}

func publisher(client producerClient, at time.Time) *kafkaPublisher {
	return &kafkaPublisher{client: client, topic: "materialize-stamps", clock: func() time.Time { return at }}
}

func stampOf(t *testing.T, record *kgo.Record) *marketv1.StageStamp {
	t.Helper()
	var stamp marketv1.StageStamp
	if err := proto.Unmarshal(record.Value, &stamp); err != nil {
		t.Fatalf("decode stamp: %v", err)
	}
	return &stamp
}

func TestPublishStampsEveryResultWithTheCommitTime(t *testing.T) {
	client := &fakeProducer{}
	p := publisher(client, stampAt)
	results := []data.Result{emaWindow("RUI.FR"), advisory("IUSAC.FR"), emaWindow("RUI.FR")}
	if err := p.Publish(context.Background(), results); err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(client.produced) != 1 || len(client.produced[0]) != 3 {
		t.Fatalf("produced %d batches; want one with 3 records", len(client.produced))
	}
	wantStages := []marketv1.Stage{
		marketv1.Stage_STAGE_EMA_MATERIALIZED,
		marketv1.Stage_STAGE_ADVISORY_MATERIALIZED,
		marketv1.Stage_STAGE_EMA_MATERIALIZED,
	}
	for i, record := range client.produced[0] {
		if record.Topic != "materialize-stamps" {
			t.Fatalf("record %d topic = %q", i, record.Topic)
		}
		stamp := stampOf(t, record)
		if stamp.Stage != wantStages[i] {
			t.Fatalf("record %d stage = %v; want %v", i, stamp.Stage, wantStages[i])
		}
		if !stamp.At.AsTime().Equal(stampAt) {
			t.Fatalf("record %d at = %v; want the one commit time %v", i, stamp.At.AsTime(), stampAt)
		}
		if !stamp.WindowEnd.AsTime().Equal(windowEnd) {
			t.Fatalf("record %d window_end = %v; want %v", i, stamp.WindowEnd.AsTime(), windowEnd)
		}
		wantSymbol := "RUI.FR"
		if stamp.Stage == marketv1.Stage_STAGE_ADVISORY_MATERIALIZED {
			wantSymbol = "IUSAC.FR"
		}
		if stamp.Symbol != wantSymbol || string(record.Key) != wantSymbol {
			t.Fatalf("record %d symbol = %q, key = %q; want %q", i, stamp.Symbol, record.Key, wantSymbol)
		}
	}
}

func TestPublishWithNoResultsProducesNothing(t *testing.T) {
	client := &fakeProducer{}
	if err := publisher(client, stampAt).Publish(context.Background(), nil); err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(client.produced) != 0 {
		t.Fatalf("published %d batches; want none", len(client.produced))
	}
}

func TestPublishFinishesDespiteCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeProducer{produce: func(produceCtx context.Context) error {
		if err := produceCtx.Err(); err != nil {
			t.Fatalf("publish inherited caller cancellation: %v", err)
		}
		deadline, ok := produceCtx.Deadline()
		if remaining := time.Until(deadline); !ok || remaining <= 0 || remaining > publishTimeout {
			t.Fatalf("publish deadline = %v; want a timeout within %v", deadline, publishTimeout)
		}
		return nil
	}}
	if err := publisher(client, stampAt).Publish(ctx, []data.Result{emaWindow("RUI.FR")}); err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(client.produced) != 1 {
		t.Fatal("canceled run skipped the stamps of a committed batch")
	}
}

func TestPublishFailureIsAnError(t *testing.T) {
	produceErr := errors.New("broker unreachable")
	client := &fakeProducer{produceErr: produceErr}
	err := publisher(client, stampAt).Publish(context.Background(), []data.Result{emaWindow("RUI.FR")})
	if !errors.Is(err, produceErr) {
		t.Fatalf("Publish error = %v; want produce error", err)
	}
}
