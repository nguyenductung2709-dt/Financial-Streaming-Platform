// Package stamps publishes measurement-only stage stamps: one market.v1.StageStamp
// per result a batch commits to PostgreSQL, read by scripts/collect-latency.py.
package stamps

import (
	"context"
	"fmt"
	"time"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Publisher publishes one stamp per result of a committed batch. On error the
// caller must not commit the batch's offsets; the retry stamps again and the
// collector keeps the first stamp.
type Publisher interface {
	Publish(context.Context, []data.Result) error
}

// Closer is a Publisher the caller closes once the consumer has stopped.
type Closer interface {
	Publisher
	Close()
}

const publishTimeout = 10 * time.Second

type noopPublisher struct{}

// Noop is the publisher used when stamping is off.
var Noop Closer = noopPublisher{}

func (noopPublisher) Publish(context.Context, []data.Result) error { return nil }
func (noopPublisher) Close()                                       {}

// producerClient lets tests avoid a broker; *kgo.Client implements it.
type producerClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type kafkaPublisher struct {
	client producerClient
	topic  string
	clock  func() time.Time
}

// NewKafka returns a publisher writing to topic through its own producer client.
func NewKafka(brokers []string, topic string) (Closer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordDeliveryTimeout(publishTimeout),
		// Let shutdown cancel records still in flight; the retried batch republishes them.
		kgo.AllowIdempotentProduceCancellation(),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Redpanda stamp producer: %w", err)
	}
	return &kafkaPublisher{client: client, topic: topic, clock: time.Now}, nil
}

// Publish stamps a committed batch. It outlives ctx cancellation, up to publishTimeout.
func (p *kafkaPublisher) Publish(ctx context.Context, results []data.Result) error {
	if len(results) == 0 {
		return nil
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	records, err := recordsFor(results, p.topic, p.clock())
	if err != nil {
		return err
	}
	if err := p.client.ProduceSync(publishCtx, records...).FirstErr(); err != nil {
		return fmt.Errorf("publish %d materialize stamps to topic %q: %w", len(records), p.topic, err)
	}
	return nil
}

func (p *kafkaPublisher) Close() { p.client.Close() }

// recordsFor builds one stamp record per result; the result's type picks the stage.
func recordsFor(results []data.Result, topic string, at time.Time) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, len(results))
	for _, result := range results {
		var window *data.EmaWindow
		stage := marketv1.Stage_STAGE_EMA_MATERIALIZED
		switch result := result.(type) {
		case *data.EmaWindow:
			window = result
		case *data.TradeAdvisory:
			window = result.EmaWindow
			stage = marketv1.Stage_STAGE_ADVISORY_MATERIALIZED
		default:
			return nil, fmt.Errorf("unsupported result type %T", result)
		}
		stamp := &marketv1.StageStamp{
			Symbol:    window.Symbol,
			WindowEnd: timestamppb.New(window.WindowEnd),
			Stage:     stage,
			At:        timestamppb.New(at),
		}
		value, err := proto.Marshal(stamp)
		if err != nil {
			return nil, fmt.Errorf("marshal materialize stamp for %s: %w", window.Symbol, err)
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(window.Symbol),
			Value: value,
		})
	}
	return records, nil
}
