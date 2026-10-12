package main

import (
	"context"
	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const publishTimeout = 30 * time.Second

func newRedpandaClient(brokers []string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordDeliveryTimeout(publishTimeout),
		// Permit deadlines and Ctrl+C to cancel even records already in flight.
		// An unacknowledged record may nevertheless have been stored by the broker.
		kgo.AllowIdempotentProduceCancellation(),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Redpanda client: %w", err)
	}
	return client, nil
}

// priceEventProducer lets publishing be tested without connecting to a broker.
// *kgo.Client implements this interface.
type priceEventProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

func publishPriceEvent(ctx context.Context, client priceEventProducer, topic string, event *marketv1.PriceEvent) error {
	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}
	// Format the record
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(event.GetSymbol()),
		Value: data,
	}
	// Bound each send while preserving an earlier caller deadline or cancellation.
	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := client.ProduceSync(publishCtx, record).FirstErr(); err != nil {
		return fmt.Errorf("publishing event to topic %q: %w", topic, err)
	}
	return nil
}

func publishPriceEventDLQ(ctx context.Context, client priceEventProducer, topic string, rejectedEvent *marketv1.RejectedRow) error {
	data, err := proto.Marshal(rejectedEvent)
	if err != nil {
		return fmt.Errorf("invalid rejected event: %w", err)
	}
	// Format the record
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.FormatUint(rejectedEvent.GetSourceRow(), 10)),
		Value: data,
	}
	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := client.ProduceSync(publishCtx, record).FirstErr(); err != nil {
		return fmt.Errorf("publishing rejected event to topic %q: %w", topic, err)
	}
	return nil
}
