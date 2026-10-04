package main

import (
	"context"
	marketv1 "financial-streaming-platform/gen/market/v1"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func newRedpandaClient(brokers []string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
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
	// Publish the record into the topic
	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publishing event to topic %q: %w", topic, err)
	}
	return nil
}
