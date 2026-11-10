package main

import (
	"context"
	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// publishTimeout bounds how long one record may wait for its acknowledgment.
const publishTimeout = 30 * time.Second

// maxBufferedRecords bounds the records waiting in the client for an
// acknowledgment. Once it is reached, Produce blocks until records finish, so
// a slow broker slows the CSV replay down instead of growing memory.
const maxBufferedRecords = 10_000

func newRedpandaClient(brokers []string, linger time.Duration) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordDeliveryTimeout(publishTimeout),
		// Permit deadlines and Ctrl+C to cancel even records already in flight.
		// An unacknowledged record may nevertheless have been stored by the broker.
		kgo.AllowIdempotentProduceCancellation(),
		// The client batches records per partition. Idempotent writes stay on, which
		// keeps each partition in order with up to 5 requests in flight per broker.
		kgo.MaxBufferedRecords(maxBufferedRecords),
		kgo.ProducerLinger(linger),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Redpanda client: %w", err)
	}
	return client, nil
}

// priceEventProducer lets publishing be tested without connecting to a broker.
// *kgo.Client implements this interface.
type priceEventProducer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

func priceEventRecord(topic string, event *marketv1.PriceEvent) (*kgo.Record, error) {
	data, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("invalid event: %w", err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(event.GetSymbol()), Value: data}, nil
}

func rejectedRowRecord(topic string, rejectedEvent *marketv1.RejectedRow) (*kgo.Record, error) {
	data, err := proto.Marshal(rejectedEvent)
	if err != nil {
		return nil, fmt.Errorf("invalid rejected event: %w", err)
	}
	key := []byte(strconv.FormatUint(rejectedEvent.GetSourceRow(), 10))
	return &kgo.Record{Topic: topic, Key: key, Value: data}, nil
}

// publisher hands records to the client without waiting for each one and
// counts them only once Redpanda has acknowledged them. It keeps the first
// failure, so the caller can stop sending and report it.
type publisher struct {
	client  priceEventProducer
	pending sync.WaitGroup

	mu        sync.Mutex
	published uint64
	rejected  uint64
	err       error
}

// publish sends a trade event, or a rejected row when rejected is true.
// Cancelling ctx cancels the record while it is unacknowledged.
func (p *publisher) publish(ctx context.Context, record *kgo.Record, rejected bool) {
	p.pending.Add(1)
	p.client.Produce(ctx, record, func(record *kgo.Record, err error) {
		defer p.pending.Done()
		p.mu.Lock()
		defer p.mu.Unlock()
		switch {
		case err != nil:
			if p.err == nil {
				p.err = fmt.Errorf("publishing to topic %q: %w", record.Topic, err)
			}
		case rejected:
			p.rejected++
		default:
			p.published++
		}
	})
}

// failed returns the first failure acknowledged so far.
func (p *publisher) failed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// wait blocks until every record is acknowledged or failed, then returns the
// acknowledged counts and the first failure.
func (p *publisher) wait() (published, rejected uint64, err error) {
	p.pending.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.published, p.rejected, p.err
}
