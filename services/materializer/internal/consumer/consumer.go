package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"financial-streaming-platform/services/materializer/internal/data"
	"financial-streaming-platform/services/materializer/internal/mapper"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	EmaWindowsTopic      = "ema-windows"
	TradeAdvisoriesTopic = "trade-advisories"
	batchSize            = 100
	commitTimeout        = 10 * time.Second
)

// Store atomically saves an ordered batch before its offsets can be committed.
type Store interface {
	SaveBatch(context.Context, []data.Result) error
}

var _ Store = (*data.Postgres)(nil)

type redpandaClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
}

type Consumer struct {
	client redpandaClient
	store  Store
	logger *slog.Logger
}

// New subscribes to both result topics. A nil logger uses slog.Default().
func New(brokers []string, groupID string, store Store, logger *slog.Logger) (*Consumer, error) {
	if groupID == "" || groupID != strings.TrimSpace(groupID) {
		return nil, errors.New("consumer group must be nonempty and trimmed")
	}
	if store == nil {
		return nil, errors.New("materializer store is nil")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(EmaWindowsTopic, TradeAdvisoriesTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Redpanda consumer: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{client: client, store: store, logger: logger}, nil
}

// Run saves batches, then commits their offsets after each database commit.
// It closes the client on return; create a new consumer to retry failed records
// from the group's committed offsets. The caller owns and closes the store.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches := c.client.PollRecords(ctx, batchSize)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fetchErr := range fetches.Errors() {
			c.logger.Error("Redpanda fetch failed", "topic", fetchErr.Topic,
				"partition", fetchErr.Partition, "error", fetchErr.Err)
			return fmt.Errorf("fetch topic=%q partition=%d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}
		records := fetches.Records()
		if len(records) > 0 {
			results := make([]data.Result, 0, len(records))
			for _, record := range records {
				if err := ctx.Err(); err != nil {
					return err
				}
				result, err := decodeRecord(record)
				if err != nil {
					return c.recordError(record, err)
				}
				results = append(results, result)
			}
			if err := c.store.SaveBatch(ctx, results); err != nil {
				var batchErr *data.BatchError
				if errors.As(err, &batchErr) && batchErr.Index >= 0 && batchErr.Index < len(records) {
					return c.recordError(records[batchErr.Index], err)
				}
				return c.batchError("save database batch", records, err)
			}
			if err := c.commitBatch(ctx, records); err != nil {
				return c.batchError("commit batch offsets", records, err)
			}
		}
		c.client.AllowRebalance()
	}
}

// Close releases a pending rebalance and closes the Redpanda client.
func (c *Consumer) Close() {
	c.client.CloseAllowingRebalance()
}

func decodeRecord(record *kgo.Record) (data.Result, error) {
	switch record.Topic {
	case EmaWindowsTopic:
		window, err := mapper.DecodeEmaWindow(record.Value)
		if err != nil {
			return nil, err
		}
		return window, nil
	case TradeAdvisoriesTopic:
		advisory, err := mapper.DecodeTradeAdvisory(record.Value)
		if err != nil {
			return nil, err
		}
		return advisory, nil
	default:
		return nil, fmt.Errorf("unsupported topic %q", record.Topic)
	}
}

func (c *Consumer) recordError(record *kgo.Record, err error) error {
	c.logger.Error("materializer record failed", "topic", record.Topic,
		"partition", record.Partition, "offset", record.Offset, "error", err)
	return fmt.Errorf("process topic=%q partition=%d offset=%d: %w",
		record.Topic, record.Partition, record.Offset, err)
}

func (c *Consumer) batchError(operation string, records []*kgo.Record, err error) error {
	last := records[len(records)-1]
	c.logger.Error("materializer batch failed", "operation", operation, "batch_size", len(records),
		"last_topic", last.Topic, "last_partition", last.Partition, "last_offset", last.Offset, "error", err)
	return fmt.Errorf("%s of %d records ending at topic=%q partition=%d offset=%d: %w",
		operation, len(records), last.Topic, last.Partition, last.Offset, err)
}

func (c *Consumer) commitBatch(ctx context.Context, records []*kgo.Record) error {
	// The database has committed. Finish acknowledging this batch even if the
	// caller is shutting down, while bounding how long shutdown can wait.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	if err := c.client.CommitRecords(commitCtx, records...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}
