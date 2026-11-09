package consumer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"financial-streaming-platform/services/materializer/internal/data"
	"financial-streaming-platform/services/materializer/internal/stamps"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errPollingStopped = errors.New("test polling stopped")

type fakeClient struct {
	fetches       []kgo.Fetches
	poll          func(context.Context) kgo.Fetches
	polls         int
	pollLimits    []int
	commits       []*kgo.Record
	commitBatches [][]*kgo.Record
	commitErr     error
	commit        func(context.Context) error
	rebalances    int
	closed        bool
	events        *[]string
}

func (f *fakeClient) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	f.polls++
	f.pollLimits = append(f.pollLimits, limit)
	if f.poll != nil {
		return f.poll(ctx)
	}
	if len(f.fetches) == 0 {
		return kgo.NewErrFetch(errPollingStopped)
	}
	result := f.fetches[0]
	f.fetches = f.fetches[1:]
	return result
}

func (f *fakeClient) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	if f.events != nil {
		*f.events = append(*f.events, "offset commit")
	}
	f.commits = append(f.commits, records...)
	f.commitBatches = append(f.commitBatches, slices.Clone(records))
	if f.commit != nil {
		return f.commit(ctx)
	}
	return f.commitErr
}

func (f *fakeClient) AllowRebalance() { f.rebalances++ }

func (f *fakeClient) CloseAllowingRebalance() {
	f.AllowRebalance()
	f.closed = true
}

type fakeStore struct {
	windows      []*data.EmaWindow
	advisories   []*data.TradeAdvisory
	batches      [][]data.Result
	saveErr      error
	failOnResult int
	saveCalls    int
	resultCalls  int
	afterSave    func()
	events       *[]string
}

func (f *fakeStore) SaveBatch(ctx context.Context, results []data.Result) error {
	f.saveCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.saveErr != nil && f.failOnResult == 0 {
		return f.saveErr
	}
	var windows []*data.EmaWindow
	var advisories []*data.TradeAdvisory
	for i, result := range results {
		f.resultCalls++
		if f.saveErr != nil && f.failOnResult == i+1 {
			return &data.BatchError{Index: i, Err: f.saveErr}
		}
		var event string
		switch result := result.(type) {
		case *data.EmaWindow:
			windows = append(windows, result)
			event = "save window"
		case *data.TradeAdvisory:
			advisories = append(advisories, result)
			event = "save advisory"
		default:
			return errors.New("unexpected result type")
		}
		if f.events != nil {
			*f.events = append(*f.events, event)
		}
	}
	f.windows = append(f.windows, windows...)
	f.advisories = append(f.advisories, advisories...)
	f.batches = append(f.batches, slices.Clone(results))
	if f.events != nil {
		*f.events = append(*f.events, "database commit")
	}
	if f.afterSave != nil {
		f.afterSave()
	}
	return nil
}

type fakeStamper struct {
	batches [][]data.Result
	calls   int
	err     error
	events  *[]string
}

func (f *fakeStamper) Publish(ctx context.Context, results []data.Result) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.batches = append(f.batches, slices.Clone(results))
	if f.events != nil {
		*f.events = append(*f.events, "materialize stamps")
	}
	return nil
}

func testConsumer(client *fakeClient, store *fakeStore, stamper *fakeStamper) *Consumer {
	return &Consumer{client: client, store: store, stamps: stamper, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func windowMessage() *marketv1.EmaWindow {
	start := time.Date(2021, 11, 12, 6, 0, 0, 0, time.UTC)
	return &marketv1.EmaWindow{
		Symbol: "RUI.FR", WindowStart: timestamppb.New(start),
		WindowEnd:          timestamppb.New(start.Add(5 * time.Minute)),
		ClosingPriceMicros: ^uint64(0), Ema38: "1.234567890123456789012345678901234", Ema100: "0.000001",
	}
}

func resultRecord(t *testing.T, topic string, offset int64) *kgo.Record {
	t.Helper()
	var message proto.Message = windowMessage()
	if topic == TradeAdvisoriesTopic {
		message = &marketv1.TradeAdvisory{
			EmaWindow: windowMessage(), Action: marketv1.AdvisoryAction_ADVISORY_ACTION_BUY,
			PreviousEma38: "0", PreviousEma100: "0.1234567890123456789012345678901234",
		}
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Partition: 2, Offset: offset, Value: payload}
}

func recordFetch(records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      records[0].Topic,
		Partitions: []kgo.FetchPartition{{Partition: records[0].Partition, Records: records}},
	}}}}
}

func TestNewConfiguresManualCommits(t *testing.T) {
	brokers := []string{"localhost:19092"}
	c, err := New(brokers, "materializer-test", &fakeStore{}, stamps.Noop, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	client := c.client.(*kgo.Client)
	for _, option := range []any{kgo.DisableAutoCommit, kgo.BlockRebalanceOnPoll} {
		if got := client.OptValue(option); got != true {
			t.Fatalf("required safety option %T = %v; want true", option, got)
		}
	}
	if got := client.OptValue(kgo.ConsumerGroup); got != "materializer-test" {
		t.Fatalf("consumer group = %v", got)
	}
	if got := client.OptValue(kgo.SeedBrokers); !reflect.DeepEqual(got, brokers) {
		t.Fatalf("brokers = %v; want %v", got, brokers)
	}
	topics := client.GetConsumeTopics()
	slices.Sort(topics)
	if !reflect.DeepEqual(topics, []string{EmaWindowsTopic, TradeAdvisoriesTopic}) {
		t.Fatalf("topics = %v", topics)
	}
	for _, option := range []any{kgo.ConsumeStartOffset, kgo.ConsumeResetOffset} {
		if got := client.OptValue(option); !reflect.DeepEqual(got, kgo.NewOffset().AtStart()) {
			t.Fatalf("starting/reset offset = %v; want earliest", got)
		}
	}
	if c.logger == nil {
		t.Fatal("default logger was not set")
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name    string
		brokers []string
		group   string
		store   Store
		stamper stamps.Publisher
	}{
		{"no brokers", nil, "materializer", &fakeStore{}, stamps.Noop},
		{"empty group", []string{"localhost:19092"}, "", &fakeStore{}, stamps.Noop},
		{"untrimmed group", []string{"localhost:19092"}, " materializer", &fakeStore{}, stamps.Noop},
		{"nil store", []string{"localhost:19092"}, "materializer", nil, stamps.Noop},
		{"nil stamp publisher", []string{"localhost:19092"}, "materializer", &fakeStore{}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, err := New(test.brokers, test.group, test.store, test.stamper, nil)
			if c != nil {
				c.Close()
				t.Fatal("expected nil consumer")
			}
			if err == nil {
				t.Fatal("expected invalid configuration to fail")
			}
		})
	}
}

func TestRunWritesWholeBatchBeforeCommitting(t *testing.T) {
	window := resultRecord(t, EmaWindowsTopic, 41)
	secondWindow := resultRecord(t, EmaWindowsTopic, 42)
	advisory := resultRecord(t, TradeAdvisoriesTopic, 9)
	otherPartition := resultRecord(t, TradeAdvisoriesTopic, 20)
	otherPartition.Partition = 3
	records := []*kgo.Record{window, advisory, secondWindow, otherPartition}
	batch := recordFetch(window)
	batch = append(batch, recordFetch(advisory)...)
	batch = append(batch, recordFetch(secondWindow)...)
	batch = append(batch, recordFetch(otherPartition)...)
	var events []string
	client := &fakeClient{fetches: []kgo.Fetches{batch}, events: &events}
	store := &fakeStore{events: &events}
	stamper := &fakeStamper{events: &events}
	c := testConsumer(client, store, stamper)
	if err := c.Run(context.Background()); !errors.Is(err, errPollingStopped) {
		t.Fatalf("Run error = %v", err)
	}
	if want := []string{"save window", "save advisory", "save window", "save advisory", "database commit", "materialize stamps", "offset commit"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("operation order = %v; want %v", events, want)
	}
	if stamper.calls != 1 || len(stamper.batches) != 1 || len(stamper.batches[0]) != len(records) {
		t.Fatal("the committed batch was not stamped exactly once with all its results")
	}
	if stamper.batches[0][0] != data.Result(store.windows[0]) || stamper.batches[0][1] != data.Result(store.advisories[0]) {
		t.Fatal("stamps were not published for the decoded results of the batch")
	}
	if !reflect.DeepEqual(client.commitBatches, [][]*kgo.Record{records}) {
		t.Fatal("batch commit did not include exactly the successfully saved records across topics and partitions")
	}
	if store.saveCalls != 1 || len(store.batches) != 1 || len(store.batches[0]) != len(records) {
		t.Fatal("records were not passed to the store as one complete ordered batch")
	}
	if len(store.windows) != 2 || store.windows[0].ClosingPriceMicros != ^uint64(0) ||
		len(store.advisories) != 2 || store.advisories[0].Action != data.AdvisoryActionBuy {
		t.Fatal("records were not decoded and routed to their writers")
	}
	for _, limit := range client.pollLimits {
		if limit != batchSize {
			t.Fatalf("poll limit = %d; want %d records", limit, batchSize)
		}
	}
	if client.rebalances != 2 || !client.closed {
		t.Fatal("rebalance gates were not released after processing and on exit")
	}
}

func TestRunDoesNotCommitFailedWrites(t *testing.T) {
	writeErr := errors.New("database write failed")
	for _, topic := range []string{EmaWindowsTopic, TradeAdvisoriesTopic} {
		t.Run(topic, func(t *testing.T) {
			failed := resultRecord(t, topic, 41)
			later := resultRecord(t, topic, 42)
			client := &fakeClient{fetches: []kgo.Fetches{recordFetch(failed), recordFetch(later)}}
			store := &fakeStore{saveErr: writeErr, failOnResult: 1}
			c := testConsumer(client, store, &fakeStamper{})
			var logs bytes.Buffer
			c.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			err := c.Run(context.Background())
			if !errors.Is(err, writeErr) {
				t.Fatalf("Run error = %v; want write error", err)
			}
			if len(client.commits) != 0 || client.polls != 1 || !client.closed {
				t.Fatal("failed write was committed, consumption continued, or client stayed open")
			}
			for _, field := range []string{`"topic":"` + topic + `"`, `"partition":2`, `"offset":41`} {
				if !strings.Contains(logs.String(), field) {
					t.Fatalf("failure log is missing %s: %s", field, logs.String())
				}
			}
			if !strings.Contains(err.Error(), topic) || !strings.Contains(err.Error(), "partition=2 offset=41") {
				t.Fatalf("error lacks record location: %v", err)
			}
		})
	}
}

func TestRunStopsOnInvalidRecords(t *testing.T) {
	for _, test := range []struct {
		name  string
		topic string
		value []byte
	}{
		{"window tombstone", EmaWindowsTopic, nil},
		{"bad window protobuf", EmaWindowsTopic, []byte{0x00}},
		{"bad advisory protobuf", TradeAdvisoriesTopic, []byte{0x00}},
		{"unknown topic", "unknown-results", []byte{0x01}},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := &kgo.Record{Topic: test.topic, Partition: 2, Offset: 41, Value: test.value}
			client := &fakeClient{fetches: []kgo.Fetches{recordFetch(record)}}
			store := &fakeStore{}
			if err := testConsumer(client, store, &fakeStamper{}).Run(context.Background()); err == nil {
				t.Fatal("expected invalid record to fail")
			}
			if store.saveCalls != 0 || len(store.windows)+len(store.advisories)+len(client.commits) != 0 || !client.closed {
				t.Fatal("invalid record was written or committed, or client stayed open")
			}
		})
	}
}

func TestRunDecodesWholeBatchBeforeWriting(t *testing.T) {
	for _, topic := range []string{EmaWindowsTopic, TradeAdvisoriesTopic} {
		t.Run(topic, func(t *testing.T) {
			first := resultRecord(t, topic, 41)
			invalid := resultRecord(t, topic, 42)
			invalid.Value = nil
			client := &fakeClient{fetches: []kgo.Fetches{recordFetch(first, invalid)}}
			store := &fakeStore{}
			if err := testConsumer(client, store, &fakeStamper{}).Run(context.Background()); err == nil {
				t.Fatal("expected invalid second record to fail")
			}
			if store.saveCalls != 0 || len(client.commitBatches) != 0 || !client.closed {
				t.Fatal("invalid batch started database writes or committed offsets")
			}
		})
	}
}

func TestRunDoesNotCommitOffsetsAfterDatabaseCommitFailure(t *testing.T) {
	commitErr := errors.New("database commit failed")
	first := resultRecord(t, EmaWindowsTopic, 41)
	second := resultRecord(t, EmaWindowsTopic, 42)
	client := &fakeClient{fetches: []kgo.Fetches{recordFetch(first, second)}}
	store := &fakeStore{saveErr: commitErr}
	c := testConsumer(client, store, &fakeStamper{})
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	err := c.Run(context.Background())
	if !errors.Is(err, commitErr) || !strings.Contains(err.Error(), "save database batch") {
		t.Fatalf("Run error = %v; want database commit failure", err)
	}
	if store.saveCalls != 1 || len(client.commitBatches) != 0 || client.polls != 1 || !client.closed {
		t.Fatal("consumer committed offsets or continued after a failed database commit")
	}
	for _, field := range []string{`"operation":"save database batch"`, `"batch_size":2`, `"last_offset":42`} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("database commit failure log is missing %s: %s", field, logs.String())
		}
	}
}

func TestRunStopsAfterCommitFailure(t *testing.T) {
	commitErr := errors.New("offset commit failed")
	first := resultRecord(t, EmaWindowsTopic, 41)
	second := resultRecord(t, EmaWindowsTopic, 42)
	later := resultRecord(t, EmaWindowsTopic, 43)
	client := &fakeClient{fetches: []kgo.Fetches{recordFetch(first, second), recordFetch(later)}, commitErr: commitErr}
	store := &fakeStore{}
	c := testConsumer(client, store, &fakeStamper{})
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	err := c.Run(context.Background())
	if !errors.Is(err, commitErr) || !strings.Contains(err.Error(), "partition=2 offset=42") {
		t.Fatalf("Run error = %v; want commit error and record location", err)
	}
	if len(store.windows) != 2 || len(client.commitBatches) != 1 ||
		!reflect.DeepEqual(client.commitBatches[0], []*kgo.Record{first, second}) ||
		client.polls != 1 || !client.closed {
		t.Fatal("consumer continued past an uncertain commit")
	}
	for _, field := range []string{`"batch_size":2`, `"last_topic":"ema-windows"`, `"last_partition":2`, `"last_offset":42`} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("batch failure log is missing %s: %s", field, logs.String())
		}
	}
}

func TestRunDoesNotCommitOffsetsWhenStampingFails(t *testing.T) {
	stampErr := errors.New("stamp publish failed")
	first := resultRecord(t, EmaWindowsTopic, 41)
	second := resultRecord(t, TradeAdvisoriesTopic, 42)
	client := &fakeClient{fetches: []kgo.Fetches{recordFetch(first, second)}}
	store := &fakeStore{}
	stamper := &fakeStamper{err: stampErr}
	c := testConsumer(client, store, stamper)
	var logs bytes.Buffer
	c.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	err := c.Run(context.Background())
	if !errors.Is(err, stampErr) || !strings.Contains(err.Error(), "publish materialize stamps") {
		t.Fatalf("Run error = %v; want stamp publish failure", err)
	}
	if store.saveCalls != 1 || len(client.commitBatches) != 0 || client.polls != 1 || !client.closed {
		t.Fatal("consumer committed offsets or continued past a stamp publish failure")
	}
	for _, field := range []string{`"operation":"publish materialize stamps"`, `"batch_size":2`} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("stamp failure log is missing %s: %s", field, logs.String())
		}
	}
}

func TestRunPreservesEarlierCommittedBatches(t *testing.T) {
	completed := resultRecord(t, EmaWindowsTopic, 40)
	first := resultRecord(t, EmaWindowsTopic, 41)
	failed := resultRecord(t, EmaWindowsTopic, 42)
	failed.Value = nil
	later := resultRecord(t, EmaWindowsTopic, 43)
	client := &fakeClient{fetches: []kgo.Fetches{recordFetch(completed), recordFetch(first, failed, later)}}
	store := &fakeStore{}
	if err := testConsumer(client, store, &fakeStamper{}).Run(context.Background()); err == nil {
		t.Fatal("expected invalid second record to fail")
	}
	if len(store.windows) != 1 || store.saveCalls != 1 || !reflect.DeepEqual(client.commits, []*kgo.Record{completed}) {
		t.Fatal("consumer lost the earlier batch commit or committed an incomplete batch")
	}
}

func TestRunLeavesPartiallyWrittenBatchUncommitted(t *testing.T) {
	writeErr := errors.New("second write failed")
	for _, topic := range []string{EmaWindowsTopic, TradeAdvisoriesTopic} {
		t.Run(topic, func(t *testing.T) {
			records := []*kgo.Record{
				resultRecord(t, topic, 41), resultRecord(t, topic, 42), resultRecord(t, topic, 43),
			}
			client := &fakeClient{fetches: []kgo.Fetches{recordFetch(records...)}}
			store := &fakeStore{saveErr: writeErr, failOnResult: 2}
			err := testConsumer(client, store, &fakeStamper{}).Run(context.Background())
			if !errors.Is(err, writeErr) || !strings.Contains(err.Error(), "offset=42") {
				t.Fatalf("Run error = %v; want second record write failure", err)
			}
			if len(store.windows)+len(store.advisories) != 0 || store.saveCalls != 1 || store.resultCalls != 2 ||
				len(client.commitBatches) != 0 || !client.closed {
				t.Fatal("partially written batch was committed or processing continued past its failure")
			}
		})
	}
}

func TestRunDoesNotCommitEmptyFetches(t *testing.T) {
	record := resultRecord(t, EmaWindowsTopic, 41)
	client := &fakeClient{fetches: []kgo.Fetches{{}, recordFetch(record)}}
	if err := testConsumer(client, &fakeStore{}, &fakeStamper{}).Run(context.Background()); !errors.Is(err, errPollingStopped) {
		t.Fatalf("Run error = %v", err)
	}
	if len(client.commitBatches) != 1 || !reflect.DeepEqual(client.commits, []*kgo.Record{record}) {
		t.Fatal("empty fetch triggered an offset commit")
	}
}

func TestRunStopsOnFetchError(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	client := &fakeClient{fetches: []kgo.Fetches{kgo.NewErrFetch(fetchErr)}}
	store := &fakeStore{}
	if err := testConsumer(client, store, &fakeStamper{}).Run(context.Background()); !errors.Is(err, fetchErr) {
		t.Fatalf("Run error = %v; want fetch error", err)
	}
	if len(store.windows)+len(store.advisories)+len(client.commits) != 0 || !client.closed {
		t.Fatal("consumer processed records on a fetch error or failed to close")
	}
}

func TestRunCancellationDoesNotCommit(t *testing.T) {
	for _, point := range []string{"before polling", "while polling"} {
		t.Run(point, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &fakeClient{fetches: []kgo.Fetches{recordFetch(resultRecord(t, EmaWindowsTopic, 41))}}
			store := &fakeStore{}
			switch point {
			case "before polling":
				cancel()
			case "while polling":
				client.poll = func(ctx context.Context) kgo.Fetches {
					cancel()
					return kgo.NewErrFetch(ctx.Err())
				}
			}
			if err := testConsumer(client, store, &fakeStamper{}).Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v; want context canceled", err)
			}
			if len(client.commits) != 0 || !client.closed {
				t.Fatal("canceled run committed an offset or failed to close")
			}
		})
	}
}

func TestRunCommitsSavedBatchDespiteCancellation(t *testing.T) {
	for _, point := range []string{"after database commit", "during offset commit"} {
		t.Run(point, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			records := []*kgo.Record{
				resultRecord(t, EmaWindowsTopic, 41), resultRecord(t, EmaWindowsTopic, 42),
			}
			client := &fakeClient{fetches: []kgo.Fetches{
				recordFetch(records...), recordFetch(resultRecord(t, EmaWindowsTopic, 43)),
			}}
			store := &fakeStore{}
			if point == "after database commit" {
				store.afterSave = cancel
			}
			client.commit = func(commitCtx context.Context) error {
				if point == "during offset commit" {
					cancel()
				}
				if err := commitCtx.Err(); err != nil {
					t.Fatalf("offset commit inherited caller cancellation: %v", err)
				}
				deadline, ok := commitCtx.Deadline()
				if remaining := time.Until(deadline); !ok || remaining <= 0 || remaining > commitTimeout {
					t.Fatalf("offset commit deadline = %v; want a timeout within %v", deadline, commitTimeout)
				}
				if client.rebalances != 0 || client.closed {
					t.Fatal("consumer released the rebalance gate or closed before committing offsets")
				}
				return nil
			}
			if err := testConsumer(client, store, &fakeStamper{}).Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v; want context canceled", err)
			}
			if store.saveCalls != 1 || len(store.windows) != 2 ||
				len(client.commitBatches) != 1 || !reflect.DeepEqual(client.commits, records) ||
				client.polls != 1 || !client.closed {
				t.Fatal("consumer failed to commit the saved batch and stop before polling again")
			}
		})
	}
}

func TestCommitBatchIgnoresExpiredCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	client := &fakeClient{commit: func(commitCtx context.Context) error {
		deadline, ok := commitCtx.Deadline()
		if remaining := time.Until(deadline); !ok || remaining <= 0 || remaining > commitTimeout {
			t.Fatalf("offset commit inherited expired caller deadline: %v", deadline)
		}
		return commitCtx.Err()
	}}
	record := resultRecord(t, EmaWindowsTopic, 41)
	if err := testConsumer(client, &fakeStore{}, &fakeStamper{}).commitBatch(ctx, []*kgo.Record{record}); err != nil {
		t.Fatalf("commitBatch error = %v; want successful offset commit", err)
	}
	if !reflect.DeepEqual(client.commits, []*kgo.Record{record}) {
		t.Fatal("expired caller deadline prevented the offset commit")
	}
}
