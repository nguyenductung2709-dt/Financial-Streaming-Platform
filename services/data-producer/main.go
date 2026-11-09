package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type config struct {
	csvPaths   []string
	brokers    []string
	topic      string
	dlqTopic   string
	dlqEnabled bool
	rate       int
	speed      float64
	maxEvents  uint64
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := run(cfg); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseConfig(args []string, output io.Writer) (config, error) {
	var cfg config
	var brokersText string
	flags := flag.NewFlagSet("data-producer", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Func("csvPath", "CSV file or directory (immediate .csv files, alphabetical); repeat for ordered inputs", func(path string) error {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("CSV path must not be empty")
		}
		cfg.csvPaths = append(cfg.csvPaths, path)
		return nil
	})
	flags.StringVar(&brokersText, "brokers", "localhost:19092", "Comma-separated broker addresses")
	flags.StringVar(&cfg.topic, "topic", "trade-events", "Trade event Redpanda topic")
	flags.StringVar(&cfg.dlqTopic, "dlqTopic", "trade-events-dlq", "Trade event dead letter queue Redpanda topic")
	flags.BoolVar(&cfg.dlqEnabled, "dlqEnabled", false, "Enable the dead letter queue topic for invalid trade events")
	flags.IntVar(&cfg.rate, "rate", 0, "Maximum events per second (0 = unlimited)")
	flags.Float64Var(&cfg.speed, "speed", 0, "Replay the gaps between trade times, sped up by this factor (0 = off; excludes -rate)")
	flags.Uint64Var(&cfg.maxEvents, "maxEvents", 0, "Maximum acknowledged events to publish (0 = unlimited)")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected arguments: %q; use named flags such as -csvPath=<path>", flags.Args())
	}
	if cfg.rate < 0 || cfg.rate > int(time.Second) {
		return config{}, fmt.Errorf("rate must be between 0 and 1000000000 events per second")
	}
	// !(x >= 0) also rejects NaN.
	if !(cfg.speed >= 0) || math.IsInf(cfg.speed, 1) {
		return config{}, fmt.Errorf("speed must be a finite number, zero or above")
	}
	if cfg.speed > 0 && cfg.rate > 0 {
		return config{}, fmt.Errorf("use either -rate or -speed, not both")
	}
	if len(cfg.csvPaths) == 0 {
		cfg.csvPaths = []string{"dataset/debs2022-gc-trading-day-12-11-21.csv"}
	}
	cfg.topic = strings.TrimSpace(cfg.topic)
	if cfg.topic == "" {
		return config{}, fmt.Errorf("topic must not be empty")
	}
	cfg.dlqTopic = strings.TrimSpace(cfg.dlqTopic)
	if cfg.dlqEnabled {
		if cfg.dlqTopic == "" {
			return config{}, fmt.Errorf("DLQ topic must not be empty when DLQ is enabled")
		}
		if cfg.dlqTopic == cfg.topic {
			return config{}, fmt.Errorf("DLQ topic must differ from the trade event topic")
		}
	}
	cfg.brokers = strings.Split(brokersText, ",")
	for i := range cfg.brokers {
		cfg.brokers[i] = strings.TrimSpace(cfg.brokers[i])
		if cfg.brokers[i] == "" {
			return config{}, fmt.Errorf("broker address must not be empty")
		}
	}
	return cfg, nil
}

func run(cfg config) error {
	// Create context
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Create the Redpanda client
	client, err := newRedpandaClient(cfg.brokers)
	if err != nil {
		return err
	}
	return runWithClient(ctx, cfg, client, os.Stdout)
}

// The real client satisfies this interface; unit tests provide a fake client.
type producerClient interface {
	priceEventProducer
	Ping(context.Context) error
	Close()
}

func runWithClient(ctx context.Context, cfg config, client producerClient, output io.Writer) error {
	defer client.Close()

	// Fail promptly if the broker cannot be reached.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := client.Ping(pingCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("connect to Redpanda: %w", err)
	}

	var published, skipped, rejected uint64
	var interval time.Duration
	if cfg.rate > 0 {
		interval = time.Second / time.Duration(cfg.rate)
		// Round up so truncating to nanoseconds does not exceed the configured rate.
		if time.Second%time.Duration(cfg.rate) != 0 {
			interval++
		}
	}
	var nextPublish time.Time
	replay := replaySchedule{speed: cfg.speed}
	// How far the sends fell behind the replay schedule; large values mean the
	// producer could not keep up with -speed, so the load was not the replayed one.
	var maxReplayLag time.Duration
	limitReached := errors.New("event limit reached")

	// Keep rate, limits, and counters continuous across the entire input sequence.
	err = StreamCSVs(cfg.csvPaths, func(row SourceRow) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		event, skip, err := rowToPriceEvent(row)
		if err != nil {
			if !cfg.dlqEnabled {
				return err
			}

			rejectedEvent, dlqErr := rowToPriceEventDLQ(row, err.Error())
			if dlqErr != nil {
				return dlqErr
			}
			if dlqErr := publishPriceEventDLQ(ctx, client, cfg.dlqTopic, rejectedEvent); dlqErr != nil {
				return dlqErr
			}
			rejected++
			return nil
		}
		if skip {
			skipped++
			return nil
		}

		if interval > 0 {
			if err := waitForReplay(ctx, nextPublish); err != nil {
				return err
			}
			// Schedule from the actual send start. Slow sends never create catch-up bursts.
			nextPublish = time.Now().Add(interval)
		}
		if cfg.speed > 0 {
			due := replay.due(event.GetTradeTime().AsTime(), time.Now())
			if err := waitForReplay(ctx, due); err != nil {
				return err
			}
			if lag := time.Since(due); lag > maxReplayLag {
				maxReplayLag = lag
			}
		}
		if err := publishPriceEvent(ctx, client, cfg.topic, event); err != nil {
			return err
		}

		published++
		if cfg.maxEvents > 0 && published >= cfg.maxEvents {
			return limitReached
		}
		return nil
	})

	// Report acknowledged events and intentionally skipped rows, including partial runs.
	status := "completed"
	if errors.Is(err, limitReached) {
		status = "limit reached"
		err = nil
	}
	if err != nil {
		status = "stopped"
	}
	fmt.Fprintf(output, "CSV processing %s: published=%d skipped=%d rejected=%d topic=%q",
		status, published, skipped, rejected, cfg.topic)
	if cfg.speed > 0 {
		fmt.Fprintf(output, " replay_lag_max_ms=%d", maxReplayLag.Milliseconds())
	}
	fmt.Fprintln(output)
	if err != nil {
		return err
	}
	return nil
}

// Wait only for published events. Skipped/rejected rows do not consume rate slots.
func waitForReplay(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
