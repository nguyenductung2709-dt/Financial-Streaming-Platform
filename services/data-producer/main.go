package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type config struct {
	csvPath   string
	brokers   []string
	topic     string
	rate      int
	maxEvents uint64
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
	flags.StringVar(&cfg.csvPath, "csvPath", "dataset/debs2022-gc-trading-day-12-11-21.csv", "Path of the CSV file")
	flags.StringVar(&brokersText, "brokers", "localhost:19092", "Comma-separated broker addresses")
	flags.StringVar(&cfg.topic, "topic", "trade-events", "Redpanda topic")
	flags.IntVar(&cfg.rate, "rate", 0, "Maximum events per second (0 = unlimited)")
	flags.Uint64Var(&cfg.maxEvents, "max-events", 0, "Maximum acknowledged events to publish (0 = unlimited)")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected arguments: %q; use named flags such as -csvPath=<path>", flags.Args())
	}
	if cfg.rate < 0 || cfg.rate > int(time.Second) {
		return config{}, fmt.Errorf("rate must be between 0 and 1000000000 events per second")
	}
	if strings.TrimSpace(cfg.csvPath) == "" {
		return config{}, fmt.Errorf("CSV path must not be empty")
	}
	cfg.topic = strings.TrimSpace(cfg.topic)
	if cfg.topic == "" {
		return config{}, fmt.Errorf("topic must not be empty")
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
	limitReached := errors.New("event limit reached")

	// Call StreamCSV with handler
	err = StreamCSV(cfg.csvPath, func(row SourceRow) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		event, skip, err := rowToPriceEvent(row)
		if errors.Is(err, errMissingTradingDate) {
			rejected++
			// Limit warnings so large replays do not print a message for every rejected row.
			if rejected <= 5 {
				fmt.Fprintf(output, "Rejected CSV line %d: %v\n", row.Line, err)
			}
			return nil
		}
		if err != nil {
			return err
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
	fmt.Fprintf(output, "CSV processing %s: published=%d skipped=%d rejected=%d topic=%q\n",
		status, published, skipped, rejected, cfg.topic)
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
