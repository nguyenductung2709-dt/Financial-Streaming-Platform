# Streaming processor

The streaming processor is a Flink 2.2 job written in Scala. It reads price events
from `trade-events`, takes the last trade of each symbol's five-minute window, and
updates that symbol's EMA 38 and EMA 100. Every result goes to `ema-windows`. When the
two EMAs cross, an advisory also goes to `trade-advisories`: BUY if EMA 38 moved above
EMA 100, SELL if it moved below.

## Building

You need Java 17 and sbt. From the repository root:

```
make test-streaming-processor
make build-streaming-processor
```

This writes `bin/streaming-processor.jar`. It's built with `sbt assembly` and bundles
the Kafka connector and relocated copies of Scala 2.13 and Protobuf. Flink itself comes
from the cluster. `make lint-streaming-processor` checks formatting and treats compiler
warnings as errors, the same way CI does. `make format-streaming-processor` fixes the
formatting.

## Running

`make run-flow` submits the job for you. To submit it by hand against the local
cluster:

```
make run-streaming-processor FLINK_ARGS="--starting-offsets earliest"
```

Each submission starts a new job, so cancel the old one in the
[Flink dashboard](http://localhost:8081) first. `make run-bounded-processor` processes
whatever is in `trade-events` right now and then finishes. A sample of the results is
logged to `docker compose logs -f taskmanager`.

The job's flags are `--brokers`, `--topic`, `--output-topic`, `--advisory-topic`,
`--group-id`, `--starting-offsets` (`committed`, `earliest` or `latest`),
`--parallelism`, `--checkpoint-interval-ms`, `--sample-limit` and `--bounded`.
`--latency-topic` turns on the latency stamps that
[monitoring](../../monitoring/README.md#latency) uses. `--help` lists the defaults.

## Windows and the watermark

Windows are five minutes long and aligned to the clock, and they use the event's
`trade_time`. The watermark trails the highest trade time seen so far by 5 seconds.
A window fires once the watermark passes its end, and any event that arrives after that
is dropped. [docs/out-of-orderness.md](../../docs/out-of-orderness.md) explains why we
chose 5 seconds. If you change it, change `OUT_OF_ORDERNESS_MS` in
`validation/config.py` as well.

An unbounded job only closes a window when a later event moves the watermark past it,
so the last windows of a replay stay open. A bounded job closes them when the input
ends.

## EMA calculation

Both EMAs start at zero for a new symbol and are updated once for every window that
contains a trade. There's no daily reset. Each update is rounded to 34 significant
digits (HALF_EVEN), and values are kept in price units rather than micros.

## Failures and recovery

Delivery is at least once, so recovering or replaying can produce duplicate results.
The materializer upserts, so duplicates are harmless.

The job doesn't skip bad input. A record that isn't valid Protobuf, a tombstone, a value
over 1 MiB, or an event with invalid fields fails the job, and the error names the
topic, partition and offset. Retrying won't get past such a record. You have to deal
with it yourself.

Offsets are committed with each checkpoint, and a failed job is retried three times,
ten seconds apart. Checkpoints on disk won't restart a job if the whole cluster is
lost. In that case, resubmit with `flink run -s <checkpoint>`.
