# Streaming processor

Continuously consumes raw Protobuf `market.v1.PriceEvent` messages from Redpanda,
validates them, and computes closing trades in five-minute event-time windows
for each symbol. Completed windows update EMA 38 and EMA 100 in keyed managed
state. Every EMA result is published as raw Protobuf `market.v1.EmaWindow` to
`ema-windows`. Crossovers produce `market.v1.TradeAdvisory` messages on
`trade-advisories`. Both streams also log a limited sample.

## Code layout

Application packages live under `src/main/scala/financial/streaming/`:

```text
StreamingProcessor.scala       # Entry point and pipeline wiring
config/
  ProcessorConfig.scala        # Command-line options and validation
source/
  PriceEventDecoder.scala      # Protobuf parsing and business-field validation
  PriceEventDeserializationSchema.scala  # Kafka decoding adapter
window/
  ClosingPriceAggregate.scala  # Select the closing trade
  WindowCloseFunction.scala    # Attach window boundaries
model/
  WindowClose.scala            # Completed window and closing trade
  EmaResult.scala              # Window and calculated EMA values
  CrossoverResult.scala        # Crossover action and previous/current EMA values
serialization/
  PriceEventTypeInformation.scala  # PriceEvent type, serializer, and snapshot
  WindowCloseSerializer.scala
  WindowCloseTypeInformation.scala
  EmaResultSerializer.scala
  EmaResultTypeInformation.scala
  CrossoverResultSerializer.scala
  CrossoverResultTypeInformation.scala
ema/
  EmaProcessFunction.scala     # Per-symbol EMA 38/100 and managed state
advisory/
  CrossoverProcessFunction.scala  # Per-symbol BUY/SELL crossover detection
sink/
  EmaWindowMapper.scala        # Convert results to Protobuf
  EmaLoggingSink.scala         # Count EMA results and log samples
  EmaRedpandaSink.scala        # Publish EMA windows
  TradeAdvisoryMapper.scala    # Convert crossovers to Protobuf
  AdvisoryLoggingSink.scala   # Count advisories and log samples
  AdvisoryRedpandaSink.scala   # Publish trade advisories
```

Tests mirror these packages under `src/test/scala/financial/streaming/`.
The pipeline test remains beside the entry point.

Moving the serializer snapshot classes into `serialization` changes their fully
qualified names. Restoring older checkpoints/savepoints requires keeping their
old snapshot classes available or migrating the state; use a fresh replay for
this development version.

## Build and test

Requires Java 17, sbt, and the Protobuf generators documented in
[the schemas guide](../../schemas/market/v1/README.md).

From the repository root:

```sh
make generate-protobuf
make test-streaming-processor
make build-streaming-processor
```

The deployment artifact is `bin/streaming-processor.jar`. sbt builds in the
service's ignored `target/` directory, then the Makefile copies the assembled JAR.
The assembly includes Kafka connector/client plus relocated Scala 2.13 and
Protobuf runtimes. Scala is relocated to avoid conflicts with the cluster's
Scala 2.12 classes. Flink and SLF4J are supplied by the cluster. Use `sbt assembly`, not
`sbt package`, for deployment.

Flink 2.2.1 / Java 17 is paired with Kafka connector 5.0.0-2.2, following the
[Apache compatibility matrix](https://flink.apache.org/downloads/).
No separate Kafka connector installation is needed in the cluster.

## Formatting and lint

From the repository root:

```sh
make format-streaming-processor  # Apply Scalafmt to Scala sources and sbt files
make lint-streaming-processor    # Check formatting and compile application/tests
```

Scalafmt uses the pinned version and style in `.scalafmt.conf`, also readable by
Metals when formatting in the editor. Generated Java bindings and Metals support
files are excluded. Compiler lint and unused-import checks are enabled; warnings
fail the build. CI checks formatting and lint before running tests and packaging.

## Run on the local Flink cluster

Start infrastructure from the repository root:

```sh
docker compose up -d redpanda-0 create-topic jobmanager taskmanager
```

If upgrading an already running cluster, wait for a healthy TaskManager in the
[Flink dashboard](http://localhost:8081) before submitting. Save and restore any
existing jobs explicitly before changing the runtime version.

Copy and submit the JAR:

```sh
docker compose cp bin/streaming-processor.jar jobmanager:/tmp/streaming-processor.jar
docker compose exec jobmanager flink run -d /tmp/streaming-processor.jar \
  --brokers redpanda-0:9092 \
  --topic trade-events \
  --group-id streaming-processor
```

Do not resubmit the same continuous job repeatedly: each submission creates a
separate Flink job. Cancel the old job before replacing it.

Read sampled EMA results from worker logs (timestamps are UTC):

```sh
docker compose logs -f taskmanager | rg 'EmaWindow symbol=|TradeAdvisory symbol='
```

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--brokers` | `redpanda-0:9092` | Kafka bootstrap servers inside Compose |
| `--topic` | `trade-events` | Topic containing raw PriceEvent Protobuf values |
| `--output-topic` | `ema-windows` | Topic for raw EmaWindow Protobuf values |
| `--advisory-topic` | `trade-advisories` | Topic for raw TradeAdvisory Protobuf values |
| `--latency-topic` | Off | Topic for raw StageStamp Protobuf values; setting it turns on per-stage latency stamps (see [monitoring](../../monitoring/README.md#latency)) |
| `--group-id` | `streaming-processor` | Kafka consumer group |
| `--starting-offsets` | `committed` | `committed`, `earliest`, or `latest` |
| `--parallelism` | `1` | Parallel instances per operator |
| `--checkpoint-interval-ms` | `30000` | Checkpoint interval, minimum 1000 ms |
| `--sample-limit` | `5` | EMA/advisory results logged per sink subtask/attempt; zero disables |
| `--bounded` | Off | Consume up to end offsets captured at source startup, then finish |
| `--help` | | Print options without starting a job |

For a one-time replay, use a distinct group, `--starting-offsets earliest`, and
`--bounded`. This replays all retained events, not just newly produced events.

## EMA flow

`trade-events → decode → keyBy(symbol) → five-minute closing prices → keyBy(symbol) → EMA → ema-windows + logs`

`EMA → keyBy(symbol) → crossovers → trade-advisories + logs`

Both EMAs start at zero for a new symbol. Each non-empty window applies the
course formula with `j = 38` and `j = 100`. Empty windows leave state unchanged;
there is no daily reset. Each update uses 34 significant decimal digits with
HALF_EVEN rounding. EMA values are in price units, not micros.

Flink keeps the previous values in keyed managed state with decimal serializers.
`EmaWindowMapper` preserves decimal precision with `toPlainString` and converts
window boundaries to Protobuf timestamps. The log sink uses the mapper for each
sample. `EmaResultSerializer` handles internal Flink transfers; the mapper defines
the Protobuf EMA output. The crossover operator compares each new EMA pair with
its previous pair, emitting BUY on an upward cross and SELL on a downward cross.
`TradeAdvisoryMapper` includes the current window, action, and previous EMA values.
Both Redpanda sinks use UTF-8 symbol keys and at-least-once delivery; recovery or
replay can produce duplicates. Input, EMA output, and advisory topics must differ.

## Out-of-orderness allowance

The watermark is the highest `trade_time` seen so far minus an allowance of
**5 seconds** (`OutOfOrdernessAllowance`, `forBoundedOutOfOrderness`). A window
fires once the watermark passes its end, so every result waits for the
allowance. An event that arrives after its window has fired is dropped. The
allowance trades latency against correctness:

- **Too small:** a late trade is dropped without any error. Its window's closing
  price is then wrong, and because each EMA builds on the previous one, so is
  every later window of that symbol. In the 3-day validation run, 5,506 dropped
  events changed 229,999 windows this way.
- **Too large:** every result is delayed by the allowance. In a replay, the
  allowance passes in milliseconds of wall-clock time. In live operation, it is
  real seconds before each EMA and advisory.

### Measurement

The allowance a trade needs is how far the watermark has already passed its
window when the trade arrives: `max trade time seen - window end + 1 ms`, in
file order. File order is also the topic order, because the producer replays
each file in order into one partition. Over all five trading days
(about 59.4M trades):

| Day | Trades that need any allowance | Largest needed |
| --- | --- | --- |
| 08-11-21 | 79 | 25 ms |
| 09-11-21 | 76 | 121 ms |
| 10-11-21 | 54 | 15 ms |
| 11-11-21 | 119 | 264 ms |
| 12-11-21 | 24 | 18 ms |

These trades are timestamped just before a window boundary and arrive a few
milliseconds after a newer trade from another exchange. The worst cases are
Paris (`.FR`) and Amsterdam (`.NL`) symbols, so this is ordinary skew between
exchange feeds.
Real trades dropped at each allowance, over the five days: 352 at 0 s, and none
at 1, 2, 5, 10, 30 or 70 s. The 13-11 and 14-11 files (weekend) contain no
trades.

Two effects look like larger lateness but do not need a larger allowance:

- **Xetra closing auction:** every day, about 430 trades with `trade_time` in
  17:30 arrive while the stream is already at 17:31, up to 65 s behind the
  newest trade. They belong to the 17:30-17:35 window, which is still open, so
  they are kept with any allowance. The earlier 70 s allowance was sized to this
  65 s gap.
- **Placeholder index rows on 10-11-21:** 5,506 index values with a
  `Trading time` of exactly 01:00:00.000 and an empty `Trading date` appear at
  about 18:56 in the file. Flink drops them as late under every allowance (they
  are almost 18 hours behind), and `compare-flow` classifies the affected
  windows as `lateness_explained`.

### Why 5 seconds

The smallest allowance that keeps every real trade is 264 ms. We use 5 s,
about 20 times that, because:

1. **The worst case varies by day:** from 15 ms to 264 ms over only five days.
   An order of magnitude of headroom covers days that are worse than the ones
   we have.
2. **A replay has the best possible ordering:** one ordered file, one partition.
   Live feeds from three exchanges arrive over a network, and a short delay on
   one feed easily adds a second or more.
3. **The costs are not symmetric:** a too-small allowance silently corrupts a
   symbol's EMAs from then on, while each extra second only delays results by
   that second. 5 s is under 2% of a five-minute window, against 23% for 70 s.

On this dataset, 5 s and 70 s drop exactly the same events, so their results
are identical. 5 s is chosen for the system's live latency: each result arrives
65 s earlier. If the allowance changes, change `OUT_OF_ORDERNESS_MS` in
`validation/config.py` with it, because the validation and latency tools apply
the same rule.

## Processing and recovery

- New executions start at committed group offsets, falling back to earliest if
  no offset exists. Explicit `earliest`/`latest` applies when no Flink source
  state is restored. Checkpoint/savepoint source state takes precedence.
- Kafka auto-commit and topic auto-creation are disabled. Completed checkpoints
  commit source offsets. `read_committed` avoids reading aborted transactions.
- Checkpoints are enabled; the cluster supplies filesystem storage settings.
  Flink retries failed jobs three times with ten seconds between attempts.
- Invalid Protobuf, tombstones, oversized values (over 1 MiB), or invalid event
  fields fail the job with topic, partition, and offset. No invalid records are
  silently dropped. The producer's RejectedRow DLQ has a different schema and
  must not be consumed as a PriceEvent topic. Repair/quarantine a poison record
  deliberately; automatic retries do not repair it.
- A dedicated Protobuf serializer preserves uint64 values, including values
  beyond signed JVM Long range, and avoids generic Kryo serialization.
- `completedEmaWindows` is a sink metric. Counts and sample limits reset on a new
  attempt. Log samples may repeat after recovery: this inspection sink is not
  an exactly-once business output or durable storage.
- Windows are `[start, end)`, aligned to five-minute boundaries. They use the
  event's `trade_time`, with a 5-second out-of-order allowance (see
  [Out-of-orderness allowance](#out-of-orderness-allowance)). A window closes
  when its watermark reaches the end; this is event-time progress, not a
  wall-clock timer. Empty windows produce no result.
- A continuous stream that stops receiving events may leave its last windows
  open. A bounded replay emits the final watermark and closes remaining windows.
- Events arriving after their window has closed are currently dropped by Flink's
  default zero allowed lateness. Late-event side output/metrics are not yet added.
- Window accumulators and results use explicit serializers; generic Kryo is not
  needed for these types.
- The bundled defaults target the local plaintext broker. Configure authenticated
  transport and secrets before connecting to a secured deployment.

Persistent checkpoint files alone do not restart jobs after the entire standalone
cluster is lost. Submit again with `flink run -s <checkpoint-or-savepoint-path>`
when restoring, or deploy with a suitable HA/application lifecycle setup.
