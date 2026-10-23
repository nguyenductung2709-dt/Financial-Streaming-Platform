# Streaming processor

Continuously consumes raw Protobuf `market.v1.PriceEvent` messages from Redpanda,
validates them, and computes closing trades in five-minute event-time windows
for each symbol. It logs a limited sample of completed windows; it does not
calculate EMA or persist business results yet.

## Code layout

- `StreamingProcessor`: `main` parses options, configures Flink, calls
  `buildPipeline`, and executes the job. `buildPipeline` only connects operators,
  so tests can inspect the real pipeline without starting a consumer.
- `ProcessorConfig`: command-line options and validation.
- `PriceEventDecoder`: Protobuf parsing and event validation.
- `PriceEventDeserializationSchema`: Kafka adapter that emits decoded events and
  adds topic/partition/offset information to decoding errors.
- `PriceEventTypeInformation`: Flink's internal Protobuf serializer and snapshot.
- `ClosingPriceAggregate`: selects the latest trade timestamp in each window;
  equal timestamps use the larger unsigned source row.
- `WindowCloseFunction`: adds the window start and end to the selected trade.
- `WindowCloseTypeInformation` / `WindowCloseSerializer`: explicit Flink
  serialization for window results, including their nested Protobuf trade.
- `SampleLoggingSink`: counts completed windows and logs a limited sample.
  Replace this endpoint with indicator output when adding the EMA pipeline.

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

Read sampled window results from worker logs:

```sh
docker compose logs -f taskmanager
```

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--brokers` | `redpanda-0:9092` | Kafka bootstrap servers inside Compose |
| `--topic` | `trade-events` | Topic containing raw PriceEvent Protobuf values |
| `--group-id` | `streaming-processor` | Kafka consumer group |
| `--starting-offsets` | `committed` | `committed`, `earliest`, or `latest` |
| `--parallelism` | `1` | Parallel instances per operator |
| `--checkpoint-interval-ms` | `30000` | Checkpoint interval, minimum 1000 ms |
| `--sample-limit` | `5` | Completed windows logged per sink subtask/attempt; zero disables |
| `--bounded` | Off | Consume up to end offsets captured at source startup, then finish |
| `--help` | | Print options without starting a job |

For a one-time replay, use a distinct group, `--starting-offsets earliest`, and
`--bounded`. This replays all retained events, not just newly produced events.

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
- `completedPriceWindows` is a sink metric. Counts and sample limits reset on a new
  attempt. Log samples may repeat after recovery: this inspection sink is not
  an exactly-once business output or durable storage.
- Windows are `[start, end)`, aligned to five-minute boundaries. They use the
  event's `trade_time`, with a 70-second out-of-order allowance (see the
  [dataset analysis](../../documents/event-time-analysis.md)). A window closes
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
