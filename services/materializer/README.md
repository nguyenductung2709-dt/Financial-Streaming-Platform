# Materializer

The materializer consumes `ema-windows` and `trade-advisories`, decodes their
Protobuf messages, and stores results in PostgreSQL for the backend API.
The database schema, Go writing layer, Protobuf mapping, Redpanda consumer, and
executable are implemented.

## Run the materializer

To start the complete producer → Flink → materializer → PostgreSQL flow, use
`make run-flow` from the repository root. It starts the local infrastructure and
creates the initial tables if missing. `make reset-flow` stops managed processes,
removes the local Compose containers, and deletes PostgreSQL, broker storage,
and Flink checkpoints. The next startup recreates the infrastructure and schema.
See the [full flow instructions](../../README.md#run-the-complete-local-flow).

To run just the materializer, follow these steps.

From the repository root, copy `.env.example` to `.env` once if you do not
already have one. Edit its `MATERIALIZER_*` settings, then start the local
services and create the topics:

```sh
docker compose up -d --wait postgres redpanda-0
docker compose run --rm create-topic
```

Apply the initial migration below once before the first run, then start the
materializer in its own terminal:

```sh
make run-materializer
```

The Makefile requires `.env` and loads the materializer settings from it.
Nonempty `MATERIALIZER_*` variables already set in your environment, or passed
as Make command-line variables, take precedence over `.env`.

| Environment variable | `.env.example` value |
| --- | --- |
| `MATERIALIZER_DATABASE_URL` | `postgresql://financial:financial_dev@localhost:15432/financial_streaming?sslmode=disable` |
| `MATERIALIZER_BROKERS` | `localhost:19092`; accepts comma-separated addresses |
| `MATERIALIZER_GROUP_ID` | `materializer` |

To customize these settings, edit `.env`, or override a setting for one run:

```sh
make run-materializer MATERIALIZER_GROUP_ID=materializer-dev
```

`make build-materializer` builds `bin/materializer`. When running that binary
directly, export `MATERIALIZER_DATABASE_URL` yourself; the binary reads environment
variables and does not load `.env`. Its broker and group defaults are the same.

Update `MATERIALIZER_DATABASE_URL` if you change PostgreSQL's credentials, port,
or database name. The database URL is never included in startup logs.

Ctrl+C or SIGTERM cancels processing. If the database has already committed the
current batch, the consumer first attempts to commit its offsets with a separate
ten-second timeout, then closes the consumer and database pool. A canceled batch
may replay on the next run if its offset commit fails or is uncertain. Configuration,
startup, or processing failures exit with a nonzero status; restart after
resolving the error. The executable does not apply migrations automatically.
The `-init-db` command initializes missing tables and exits; `-reset-db`
initializes them if needed, truncates the three read-model tables, and exits.
The full-flow scripts use these explicit commands with the same database URL
as the running service. Existing tables are preserved by initialization, and
partial schemas require manual repair. Neither command is a migration version
tracker.

## Tables

| Table | Purpose | Primary key |
| --- | --- | --- |
| `symbols` | Symbols observed in either result topic | `symbol` |
| `ema_windows` | Closing price and EMA38/EMA100 for each window | `symbol, window_start` |
| `trade_advisories` | BUY/SELL action and previous EMA values | `symbol, window_start` |

EMA windows reference their symbol. Advisories reference their EMA window,
which supplies the closing price, current EMAs, and window end without duplicating
those values. An advisory contains the entire EMA window in Protobuf: its writer
must upsert the symbol and embedded EMA window before saving the advisory, all
in one transaction. This supports either topic arriving first.

Window timestamps use `TIMESTAMPTZ`. EMA values use unconstrained `NUMERIC` to
preserve their decimal strings. Closing prices use `NUMERIC(20, 0)` with the
positive `uint64` range enforced, and windows must be exactly five minutes long.
The primary keys support repeated delivery when the writer uses upserts.

## Apply the initial migration

From the repository root, start PostgreSQL if needed:

```sh
docker compose up -d --wait postgres
```

Apply the migration once:

```sh
docker compose exec -T postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < services/materializer/migrations/001_create_read_models.up.sql
```

The SQL runs in a transaction. It fails if the tables already exist; it does not
drop or replace existing tables. This step uses a manual SQL migration, without
an automatic version-tracking runner. Add future changes as new migrations and
keep track of which migrations have been applied. Container startup does not
apply migrations automatically. Each migration has an `.up.sql` file to apply
the change and a `.down.sql` file to reverse it.

The live dashboard also requires migrations 002, 003, and 004. Apply only missing
migrations, in order, following the [backend setup](../backend/README.md).
The materializer's `-init-db` flag initializes migration 001 only. `make run-flow`
also runs the backend's setup helper to apply missing migrations through 004,
while preserving existing results. Roll back newer migrations in reverse order before
rolling back 001.

To roll back this initial migration, run the following command. It drops all
three materializer tables and their stored results:

```sh
docker compose exec -T postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < services/materializer/migrations/001_create_read_models.down.sql
```

## Check the schema

After applying the migration:

```sh
docker compose exec -T postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < services/materializer/tests/schema_test.sql
```

The checks cover duplicate writes, exact price/EMA precision, equivalent time
zones, foreign keys, the price range, window duration, and advisory actions.
Fixtures are rolled back, leaving application data unchanged.

## Go models

`internal/data/symbol.go`, `internal/data/ema_window.go`, and `internal/data/trade_advisory.go` define the
materializer's Go models. Timestamps use `time.Time`, prices use `uint64`, and
EMA values use exact decimal strings. Database operations live beside their
models in the same `data` package. The shared `upsertSymbol` helper uses the
current transaction, so failed result writes also roll back symbol creation.
The mapper converts generated Protobuf messages into the result models
before writing to the database. This package is internal to the materializer;
other services use their own data access code.

## Protobuf mapping

`internal/mapper` exposes `DecodeEmaWindow(payload)` and
`DecodeTradeAdvisory(payload)` for raw Protobuf record values. Both return a
validated `data` model, or an error with a nil model. They reject empty values,
malformed Protobuf, missing or invalid timestamps, invalid window fields, and
unsupported advisory actions. Timestamps are checked before converting to
`time.Time`; prices and decimal strings are copied without floating-point
conversion. Unknown Protobuf fields are accepted for schema compatibility.

The mapper and database writers share the models' `Validate` methods. The mapper
performs no database operations and has no dependency on the consumer.

## Redpanda consumer

`internal/consumer.New(brokers, groupID, store, logger)` creates a consumer for
`ema-windows` and `trade-advisories`, using the existing `franz-go` dependency.
The store implements `SaveBatch(ctx, []data.Result)`; `*data.Postgres` already
satisfies this interface. A nil logger uses Go's default structured
logger. The consumer resumes from committed group offsets, falling back to the
earliest available records when the group has no committed offset.

`Run(ctx)` polls up to 100 records at a time. It returns available records without
waiting for a full batch, then decodes and validates every record before starting
database writes. The store deduplicates repeated keys using the last input value,
then runs multi-row upserts inside one PostgreSQL transaction. Auto-commit is
disabled. Only after that database transaction commits does one synchronous offset commit
advance each topic-partition past its last saved record. A full batch uses one
database transaction, at most three upsert statements, and one offset-commit call
for 100 records. Offset commits have an independent ten-second timeout. After a
successful database commit, caller cancellation or deadline expiry does not
interrupt the offset commit; cancellation stops processing before the next poll.
Rebalances are blocked while processing and committing the batch.

A decoding or model-validation error stops the loop and logs the affected
record's topic, partition, offset, and error. Its batch remains uncommitted, so
restarting encounters the same invalid record until the decoding/validation issue
is fixed or an operator explicitly handles the record. There is no automatic
skip or dead-letter topic. Multi-row SQL errors, transaction-level
errors, and offset-commit errors log the batch size, last record location, and
error. Fetch errors stop the loop and log their topic and partition. A failed write rolls
back the whole database batch and leaves its offsets uncommitted; earlier
completed batches stay committed. If the database commits but the broker commit
fails, or its outcome is uncertain, the batch may replay. The existing upserts
handle repeated delivery.

`Run` closes the Redpanda client on return, including on cancellation or failure.
Create a new consumer to retry from committed offsets; do not reuse a failed
instance. `Close()` also closes a consumer created without calling `Run`. The
caller owns the database store and closes it after the consumer stops.

The consumer tests use fake broker and store interfaces to check routing,
write-before-commit ordering, failure handling, and cancellation. They run
without a broker or database. Full flow tests with Redpanda
and PostgreSQL are a separate step.

## Go database layer

`data.Postgres` uses Go's `database/sql` with `github.com/lib/pq v1.12.3`.
`NewPostgres(ctx, databaseURL)` creates a connection pool and checks connectivity.
The caller closes the store when finished. Startup and each database transaction
use a ten-second context timeout, or an earlier caller deadline. For `SaveBatch`,
that timeout covers the whole transaction. Connection attempts use the
driver's `connect_timeout`, defaulting to ten seconds; set a positive value in
the connection string to override it. Operations use the driver's normal context
cancellation, so a network failure can delay return beyond the context deadline.
Connection-string parsing errors are reported without exposing credentials.

`SaveEmaWindow` upserts the symbol and window in one transaction.
`SaveTradeAdvisory` upserts the symbol, embedded window, and advisory together.
`SaveBatch` accepts an ordered slice of EMA windows and trade advisories. It
validates every input before opening a transaction, including inputs overwritten
by later records. For each `(symbol, window_start)`, the last window value wins
across both standalone windows and embedded advisory windows; the last advisory
independently supplies the action and previous EMAs. Equivalent timestamp time
zones identify the same key. SQL writes are sorted by key for consistent row-lock
ordering, then applied to symbols, windows, and advisories in foreign-key order.
The single-result methods share the same parameterized upsert helpers.

Each table uses one multi-row statement for a normal 100-record batch; an empty
advisory set needs no statement. Larger direct `SaveBatch` calls split each table
into statements of at most 1,000 rows, all inside the same transaction, to bound
the number of SQL parameters. A `data.BatchError` identifies the zero-based input
that failed validation. SQL execution and commit failures are batch-level errors,
because a multi-row statement does not reliably identify one source record.
Decimal values and unsigned prices are sent as text and cast to PostgreSQL
`NUMERIC`, preserving precision without floating-point conversion.

Run validation tests from the repository root:

```sh
make test-materializer
```

This runs uncached race tests and writes coverage to `bin/materializer-coverage.out`.
Use `make format-materializer` to format this service; `make formatall` includes it.
The pre-commit hook formats staged materializer Go files and runs recursive vet
checks against the staged snapshot.

Run database integration tests against the local PostgreSQL service:

```sh
MATERIALIZER_TEST_DATABASE_URL='postgresql://financial:financial_dev@localhost:15432/financial_streaming?sslmode=disable' \
  make test-materializer-integration
```

The integration tests create temporary schemas, apply the migration there, and
remove those schemas afterward. They check duplicate delivery, either topic
arriving first, exact decimals, last-value semantics for mixed and duplicate
batches, full batch rollback on write or commit errors, cancellation, statement
chunking, and migration rollback.
