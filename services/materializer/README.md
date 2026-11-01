# Materializer

The materializer will consume `ema-windows` and `trade-advisories`, decode their
Protobuf messages, and store results in PostgreSQL for the backend API.
The database schema, Go writing layer, and Protobuf mapping are implemented;
the Redpanda consumer follows separately.

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

## Go database layer

`data.Postgres` uses Go's `database/sql` with `github.com/lib/pq v1.12.3`.
`NewPostgres(ctx, databaseURL)` creates a connection pool and checks connectivity.
The caller closes the store when finished. Startup and writes use a ten-second
context timeout, or an earlier caller deadline. Connection attempts use the
driver's `connect_timeout`, defaulting to ten seconds; set a positive value in
the connection string to override it. Operations use the driver's normal context
cancellation, so a network failure can delay return beyond the context deadline.
Connection-string parsing errors are reported without exposing credentials.

`SaveEmaWindow` upserts the symbol and window in one transaction.
`SaveTradeAdvisory` upserts the symbol, embedded window, and advisory together.
Decimal values and unsigned prices are sent as text and cast to PostgreSQL
`NUMERIC`, preserving precision without floating-point conversion.

Run validation tests from the repository root:

```sh
go test -race ./services/materializer/internal/...
```

Run database integration tests against the local PostgreSQL service:

```sh
MATERIALIZER_TEST_DATABASE_URL='postgresql://financial:financial_dev@localhost:15432/financial_streaming?sslmode=disable' \
  go test -race -tags integration ./services/materializer/internal/data
```

The integration tests create temporary schemas, apply the migration there, and
remove those schemas afterward. They check duplicate delivery, either topic
arriving first, exact decimals, transaction rollback, and migration rollback.
