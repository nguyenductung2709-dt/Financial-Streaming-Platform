# Materializer

The materializer will consume `ema-windows` and `trade-advisories`, decode their
Protobuf messages, and store results in PostgreSQL for the backend API.
This first step supplies the database schema; the Go consumer follows separately.

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
  < services/materializer/migrations/001_create_read_models.sql
```

The SQL runs in a transaction. It fails if the tables already exist; it does not
drop or replace existing tables. This step uses a manual SQL migration, without
an automatic version-tracking runner. Add future changes as new migrations and
keep track of which migrations have been applied. Container startup does not
apply migrations automatically.

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
