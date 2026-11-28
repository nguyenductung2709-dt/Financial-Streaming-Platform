# Materializer

The materializer is a Go service that moves Flink results into PostgreSQL. It reads the 
`ema-windows` and `trade-advisories` topics from Redpanda as one consumer group and stores 
each result in the database. The [backend](../backend/README.md) then reads the data from 
there. The tables are described in [docs/database.md](../../docs/database.md).

## Why PostgreSQL

- It supports upserts, so reprocessing a record just overwrites the existing row
- It stores prices and EMAs exactly, without floating-point rounding
- It can notify listeners when a row changes. The backend uses this to push live updates to 
the dashboard over SSE

The database has three tables: `symbols`, `ema_windows` and `trade_advisories`. The last two 
are keyed by symbol and window start.

## Running the materializer

`make run-flow` runs the materializer as part of the pipeline. To run it on its own:

```
docker compose up -d --wait postgres redpanda-0
docker compose run --rm create-topic
docker compose run --rm materializer -init-db
make run-materializer
```

`-init-db` creates any missing tables and exits. You only need it once.

`-reset-db` does the same and then empties the tables.

## Configuration

Configuration comes from environment variables. In the container, `compose.yaml` sets
the database URL and the brokers. The only thing you would normally change is 
`MATERIALIZER_GROUP_ID` (default `materializer`), for example:

```
make run-materializer MATERIALIZER_GROUP_ID=materializer-dev
```

If you run the binary directly with `go run ./services/materializer`, export
`MATERIALIZER_DATABASE_URL` yourself. The binary does not read `.env`.

If `MATERIALIZER_STAMP_TOPIC` is set (`make measure-flow` does this), the materializer also 
publishes a latency stamp for each saved result.

## How writes work

The consumer reads up to 100 records at a time. It decodes and validates every record in the 
batch before writing anything. It then saves the whole batch in a single database transaction 
(using one multi-row upsert per table), and only after that transaction commits does it 
commit the offsets to Redpanda. If the write fails, the transaction is rolled back and no 
offsets are committed, so the same batch is read again.

The same happens if the materializer crashes after the database commit but before the offset 
commit: the batch is read again. This is safe because every write is an upsert on `(symbol, 
window_start)`, so re-reading a batch overwrites the same rows with the same values instead 
of adding duplicates. If a batch contains the same key more than once, only the last one is 
written. Together with the at-least-once Flink job, this gives at-least-once delivery into 
the database.

If `MATERIALIZER_STAMP_TOPIC` is set, the latency stamps are published after the database 
commit and before the offsets are committed. If publishing fails, the batch is read again.

Each advisory message carries its full EMA window, and the materializer saves the window 
together with the advisory (symbols first, then windows, then advisories). The two topics can 
therefore be consumed in any order without breaking the database rule that every advisory 
needs a matching window.

Prices are stored as integers (micros) and EMAs as decimal strings, so no value passes 
through a float.

An invalid record stops the consumer and logs its topic, partition and offset. There is no 
automatic skip and no dead-letter topic, so a bad record has to be handled by hand.

## Tests

```
make test-materializer
MATERIALIZER_TEST_DATABASE_URL='postgresql://financial:financial_dev@localhost:15432/
financial_streaming?sslmode=disable' \
  make test-materializer-integration
```

The unit tests run with `-race`, which needs cgo and a C compiler. The integration tests 
create and drop their own schemas, so they will not touch your data.
