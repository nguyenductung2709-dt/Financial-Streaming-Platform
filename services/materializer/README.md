# Materializer

The materializer reads `ema-windows` and `trade-advisories` and writes them into
PostgreSQL, where the backend picks them up. The tables are described in
[docs/database.md](../../docs/database.md).

`make run-flow` runs it as part of the pipeline. To run it on its own:

```
docker compose up -d --wait postgres redpanda-0
docker compose run --rm create-topic
docker compose run --rm materializer -init-db
make run-materializer
```

`-init-db` creates any missing tables and exits. You only need it once. `-reset-db`
does the same and then empties the tables.

Configuration comes from environment variables. In the container, `compose.yaml` sets
the database URL and the brokers, so the only one you'd normally change is
`MATERIALIZER_GROUP_ID` (default `materializer`). For example:

```
make run-materializer MATERIALIZER_GROUP_ID=materializer-dev
```

If you run the binary directly with `go run ./services/materializer`, export
`MATERIALIZER_DATABASE_URL` yourself. The binary doesn't read `.env`.

## How writes work

The consumer polls up to 100 records at a time and decodes and validates all of them
before writing anything. It saves the batch in one transaction and only then commits
the offsets. If the write fails, nothing is committed and the batch is read again next
time.

Every write is an upsert on `(symbol, window_start)`, and the last value in a batch
wins. This means duplicates from Flink, and an advisory arriving before its window,
both work out. Prices stay integers and EMAs stay decimal strings the whole way, so
nothing passes through a float.

An invalid record stops the consumer and logs where it is. There's no automatic skip
and no dead-letter topic, so a bad record has to be handled by hand.

When `MATERIALIZER_STAMP_TOPIC` is set (`make measure-flow` does this), the
materializer also publishes a latency stamp for each saved result.

## Tests

```
make test-materializer
MATERIALIZER_TEST_DATABASE_URL='postgresql://financial:financial_dev@localhost:15432/financial_streaming?sslmode=disable' \
  make test-materializer-integration
```

The unit tests run with `-race`, which needs cgo and a C compiler. The integration
tests create and drop their own schemas, so they won't touch your data.
