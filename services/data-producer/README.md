# Data producer

The producer is a Go program that replays the trade CSV files into the `trade-events`
topic. Each row is published as a `market.v1.PriceEvent`, keyed by symbol. It runs as a
container next to Redpanda.

`make run-flow` starts it for you. To run it on its own, bring up Redpanda first:

```
make infra-up
make run-data-producer ARGS="-csvPath=dataset/<file>.csv -rate=1000 -dlqEnabled"
```

`ARGS="-help"` prints every flag. These are the ones you'll usually want:

- `-csvPath` is a CSV file, or a directory whose `.csv` files are read in alphabetical
  order. Repeat it to replay several inputs one after another. If you leave it out, the
  producer reads `dataset/debs2022-gc-trading-day-12-11-21.csv`
- `-rate` sends events evenly spaced at a fixed number per second. The default, 0, means no 
limit
- `-maxEvents` stops after that many acknowledged events
- `-dlqEnabled` sends invalid rows to `trade-events-dlq` instead of stopping the run

Inputs are mounted into the container read-only, so any host path works, except a path
that contains a `:`.

## What it does with each row

With several input files, the counters and the rate limit continue across them, so a
whole week can be replayed as one stream.

From each row the producer reads only the symbol, security type, last price, trading
date and trading time.

- Rows with a missing or zero last price (quote or housekeeping updates) are skipped
- Index updates often have an empty trading date, so the producer uses the row's
  system date for them
- Prices are converted to integer millionths (micros) instead of floating-point
  numbers, so no rounding error enters the pipeline
- With `-dlqEnabled`, a row that still cannot be parsed is published to
  `trade-events-dlq` with its row number, its values and the reason. Without it, the
  run stops

## How it publishes

The producer uses the [franz-go](https://github.com/twmb/franz-go) client. It doesn't wait 
for each acknowledgment before reading the next row. It hands each record to the client, 
which groups records into batches with a 1 ms linger.

Up to 10,000 records can be waiting for an acknowledgment. After that the producer
blocks, so a slow broker slows the replay down instead of filling up memory.
Idempotent writes keep each partition in CSV order. The first record that fails stops
the replay.

**!!!**

Throughput: about <N> events/s without a rate limit, compared with about <M> events/s
when waiting for every acknowledgment (<machine and number of events>).

Previous numbers:
Without a rate limit, it reaches about 100k events/s on a 4-core machine. When it
waited for every acknowledgment, it managed about 5k (issue #60).

**!!!**

The last line of a run looks like
`CSV processing completed: published=N skipped=N rejected=N topic="trade-events"`.
`make measure-flow` parses this line, so keep its format if you change it.

## Leftover containers

`docker compose run` doesn't pass Ctrl+C on to the container: the client exits and the
container keeps publishing. Our scripts name the container `data-producer-run-<pid>` and
stop it by name. If a shell gets killed before it can do that, find the leftover with
`docker ps --filter name=data-producer-run` and stop it.

## Tests

```
make test-data-producer
make format-data-producer
```
