# Data producer

The producer reads trade CSV files and publishes every row to `trade-events` as a
`market.v1.PriceEvent`, keyed by symbol. It runs as a container next to Redpanda.

`make run-flow` starts it for you. To run it on its own, bring up Redpanda first:

```
make infra-up
make run-data-producer ARGS="-csvPath=dataset/<file>.csv -rate=1000 -dlqEnabled"
```

`ARGS="-help"` prints every flag. These are the ones you'll usually want:

- `-csvPath` is a CSV file, or a directory whose `.csv` files are read in alphabetical
  order. Repeat it to replay several inputs one after another. If you leave it out, the
  producer reads `dataset/debs2022-gc-trading-day-12-11-21.csv`.
- `-rate` caps events per second. The default, 0, means no cap.
- `-maxEvents` stops after that many acknowledged events.
- `-dlqEnabled` sends invalid rows to `trade-events-dlq` instead of stopping the run.

Inputs are mounted into the container read-only, so any host path works, except a path
that contains a `:`.

## How it publishes

The producer doesn't wait for each acknowledgment before reading the next row. Up to
10,000 records can be in flight, and after that it blocks, so a slow broker slows the
replay down instead of filling up memory. Idempotent writes keep each partition in CSV
order. The first record that fails stops the replay.

Without a rate limit, it reaches about 100k events/s on a 4-core machine. When it
waited for every acknowledgment, it managed about 5k (issue #60).

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
