# Data producer

Reads trade CSV files and publishes each row as a `PriceEvent` to the `trade-events`
topic in Redpanda. It runs as a container on the Compose network, next to Redpanda.

## Run

From the repository root, with Redpanda and the topics up (`make infra-up`):

```sh
make run-data-producer ARGS="-csvPath=dataset/<file>.csv -rate=1000 -dlqEnabled"
```

This builds the image and runs the producer in the foreground; Ctrl+C stops the
container. `make run-flow`, `make measure-flow` and `make validate-flow` run it as part
of the complete flow. Print the flags with `make run-data-producer ARGS="-help"`.

## CSV input

- `-csvPath` takes a file or a directory (its immediate `.csv` files, alphabetical) and
  can be repeated; the files are replayed in the order given.
- Any host path works. Each file is mounted read-only into the container at
  `/input/<NN>-<name>` and the flags are rewritten to match. A `:` in a path is
  rejected, because it breaks Docker's volume syntax.
- A missing path, an empty directory, or one file listed twice (for example an alias
  symlink next to its target) is rejected before anything runs.
- Without `-csvPath`, the producer reads `dataset/debs2022-gc-trading-day-12-11-21.csv`.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-csvPath` | see above | CSV file or directory; repeat for ordered inputs |
| `-brokers` | `redpanda-0:9092` in the container (`localhost:19092` for a bare binary) | Comma-separated broker addresses; a localhost address is rejected, because inside the container it is the container itself |
| `-topic` | `trade-events` | Topic for events |
| `-dlqTopic` | `trade-events-dlq` | Topic for rejected rows |
| `-dlqEnabled` | off | Publish invalid rows to the dead letter topic instead of stopping |
| `-rate` | `0` | Maximum events per second (0 is unlimited) |
| `-maxEvents` | `0` | Stop after this many acknowledged events (0 is unlimited) |
| `-linger` | `1ms` | How long the client waits for more records before it sends a batch |

The run ends with `CSV processing <status>: published=N skipped=N rejected=N`, which
`make measure-flow` reads.

## Publishing

The producer hands each record to the franz-go client and reads the next row without
waiting for the acknowledgment. The client batches the records and sends them.

- `published` and `rejected` count only records Redpanda acknowledged. At the end, the
  producer waits for every record it sent before it prints the summary line.
- The first failed record stops the replay: no further rows are read, the records
  already sent still finish, and the run ends with status `stopped` and the error.
- `-maxEvents=N` sends N events and waits for them. The status is `limit reached` only
  when all N were acknowledged.
- At most 10,000 records wait for an acknowledgment. Beyond that the producer blocks
  until some finish, so a slow broker slows the replay down instead of growing memory.
- Idempotent writes stay on, so each partition keeps the CSV order even with several
  requests in flight. A record that is not acknowledged within 30 s fails the run.

With `-rate=0` the producer reached about 100k events/s with any linger, against about
5k events/s when it waited for each acknowledgment (issue #60; 5M events, producer and
Redpanda in containers on one 4-core machine). Linger 0 was the slowest; 1 ms and 5 ms
were within noise of each other, and 1 ms holds a lone record back the least.

## Notes

`docker compose run` does not forward Ctrl+C or SIGTERM to its container: the client
exits and the container keeps publishing. The scripts therefore name the container
(`data-producer-run-<pid>`) and stop it by name on every exit path. If a shell is
killed outright, stop a leftover with `docker ps --filter name=data-producer-run`
and `docker stop <name>`.
