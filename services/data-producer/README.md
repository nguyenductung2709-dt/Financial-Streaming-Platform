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
| `-speed` | `0` | Replay the gaps between trade times, sped up by this factor (0 is off); cannot be combined with `-rate` |
| `-maxEvents` | `0` | Stop after this many acknowledged events (0 is unlimited) |

The run ends with `CSV processing <status>: published=N skipped=N rejected=N`, which
`make measure-flow` reads. Under `-speed` the line ends with `replay_lag_max_ms=N`.

## Replay speed

`-rate` spaces events evenly. `-speed=<x>` keeps the shape of the trading day instead:
an event is sent when the wall clock has moved `(trade time - first trade time) / x`
since the first event, so the bursts and quiet stretches of the data stay in the load.
`-speed=1` is real time, `-speed=100` turns 5 minutes of trading into 3 seconds.

- The schedule follows the highest trade time seen so far. An out-of-order event is sent
  at once, and the schedule never runs backwards.
- The schedule is anchored to the first event, so time spent publishing does not add
  up. A producer that falls behind sends back to back until it has caught up.
  `replay_lag_max_ms` is the furthest it fell behind: when it reaches seconds, the
  producer could not keep up with the speed (it tops out near 4.7k events/s) and the
  load was closer to `-rate=0` than to the replay.
- Skipped and rejected rows do not wait.
- Several CSV files replay the gap between them too, so the night between two trading
  days becomes a pause of its length divided by the speed.

## Notes

`docker compose run` does not forward Ctrl+C or SIGTERM to its container: the client
exits and the container keeps publishing. The scripts therefore name the container
(`data-producer-run-<pid>`) and stop it by name on every exit path. If a shell is
killed outright, stop a leftover with `docker ps --filter name=data-producer-run`
and `docker stop <name>`.
