# Resource monitoring

Collects CPU, memory, network I/O and block I/O for the whole pipeline (issue #49)
and the latency of every EMA window and trade advisory (issue #48), for the report's
performance evaluation. The resource monitoring lives in `compose.monitoring.yaml`,
so the default `compose.yaml` stays light.

## Measure a run

```sh
make measure-flow CSV=dataset/<file>.csv RATE=0 MAX_EVENTS=2000000
```

Resets the flow, starts the infrastructure and the monitoring stack, runs the
materializer and the producer as containers, finds the start and end of the run by itself, and
writes the results to `bin/metrics/<timestamp>/` (`OUT=dir` to choose). `RATE` is
the producer's events per second (`0` is unlimited). The reset permanently
clears the project topics and database results. Use `MAX_EVENTS` to bound a run;
Redpanda's memory grows with the amount of data published (about 40 MiB per minute
in the 2M-event run, with no plateau observed).

| File | Content |
| --- | --- |
| `summary.txt` | Table per component: CPU and memory (average, maximum), bytes moved; then the latency table |
| `summary.csv` | The same per component and metric: `avg`, `max`, `total` |
| `timeseries.csv` | Every sample: `offset_s,component,kind,metric,value` |
| `pipeline.json` | Event counts, durations, producer and end-to-end events per second, and the `latency` coverage counts |
| `latency.csv` | One row per EMA window: the wall-clock moments and the latency stages ([Latency](#latency)) |
| `latency-summary.csv` | Per stage: `count`, `mean_ms`, `p50_ms`, `p95_ms`, `p99_ms`, `max_ms` |
| `run.json` | Window, sampling settings, labels, producer arguments |

Other targets: `make monitoring-up` / `make monitoring-down` (Prometheus on
`:9090`, Grafana on `:3000` with the "Pipeline resources" dashboard, no login), and
`make collect-metrics START=<epoch|ISO> [END=...]` to export any past window; the
data stays in Prometheus for 30 days. `START` and `END` without a UTC offset are
read as UTC. `measure-flow` prints its start as epoch seconds. Change the URL with
`PROMETHEUS_URL=http://localhost:<port>` when `PROMETHEUS_PORT` is overridden.

## Latency

`measure-flow` also reports how long the pipeline takes to produce each EMA window and
trade advisory. It starts the Flink job with `--latency-topic latency-stamps`, so three
operators stamp the wall-clock time they finish a window, and the materializer
(`MATERIALIZER_STAMP_TOPIC=materialize-stamps`) stamps each result it commits; afterwards
`scripts/collect-latency.py` joins those stamps with the broker's append time on the
output topics and with the moment the producer published the event that let the window
close. Producer, Flink and Redpanda are containers on one host, so they share one clock.

| Moment | What happened | Where it comes from |
| --- | --- | --- |
| t0 | The trigger event was published: the first event in `trade-events` whose `trade_time` reaches `window_end` plus the 70 s out-of-orderness allowance | `trade-events` record timestamp, rebuilt by the collector with the watermark rule |
| t1 | Flink fired the window | stamp in `latency-stamps` |
| t2 | Flink computed the EMA | stamp in `latency-stamps` |
| t3 | Flink evaluated the crossover, for every window (advisories themselves are rare) | stamp in `latency-stamps` |
| t4e / t4a | The EMA / advisory landed on `ema-windows` / `trade-advisories` | the topics' broker append time (`LogAppendTime`) |
| t5e / t5a | The materializer committed the EMA / advisory to PostgreSQL | stamp in `materialize-stamps` |

| Stage | Definition |
| --- | --- |
| `ingest_to_fire` | t1 - t0: Kafka and Flink queueing, plus up to 200 ms of Flink's periodic watermark interval. Rises when the pipeline falls behind; below that it is dominated by the watermark interval |
| `ema_compute` | t2 - t1 |
| `advisory_compute` | t3 - t2 |
| `ema_publish` | t4e - t2: sink serialization, producer batching, broker append |
| `advisory_publish` | t4a - t3, for windows that emitted an advisory |
| `end_to_end_ema` | t4e - t0 |
| `end_to_end_advisory` | t4a - t0, for windows that emitted an advisory |
| `ema_materialize` / `advisory_materialize` | t5e - t4e / t5a - t4a: materializer consumption and PostgreSQL commit |
| `end_to_end_ema_materialized` / `end_to_end_advisory_materialized` | t5e - t0 / t5a - t0: the result visible in PostgreSQL |

Without materialize stamps (as under `run-flow`) the `*_materialized` stages stay empty.

The wait for the watermark (a window fires 70 s of event time after it ends) is set by the
data and is not counted. Gaps between stamps include queueing behind earlier records:
they measure latency, not service time.

```sh
make collect-latency OUT=bin/metrics/<run>     # a run that already finished (topics intact)
make sweep-flow RATES="500 1000 2000" CSV=dataset/<file>.csv MAX_EVENTS=1000000
```

`collect-latency` reads the topics, so it works until the next `reset-flow`. `sweep-flow`
runs `measure-flow` once per rate into `<out>/rate-<rate>/` (each run resets the flow first)
and writes `sweep-summary.csv` with the percentiles and throughput of every rate and stage;
it stops at the first failed run and keeps the earlier ones.

Things to know when reading the numbers:

- Stamps have 1 ms resolution, so sub-millisecond stages such as `ema_compute` mostly
  read 0 to 2 ms.
- The producer publishes synchronously and tops out near 4.7k events/s; the pipeline kept
  up with that (it settled within 2 to 4 s of the producer finishing), so even `RATE=0`
  measured steady-state latency. In a 1,000 vs 20,000 events/s sweep `ingest_to_fire` was
  lower at the higher rate, while the tails of `ema_compute`, `advisory_compute` and the
  end-to-end stages were higher. A faster producer (issue #60) that outruns the pipeline
  would make the figures measure backlog instead.
- A bounded run flushes its last windows without a trigger event. They are counted as
  `flushed` in the coverage line and left out of the statistics.
- `ema-windows` and `trade-advisories` must be created with `message.timestamp.type=LogAppendTime`
  (`create-topic` and `reset-flow` do this). The collector refuses topics that were not and tells
  you to run `make reset-flow`.
- The collector reads `trade-events` and refuses topics with more than one partition,
  because the trigger rule assumes one partition and parallelism 1 (issue #38).
- Duplicates from at-least-once delivery keep the first occurrence and are counted.

## Where the numbers come from

| Source | Covers | Interval |
| --- | --- | --- |
| `docker-stats-exporter` | Every container of this Compose project, including the producer and materializer | 2 s |
| Flink's Prometheus reporter | JobManager and TaskManager internals | 5 s |
| Redpanda `/public_metrics` | Broker metrics, topic and consumer-group offsets | 5 s |

`docker-stats-exporter` reads Docker's stats API instead of using cAdvisor, which
cannot see containers when Docker uses the containerd image store (the default
since Docker 29). A container that exits between the listing and its stats call, or
whose first sample is still empty, is skipped for that scrape instead of failing it.

Columns in `summary.csv` (the monitoring containers are left out; `--exclude ''` on
`scripts/collect-resource-metrics.py` keeps them):

| Metric | `avg` / `max` | `total` |
| --- | --- | --- |
| `cpu_cores` | cores in use (1 = one full core) | CPU-seconds |
| `memory_bytes` | bytes: container working set (usage minus inactive page cache, as `docker stats` shows) | not set |
| `network_*_bytes_per_s`, `block_*_bytes_per_s` | bytes per second | bytes over the window |

Totals use Prometheus `increase()`, so they are accurate to about one scrape interval.

## Comparing with runs from before the containers

Until issue #89 the producer and materializer were host processes, measured by a
`process-exporter` that no longer exists. Exports from those runs have `kind=host` rows
for `data-producer` and `materializer`; new runs have the same component names with
`kind=docker`, so rows line up by component and metric. Two things differ: memory was
the process's resident set and is now the container's working set (close for a Go
program, but not the same number), and the producer's network I/O is measured now, so
the old exports have nothing to compare it with. Re-exporting an old window with
`make collect-metrics` no longer produces the host rows.

## How a run is timed

The start is taken just before the producer container launches, so it includes the
container's startup. Every 2 s the script reads
Flink's source count, the end offsets of `ema-windows` and `trade-advisories`, and
the materializer's committed offsets. The run is finished when the producer has exited, the
materializer has caught up and nothing has changed for 20 s (`--idle-seconds`); the
end is the last moment anything changed. Detection lags by up to about 7 s (5 s scrape
plus 2 s poll), which matters for short runs. The end is when the last result was
written and committed, not when every input window closed.

## Limits

- The producer is a one-off container that exists only while it runs, and the exporter
  scrapes every 2 s, so its last second or two of CPU and network is not seen. Totals
  are accurate to about one scrape interval.
- `docker compose run` does not forward Ctrl+C or SIGTERM to the container it starts
  (the client exits and the container keeps running), so the flow scripts stop the
  producer container by name on every exit path.
- Untested on Docker Desktop (macOS, Windows), where the bind-mounted CSV may be read
  more slowly.
- The exporter mounts the Docker socket and Grafana allows anonymous admin access.
  Every port is bound to `127.0.0.1`; this is for local development only.
- Scrape targets are static. A multi-broker or multi-TaskManager Compose (issue #38)
  needs its targets added to `prometheus.yml`.
