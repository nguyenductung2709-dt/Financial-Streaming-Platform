# Resource monitoring

Collects CPU, memory, network I/O and block I/O for the whole pipeline, for the
report's performance evaluation (issue #49). It lives in `compose.monitoring.yaml`,
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
| `summary.txt` | Table per component: CPU and memory (average, maximum), bytes moved |
| `summary.csv` | The same per component and metric: `avg`, `max`, `total` |
| `timeseries.csv` | Every sample: `offset_s,component,kind,metric,value` |
| `pipeline.json` | Event counts, durations, producer and end-to-end events per second |
| `run.json` | Window, sampling settings, labels, producer arguments |

Other targets: `make monitoring-up` / `make monitoring-down` (Prometheus on
`:9090`, Grafana on `:3000` with the "Pipeline resources" dashboard, no login), and
`make collect-metrics START=<epoch|ISO> [END=...]` to export any past window; the
data stays in Prometheus for 30 days. `START` and `END` without a UTC offset are
read as UTC. `measure-flow` prints its start as epoch seconds. Change the URL with
`PROMETHEUS_URL=http://localhost:<port>` when `PROMETHEUS_PORT` is overridden.

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

- Latency is not measured here (issue #48); this covers throughput and resources.
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
