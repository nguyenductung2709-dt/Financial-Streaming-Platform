# Monitoring

We use this to measure the pipeline for the report: CPU, memory, network and disk I/O
for every container, and how long each EMA window and advisory takes to come out. The
monitoring stack is kept in `compose.monitoring.yaml` so that the normal setup stays
light.

## Measuring a run

```
make measure-flow CSV=dataset/<file>.csv RATE=20000 MAX_EVENTS=2000000
```

This resets the pipeline, which deletes the topics and database results. It then starts
Prometheus and Grafana, replays the CSV, and waits until the last result has been
written. Results go to `bin/metrics/<timestamp>/`, or to `OUT` if you set it.
`summary.txt` is the file to read first. It has the resource numbers per component,
followed by latency percentiles per stage. Everything else is the raw data behind it:
`summary.csv`, `timeseries.csv`, `latency.csv`, `latency-summary.csv`, `pipeline.json`
and `run.json`.

To compare several rates:

```
make sweep-flow RATES="500 1000 2000" CSV=dataset/<file>.csv MAX_EVENTS=1000000
```

Be careful with `RATE=0`. Without a limit, the producer can publish faster than the
pipeline keeps up, and the latency numbers then measure the backlog instead of normal
operation. If `producer_s` in `pipeline.json` is much shorter than `total_s`, that's
what happened. Always set `MAX_EVENTS` as well: Redpanda's memory keeps growing with the
amount of data published.

Prometheus is on http://localhost:9090 and Grafana on http://localhost:3000, with a
"Pipeline resources" dashboard and no login. `make monitoring-up` and
`make monitoring-down` start and stop them on their own.
`make collect-metrics START=<time> END=<time>` exports any time window from the last 30
days. Times without a time zone are read as UTC.

## Where the numbers come from

Container stats come from a small exporter in `docker-stats-exporter/` that reads
Docker's stats API every 2 seconds. We don't use cAdvisor because it can't see
containers when Docker uses the containerd image store, which is the default since
Docker 29. Flink and Redpanda metrics come from their own Prometheus endpoints every
5 seconds.

CPU is reported in cores (1.0 is one full core), and its total in CPU-seconds. Memory is
the container's working set, the same number `docker stats` shows. Totals come from
Prometheus `increase()`, so they're accurate to about one scrape interval.

## Latency

During `measure-flow`, Flink and the materializer publish a timestamp for every window
at each step. `scripts/collect-latency.py` then lines these up with the time the broker
appended each message. Everything runs on one machine, so all the clocks agree.

| Moment | What happened |
| --- | --- |
| tw | The first event past the window's end was published |
| t0 | The event that moved the watermark past the window's end was published |
| t1 | Flink fired the window |
| t2 | Flink computed the EMA |
| t3 | Flink checked for a crossover |
| t4e, t4a | The EMA or advisory was appended to its topic |
| t5e, t5a | The materializer committed the EMA or advisory to PostgreSQL |

Each stage in `latency-summary.csv` is the gap between two of these moments. For
example, `ema_compute` is t2 minus t1, `end_to_end_ema` is t4e minus t0, and
`end_to_end_ema_materialized` is t5e minus t0. The stage names in
`scripts/collect-latency.py` follow this pattern. `ingest_to_fire` (t1 minus t0) also
includes up to 200 ms of Flink's watermark interval.

The `end_to_end_*` stages start at t0, so they leave out the 1 second of event time
the window waits for its watermark. How long that wait takes in real time depends on
the replay rate and on how busy the data is. The `window_end_to_*` stages start at tw
and include the wait, which is reported on its own as `watermark_wait`. Other DEBS
projects usually report something like `window_end_to_ema`. If you compare with them,
look at `watermark_wait` as well.

Timestamps have millisecond resolution, so very short stages mostly read 0 to 2 ms. The
windows flushed at the end of a bounded run have no triggering event and are left out.
If at-least-once delivery produced duplicate stamps, only the first one is used.
`collect-latency` reads the topics again, so you can rerun it on a finished run with
`make collect-latency OUT=bin/metrics/<run>` until the next reset.

## Limitations

This setup is meant for local use only. The exporter mounts the Docker socket, Grafana
lets anyone in as admin, and every port is bound to 127.0.0.1.

The producer container only exists while it's running, so its last second or two of
CPU and network use isn't sampled.

Runs from before issue #89 measured the producer and the materializer as host
processes, so they appear as `kind=host` and their memory is the resident set size.
Newer runs show the same components as `kind=docker`, with working-set memory, so the
two kinds of run aren't exactly comparable.
