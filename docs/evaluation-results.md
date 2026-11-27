# Performance evaluation results

This document reports five experiments on the full DEBS 2022 trading week. They check that the results are correct, and measure throughput, latency, resource use and scaling.

All runs were made on 24 November 2026 with a 1 second out-of-orderness allowance. An earlier set of runs with a 5 second allowance is compared in [5 s vs. 1 s allowance](#5-s-vs-1-s-allowance).

**Contents:** [Summary](#summary) · [Setup](#setup) · [E1 Correctness](#e1-correctness) · [E2 Maximum rate](#e2-maximum-rate) · [E3 Latency vs. rate](#e3-latency-vs-rate) · [E4 Flink alone](#e4-flink-alone) · [E5 Whole pipeline on the cluster](#e5-whole-pipeline-on-the-cluster) · [Comparisons](#comparisons) · [Caveats](#caveats)

## Summary

| | Result |
|---|---|
| Correctness | All checks passed on the full week, 0 unexplained windows |
| Throughput, one broker | 203,000 events/s end to end, limited by the producer |
| Throughput, Flink alone | 2.5 M events/s with 1 task, 3.9 M with 2 |
| End-to-end latency, one broker | Median 155–223 ms, P99 under 360 ms, at every rate from 5k to 203k events/s |
| Scaling, whole pipeline | No gain from more partitions, because the producer is the limit |
| CPU use at maximum rate | About 1.7 of 10 cores |

## Setup

### Environment

| Item | Value |
|---|---|
| Machine | Apple M4 (4 performance + 6 efficiency cores), 24 GB RAM. Docker limited to 10 CPUs and 14.6 GB |
| Software | Redpanda 26.2.3, Flink 2.2.1 (one TaskManager), PostgreSQL, Go producer and materializer |
| Single-broker mode | One broker, 1 partition per topic, Flink parallelism 1 |
| Cluster mode (`CLUSTER=1`) | 3 brokers (1 core and 2 GB each), replication factor 3, N partitions per topic, Flink parallelism N |
| Dataset | DEBS 2022, 8–14 Nov 2021: 7 files, 25 GB, 289 M rows, of which 59,391,670 are trades with a price (4,970 symbols) |
| Watermark | `forBoundedOutOfOrderness(1 s)` |
| Resource metrics | Prometheus scrapes Docker stats every 2 s, and Flink and Redpanda metrics every 5 s |

### Experiments

| | Question | Mode | Runs |
|---|---|---|---|
| E1 | Are the results correct? | Single broker, bounded job | `make validate-flow CSV=dataset RATE=0` |
| E2 | Throughput, latency and resources at the maximum rate | Single broker | `make measure-flow CSV=dataset RATE=0` |
| E3 | How does latency change with the input rate? | Single broker | Full week at 50k, 100k and 150k events/s, and 3 M trades of day 10 at 5k–150k events/s |
| E4 | How fast is Flink on its own? | Cluster, N = 1, 2, 4 | Data preloaded, bounded Flink job, 3 runs per N |
| E5 | Does the whole pipeline scale? | Cluster, N = 1, 2, 4 | Full week end to end, 3 runs per N, each on a freshly reset cluster, then `make compare-flow LIVE=1` |

### Metrics

**Throughput** is trades per second. *Producer throughput* divides the trade count by the producer's run time. *End-to-end throughput* divides it by the time until the last result is in PostgreSQL.

**Latency** is measured for every window. All components run on one machine and share its clock. A window can't fire as soon as a trade past its end arrives, because a trade up to 1 s late could still come. It fires once the **closing trade** arrives: the first trade at least 1 s past the window's end.

```
first trade past    closing trade    Flink fires    EMA         EMA on        EMA committed
the window's end    published        the window     computed    ema-windows   in PostgreSQL
       |----------------|--------------|-------------|-------------|---------------|
         watermark wait       fire         compute       publish         store
                        |<------------------- end-to-end latency ------------------>|
       |<---------------------------- wait included ------------------------------->|
```

- **End-to-end latency** is the main measure. Wherever this document says "latency" without a qualifier, it means this.
- **Watermark wait** is reported separately, because it depends on the replay speed: it is the wall-clock time the replay takes to move 1 s forward in trade time.
- P50, P95 and P99 are the 50th, 95th and 99th percentiles over all windows of a run.

## E1: Correctness

E1 runs the three checks described in [validation/README.md](../validation/README.md).

| Check | Result |
|---|---|
| 1. Flink's output vs. a Python replay of the published trades | 1,857,457 windows, **0 unexplained** |
| &nbsp;&nbsp;&nbsp;identical without any adjustment | 1,170,984 |
| &nbsp;&nbsp;&nbsp;differ only because of late events Flink dropped | 686,473 |
| 2. Python replay vs. a reference computed directly from the CSV | 1,855,645 of 1,855,645 windows match |
| 3. PostgreSQL vs. the topics | 1,855,645 windows, 11,349 advisories and 4,970 symbols match |
| **Result** | **passed** |

- **Dropped events.** Flink dropped 5,506 late events, all of them index updates (SecType I) from 10 November with no trading date and a placeholder time of `01:00:00.000`, sent in the evening. No other event of the week is dropped. Real trades are at most 264 ms out of order.
- **Why check 1 has 1,812 more windows.** Check 1 also counts windows that exist only in a replay that keeps every event. The dropped index updates fall into 1,812 windows that contain nothing else, so Flink correctly never creates them. They are counted as differing because of dropped late events, not as errors.
- Every number is the same as with the 5 s allowance.

## E2: Maximum rate

Full week, single broker, no rate limit.

### Throughput

| Metric | Value |
|---|---|
| Trades published | 59,391,670 (229.7 M non-trade rows skipped) |
| Run time, producer / whole pipeline | 290.5 s / 292.6 s |
| Producer throughput | 204,446 events/s (5.4 MB/s to Redpanda, about 86 MB/s of CSV read) |
| **End-to-end throughput** | **202,969 events/s** |
| Results | 1,855,638 EMA windows, 11,349 advisories |
| Missing timing data / duplicates / missing from PostgreSQL | 0 / 0 / 0 |

### Latency

Each stage of one window, in order (all 1,855,638 windows):

| Stage | From → To | P50 (ms) | P95 | P99 | Max |
|---|---|---|---|---|---|
| Watermark wait | First trade past the window's end → closing trade published | 6 | 11 | 17 | 20 |
| Fire | Closing trade published → Flink fires the window | 105 | 198 | 205 | 303 |
| Compute | Flink fires the window → EMA computed | 3 | 32 | 86 | 100 |
| Publish | EMA computed → EMA appended to `ema-windows` | 0 | 3 | 6 | 15 |
| Store | EMA appended to `ema-windows` → EMA committed in PostgreSQL | 54 | 124 | 230 | 390 |

Totals, measured per window (percentiles don't add up, so these aren't sums of the rows above):

| Total | P50 (ms) | P95 | P99 | Max |
|---|---|---|---|---|
| **End-to-end latency** (fire to store) | **172** | **285** | **350** | 461 |
| Wait included (all five stages) | 178 | 291 | 354 | 463 |
| Advisory end to end (the 11,349 windows with a crossover) | 181 | 461 | 484 | 489 |

- **Fire is the slowest stage.** Flink advances the watermark only every 200 ms, so a window waits 0–200 ms after its closing trade arrives. That matches the 105 ms median and 205 ms P99.
- **Store is second.** The materializer writes to PostgreSQL in batches.

### Resources

| Container | CPU avg (cores) | CPU max (cores) | Memory max (MiB) |
|---|---|---|---|
| Producer | 1.14 | 1.18 | 73 |
| Flink TaskManager | 0.28 | 0.84 | 1,232 |
| Redpanda | 0.10 | 0.12 | 2,077 |
| PostgreSQL | 0.13 | 0.20 | 242 |
| Materializer | 0.07 | 0.12 | 17 |

The pipeline uses about 1.7 of the 10 available cores. The producer's CSV loop is single-threaded, so it runs at a little over one core, and that is what limits throughput.

## E3: Latency vs. rate

Single broker, fixed producer rates. No run had missing timing data or duplicates.

### Full week

| Target rate | Achieved (events/s) | Run time (s) | Latency P50 (ms) | P95 | P99 | Max | Watermark wait P50 (ms) | CPU cores: producer / Flink / Redpanda |
|---|---|---|---|---|---|---|---|---|
| 50k | 49,766 | 1,193 | 170 | 277 | 310 | 427 | 51 | 0.49 / 0.15 / 0.08 |
| 100k | 99,231 | 599 | 165 | 261 | 289 | 332 | 25 | 0.64 / 0.16 / 0.07 |
| 150k | 148,004 | 401 | 158 | 263 | 293 | 337 | 15 | 0.87 / 0.21 / 0.08 |
| no limit (E2) | 202,969 | 293 | 172 | 285 | 350 | 461 | 6 | 1.14 / 0.28 / 0.10 |

### Short runs (first 3 M trades of day 10)

| Target rate | Achieved (events/s) | Latency P50 (ms) | P95 | P99 | Watermark wait P50 (ms) | Wait included P50 (ms) |
|---|---|---|---|---|---|---|
| 5k | 4,978 | 212 | 304 | 336 | 513 | 708 |
| 10k | 9,856 | 223 | 307 | 330 | 257 | 464 |
| 25k | 23,690 | 193 | 309 | 338 | 103 | 279 |
| 50k | 47,560 | 162 | 289 | 312 | 51 | 204 |
| 100k | 84,120 | 155 | 253 | 278 | 25 | 176 |
| 150k | 118,794 | 162 | 273 | 318 | 11 | 173 |

- **Latency barely depends on the rate.** The median stays between 155 and 223 ms across a 30× range.
- **No backlog builds up.** In every full-week run, the per-minute median stays between 130 and 197 ms from start to finish.
- **The watermark wait falls as the rate rises,** roughly in proportion, because a faster replay covers 1 s of trade time in less wall-clock time.
- **The short runs at 100k and 150k fall short of their targets.** They last only 20–35 s, while the full-week runs at the same rates reach theirs, so the shortfall is probably the producer ramping up.

## E4: Flink alone

Cluster mode. The full week is loaded into the topics first, so Flink reads as fast as it can and no producer runs. Throughput is the 59.4 M trades divided by the median job duration.

| N | Job duration, 3 runs (s) | Throughput | Speed-up vs. N = 1 | TaskManager CPU avg / max (cores) |
|---|---|---|---|---|
| 1 | 24.1 / 22.1 / 23.7 | 2.51 M events/s | 1.00× | 2.0 / 3.7 |
| 2 | 15.3 / 15.8 / 13.7 | 3.89 M events/s | 1.55× | 2.9 / 5.6 |
| 4 | 16.9 / 17.1 / 16.9 | 3.52 M events/s | 1.40× | 4.4 / 7.3 |

- **Flink is far from the limit.** With one task it is about 12 times faster than the producer's maximum rate.
- **Two tasks help, four don't.** We haven't verified why. A likely cause is the cost of passing data between parallel tasks on a machine with only 4 performance cores.
- **The brokers are never the limit.** Each uses at most about 0.24 cores.

## E5: Whole pipeline on the cluster

Cluster mode, full week end to end, three runs per N. Throughput and latency are the median of the three runs: each run has its own P50, P95 and P99, and the table shows the median of each. Correctness is checked once per N, after the last run.

### Throughput and latency

| N | Throughput, median (min–max) | Latency P50 / P95 / P99 (ms) | Missing / duplicate |
|---|---|---|---|
| 1 | 195k (195k–197k) | 181 / 320 / 437 | 0 / 0 |
| 2 | 187k (185k–191k) | 214 / 354 / 472 | 0 / 0 |
| 4 | 178k (177k–184k) | 233 / 453 / 750 | 0 / 0 |

### Correctness

| N | Result | Reference windows matched | Windows / advisories in PostgreSQL, all matching | Still open at the end |
|---|---|---|---|---|
| 1 | passed, 0 unexplained | 1,855,645 / 1,855,645 | 1,855,638 / 11,349 | 7 |
| 2 | passed, 0 unexplained | 1,855,645 / 1,855,645 | 1,855,638 / 11,349 | 7 |
| 4 | passed, 0 unexplained | 1,855,645 / 1,855,645 | 1,855,548 / 11,349 | 97 |

The job here isn't bounded, so the last windows of the week have no later trade to close them. They never reach PostgreSQL, which is why the PostgreSQL count is the reference count minus the open windows. With more partitions the watermark follows the slowest one, so more windows stay open. This is expected, not an error.

### Resources

| N | Producer | Brokers 0 / 1 / 2 | TaskManager | Materializer | PostgreSQL | Broker memory max (MiB) |
|---|---|---|---|---|---|---|
| 1 | 1.18 | 0.10 / 0.09 / 0.09 | 0.30 | 0.08 | 0.13 | 1,943 |
| 2 | 1.20 | 0.14 / 0.14 / 0.11 | 0.36 | 0.07 | 0.13 | 1,947 |
| 4 | 1.24 | 0.18 / 0.19 / 0.17 | 0.48 | 0.07 | 0.13 | 1,904 |

CPU is the average in cores.

### Flink load

Share of time each Flink task was busy, from Flink's own metrics, averaged over runs and parallel subtasks:

| Task | N = 1 | N = 2 | N = 4 |
|---|---|---|---|
| Source (reads `trade-events`) | 7% | 5% | 4% |
| Window (keeps each window's last trade) | 8% | 4% | 2% |
| EMA, and writing to `ema-windows` | 3% | 7% | 7% |
| Crossover, and writing to `trade-advisories` | 7% | 7% | 6% |
| Writing latency timestamps | 2% | 6% | 6% |

### Findings

- **Throughput doesn't rise with N, because the producer is the limit.** No Flink task was busy more than 10% of the time, and backpressure never went above 0.03%.
- **Each extra partition costs a little.** Throughput is 4% lower at N = 2 and 9% lower at N = 4, and latency rises. Brokers and the TaskManager use more CPU as N grows, which suggests the cost comes from handling more, smaller batches. This hasn't been verified.
- **The runs were stable.** All nine started on a freshly reset cluster, and none failed or had to be rerun.

## Comparisons

### Cost of replication

E2 (one broker) against E5 at N = 1 (three brokers):

| Setup | Throughput | Latency P50 / P95 / P99 (ms) |
|---|---|---|
| 1 broker | 203k events/s | 172 / 285 / 350 |
| 3 brokers, replication factor 3 | 195k events/s | 181 / 320 / 437 |

Keeping three copies of every message costs about 4% throughput and 9 ms of median latency. The P99 grows more, by 87 ms.

### 5 s vs. 1 s allowance

The same experiments were first run with a 5 s allowance on 21 November, on the code from before the 3-broker cluster change (#118). The 1 s runs use current `main` with only the allowance changed.

| | 5 s | 1 s |
|---|---|---|
| E1 correctness | passed, 0 unexplained, 5,506 events dropped | identical |
| E2 throughput | 191,437 events/s | 202,969 events/s |
| E2 latency P50 / P95 / P99 (ms) | 181 / 301 / 367 | 172 / 285 / 350 |
| E2 watermark wait P50 | 14 ms | 6 ms |
| E2 wait included P50 | 194 ms | 178 ms |

E3 short runs, median in ms:

| Rate | Watermark wait, 5 s → 1 s | Wait included, 5 s → 1 s |
|---|---|---|
| 5k | 701 → 513 | 871 → 708 |
| 10k | 351 → 257 | 539 → 464 |
| 25k | 140 → 103 | 287 → 279 |
| 50k | 68 → 51 | 227 → 204 |
| 100k | 35 → 25 | 201 → 176 |
| 150k | 23 → 11 | 187 → 173 |

- **Correctness doesn't change.** No real trade is more than 264 ms late, so both allowances drop exactly the same events.
- **End-to-end latency doesn't change.** It starts at the closing trade, after the watermark wait. The small differences in latency and throughput are within what we'd expect between single runs on different code versions.
- **The watermark wait gets shorter, but by less than expected.** A five times smaller allowance cuts the wait by a quarter to a half, because most of the wait falls in the first second after a window ends. It only matters at low rates: from 50k events/s up, the wait is under 70 ms with either allowance.

## Caveats

- Everything runs on one laptop, so the producer, the brokers and Flink compete for the same CPUs.
- E2 and E3 are a single run per setting. E4 and E5 have three runs per N.
- The replays run much faster than real time. With a live feed, the watermark wait would be about 1 s of real time. End-to-end latency doesn't include it.
- E4 is the best case for Flink: the data is already in the topics, and no producer competes for CPU.
- The first full-week E3 attempt failed at 50k, although the pipeline completed and wrote all 1,855,638 windows. The latency script rejected one window because the broker's timestamp for its EMA was earlier than Flink's timestamp for producing it. This is a consistency check in `scripts/collect-latency.py`, not a result check. The cause wasn't confirmed before the topics were deleted. The rerun passed at all three rates.
