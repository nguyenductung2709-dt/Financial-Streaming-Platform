# Performance evaluation results

These runs measure whether the pipeline produces correct results for the full DEBS 2022 week, how fast it goes, how long a result takes to reach PostgreSQL, and how it scales on a 3-broker cluster.

All runs were made on 24 November 2026 with a 1 second out-of-orderness allowance. An earlier set of runs with a 5 second allowance was made on 21 November, and [Change from 5 s to 1 s](#change-from-5-s-to-1-s) compares the two.

## Summary

- **Correct.** Every check passed on the full week: 0 unexplained windows, and PostgreSQL matches an independent reference computed from the CSV.
- **Throughput.** About 203,000 events/s end to end on one broker. The producer, which reads the CSV on a single thread, is the limit. Flink alone can process 2.5–3.9 M events/s.
- **Latency.** The median time from a window's closing trade to its EMA row in PostgreSQL is 155–223 ms at every rate tested (5k to 203k events/s). The 99th percentile stays under 360 ms on one broker.
- **Scaling.** More partitions and Flink tasks don't raise end-to-end throughput, because the producer is the limit. Each step adds a little latency.

## Setup

| Item | Value |
|---|---|
| Machine | Apple M4 (4 performance + 6 efficiency cores), 24 GB RAM. Docker limited to 10 CPUs and 14.6 GB |
| Software | Redpanda 26.2.3, Flink 2.2.1 (one TaskManager), PostgreSQL, Go producer and materializer |
| Single-broker mode | `compose.yaml`: one broker, 1 partition per topic, Flink parallelism 1 |
| Cluster mode (`CLUSTER=1`) | 3 brokers (1 core and 2 GB each), replication factor 3, N partitions per topic, Flink parallelism N |
| Dataset | DEBS 2022, 8–14 Nov 2021: 7 files, 25 GB, 289 M rows. 59,391,670 of the rows are trades with a price, across 4,970 symbols |
| Watermark | `forBoundedOutOfOrderness(1 s)` |
| Resource metrics | Prometheus scrapes Docker stats every 2 s and Flink and Redpanda metrics every 5 s |

| Experiment | Question | How it was run |
|---|---|---|
| E1 | Are the full-week results correct? | `make validate-flow CSV=dataset RATE=0` (bounded job, single broker) |
| E2 | Throughput, latency and resource use at the maximum rate | `make measure-flow CSV=dataset RATE=0` (single broker) |
| E3 | How does latency change with the input rate? | `make measure-flow CSV=dataset RATE=…` at 50k, 100k and 150k events/s over the full week, plus short runs of 3 M trades from day 10 at 5k–150k events/s (single broker) |
| E4 | How fast is Flink alone, and does it scale? | The full week is loaded into the cluster first, then the bounded Flink job runs with N = 1, 2 and 4, three times each |
| E5 | Does the whole pipeline scale? | The full week runs end to end on the cluster with N = 1, 2 and 4, three times each on a freshly reset cluster, followed by `make compare-flow LIVE=1` for each N |

### How throughput and latency are measured

**Throughput** is trades per second. Producer throughput divides the trade count by the producer's run time. End-to-end throughput divides it by the time until the last result is in PostgreSQL.

**Latency** is measured per window. All components run on one machine and share its clock. A window closes in three steps:

1. A trade at or past the window's end is published. Flink can't close the window yet, because a trade up to 1 s late could still arrive.
2. The **closing trade** is published: the first trade at least 1 s past the window's end. It moves the watermark past the window, so Flink fires it.
3. Flink computes the EMA and writes it to `ema-windows`, and the materializer commits it to PostgreSQL.

| Name in this document | From | To |
|---|---|---|
| **End-to-end latency** | Closing trade published (step 2) | EMA committed in PostgreSQL (step 3) |
| Watermark wait | First trade past the window's end (step 1) | Closing trade published (step 2) |
| Wait included | First trade past the window's end (step 1) | EMA committed in PostgreSQL (step 3) |

Wherever this document says latency without a qualifier, it means end-to-end latency. The watermark wait depends on the replay speed: it is the wall-clock time the replay takes to move 1 s forward in trade time. That's why it's reported separately. P50, P95 and P99 are the 50th, 95th and 99th percentiles over all windows in a run.

## E1: Correctness (full week)

E1 runs three checks, described in [validation/README.md](../validation/README.md):

| Check | Result |
|---|---|
| 1. Flink's output against a Python replay of the published trades | 1,857,457 windows compared, **0 unexplained** |
| &nbsp;&nbsp;&nbsp;of which identical without any adjustment | 1,170,984 |
| &nbsp;&nbsp;&nbsp;of which differ only because of the late events Flink dropped | 686,473 |
| 2. Python replay against a reference computed directly from the CSV | 1,855,645 of 1,855,645 windows match |
| 3. PostgreSQL against the topics | 1,855,645 windows, 11,349 advisories and 4,970 symbols all match |
| Result | **passed** |

Flink dropped 5,506 late events. All of them are index updates (SecType I) from 10 November that have no trading date and a placeholder time of `01:00:00.000`, and were sent in the evening. No other event of the week is dropped. Real trades are at most 264 ms out of order, well within the 1 s allowance.

Check 1 compares 1,812 more windows than the other two (1,857,457 against 1,855,645). Check 1 counts every window that appears in Flink's output or in a replay that keeps every event. The dropped index updates land in 1,812 windows of their own, at the placeholder time, which contain no other event. Flink drops those events, so it correctly never creates these windows. Each one is counted among the windows that differ only because of dropped late events, not as an error.

All of these numbers are the same as with the 5 s allowance.

## E2: Full week at maximum rate (single broker)

| Metric | Value |
|---|---|
| Trades published | 59,391,670 (229.7 M non-trade rows skipped) |
| Run time, producer / whole pipeline | 290.5 s / 292.6 s |
| Producer throughput | 204,446 events/s (5.4 MB/s sent to Redpanda, about 86 MB/s of CSV read) |
| End-to-end throughput | 202,969 events/s |
| Results | 1,855,638 EMA windows, 11,349 advisories |
| Windows with missing timing data / duplicate results / missing from PostgreSQL | 0 / 0 / 0 |

Latency of each stage, over all 1,855,638 windows. Every row starts and ends at a different point, so the rows overlap and don't add up.

| From | To | P50 (ms) | P95 | P99 | Max |
|---|---|---|---|---|---|
| Closing trade published | Flink fires the window | 105 | 198 | 205 | 303 |
| Closing trade published | EMA appended to the `ema-windows` topic | 112 | 204 | 238 | 347 |
| EMA appended to `ema-windows` | EMA committed in PostgreSQL | 54 | 124 | 230 | 390 |
| **Closing trade published** | **EMA committed in PostgreSQL (end to end)** | **172** | **285** | **350** | 461 |
| Closing trade published | Advisory committed in PostgreSQL (only the 11,349 windows with a crossover) | 181 | 461 | 484 | 489 |
| First trade past the window's end | Closing trade published (watermark wait) | 6 | 11 | 17 | 20 |
| First trade past the window's end | EMA committed in PostgreSQL (wait included) | 178 | 291 | 354 | 463 |

Firing the window is the slowest step. Flink advances the watermark only every 200 ms, so a window waits between 0 and 200 ms after its closing trade arrives, which matches the 105 ms median and 205 ms P99.

| Container | CPU avg (cores) | CPU max (cores) | Memory max (MiB) |
|---|---|---|---|
| Producer | 1.14 | 1.18 | 73 |
| Flink TaskManager | 0.28 | 0.84 | 1,232 |
| Redpanda | 0.10 | 0.12 | 2,077 |
| PostgreSQL | 0.13 | 0.20 | 242 |
| Materializer | 0.07 | 0.12 | 17 |

The pipeline uses about 1.7 of the 10 available cores. The producer uses a little over one core because its CSV loop is single-threaded, and that loop is what limits throughput.

## E3: Latency vs. input rate (single broker)

Full week at a fixed producer rate. The last row is E2, with no rate limit.

| Target rate | Achieved (events/s) | Run time (s) | End-to-end P50 (ms) | P95 | P99 | Max | Watermark wait P50 (ms) | Missing / duplicate | CPU cores: producer / Flink / Redpanda |
|---|---|---|---|---|---|---|---|---|---|
| 50k | 49,766 | 1,193 | 170 | 277 | 310 | 427 | 51 | 0 / 0 | 0.49 / 0.15 / 0.08 |
| 100k | 99,231 | 599 | 165 | 261 | 289 | 332 | 25 | 0 / 0 | 0.64 / 0.16 / 0.07 |
| 150k | 148,004 | 401 | 158 | 263 | 293 | 337 | 15 | 0 / 0 | 0.87 / 0.21 / 0.08 |
| none (E2) | 202,969 | 293 | 172 | 285 | 350 | 461 | 6 | 0 / 0 | 1.14 / 0.28 / 0.10 |

Short runs with the first 3 M trades of day 10:

| Target rate | Achieved (events/s) | End-to-end P50 (ms) | P95 | P99 | Watermark wait P50 (ms) | Wait included P50 (ms) |
|---|---|---|---|---|---|---|
| 5k | 4,978 | 212 | 304 | 336 | 513 | 708 |
| 10k | 9,856 | 223 | 307 | 330 | 257 | 464 |
| 25k | 23,690 | 193 | 309 | 338 | 103 | 279 |
| 50k | 47,560 | 162 | 289 | 312 | 51 | 204 |
| 100k | 84,120 | 155 | 253 | 278 | 25 | 176 |
| 150k | 118,794 | 162 | 273 | 318 | 11 | 173 |

- End-to-end latency barely depends on the rate: the median stays between 155 and 223 ms across a 30× range.
- No backlog builds up. In every full-week run, the median of each minute stays between 130 and 197 ms from start to finish.
- The watermark wait falls as the rate rises, roughly in proportion: at a higher rate, the replay covers 1 s of trade time in less wall-clock time.
- The short runs at 100k and 150k don't reach their targets. They last only 20–35 s, and the full-week runs at the same rates do reach theirs, so the shortfall is probably the producer ramping up at the start.

The first full-week attempt failed at 50k, although the pipeline itself completed and wrote all 1,855,638 windows. The latency script rejected one window because the broker's timestamp for its EMA was earlier than Flink's timestamp for producing it. This is a consistency check in `scripts/collect-latency.py`. The likely cause is that the broker's clock and Flink's clock differ by a few milliseconds, so it doesn't indicate a wrong result. The cause couldn't be confirmed because the topics were deleted before the rerun. The rerun passed at all three rates.

## E4: Flink alone (cluster, no producer)

The full week is loaded into the topics first, so Flink reads as fast as it can and the producer plays no part. Throughput is the 59.4 M trades divided by the median job duration, and the speed-up is relative to N = 1.

| N | Job duration, 3 runs (s) | Throughput | Speed-up | TaskManager CPU avg / max (cores) |
|---|---|---|---|---|
| 1 | 24.1 / 22.1 / 23.7 | 2.51 M events/s | 1.00× | 2.0 / 3.7 |
| 2 | 15.3 / 15.8 / 13.7 | 3.89 M events/s | 1.55× | 2.9 / 5.6 |
| 4 | 16.9 / 17.1 / 16.9 | 3.52 M events/s | 1.40× | 4.4 / 7.3 |

- With a single task, Flink is about 12 times faster than the producer's maximum rate.
- Two tasks help, but four are slower than two. We haven't verified why. A likely cause is the cost of passing data between parallel tasks, on a machine with only 4 performance cores.
- The brokers are never the limit. Each uses at most about 0.24 cores.

## E5: Whole pipeline on the cluster

Throughput and latency are the median of three runs. For latency, each run gives its own P50, P95 and P99, and the table shows the median of each. Correctness is checked once per N, after the last run.

| N | Throughput, median (min–max) | End-to-end latency P50 / P95 / P99 (ms) | Missing / duplicate | Correctness |
|---|---|---|---|---|
| 1 | 195k (195k–197k) | 181 / 320 / 437 | 0 / 0 | passed, 0 unexplained |
| 2 | 187k (185k–191k) | 214 / 354 / 472 | 0 / 0 | passed, 0 unexplained |
| 4 | 178k (177k–184k) | 233 / 453 / 750 | 0 / 0 | passed, 0 unexplained |

| N | Reference windows matched | Windows / advisories in PostgreSQL, all matching | Still open at the end |
|---|---|---|---|
| 1 | 1,855,645 / 1,855,645 | 1,855,638 / 11,349 | 7 |
| 2 | 1,855,645 / 1,855,645 | 1,855,638 / 11,349 | 7 |
| 4 | 1,855,645 / 1,855,645 | 1,855,548 / 11,349 | 97 |

These runs don't use a bounded job, so the last windows of the week have no later trade to close them. They stay open and never reach PostgreSQL, which is why the PostgreSQL count is the reference count minus the open windows. With more partitions, the watermark follows the slowest partition, so more windows stay open. This is expected, not an error.

| N | CPU cores: producer | Brokers 0 / 1 / 2 | TaskManager | Materializer | PostgreSQL | Broker memory max (MiB) |
|---|---|---|---|---|---|---|
| 1 | 1.18 | 0.10 / 0.09 / 0.09 | 0.30 | 0.08 | 0.13 | 1,943 |
| 2 | 1.20 | 0.14 / 0.14 / 0.11 | 0.36 | 0.07 | 0.13 | 1,947 |
| 4 | 1.24 | 0.18 / 0.19 / 0.17 | 0.48 | 0.07 | 0.13 | 1,904 |

Share of time each Flink task was busy, from Flink's own metrics, averaged over runs and parallel subtasks:

| Task | N = 1 | N = 2 | N = 4 |
|---|---|---|---|
| Source (reads `trade-events`) | 7% | 5% | 4% |
| Window (keeps each window's last trade) | 8% | 4% | 2% |
| EMA, and writing to `ema-windows` | 3% | 7% | 7% |
| Crossover, and writing to `trade-advisories` | 7% | 7% | 6% |
| Writing latency timestamps | 2% | 6% | 6% |

- Throughput doesn't rise with N, because the producer is the limit. No Flink task was busy more than 10% of the time, and backpressure never went above 0.03%.
- Each extra partition costs a little: throughput is 4% lower at N = 2 and 9% lower at N = 4, and latency rises too. Brokers and the TaskManager use more CPU as N grows, which suggests the extra cost comes from handling more, smaller batches. This hasn't been verified.
- All nine runs started on a freshly reset cluster. None failed or had to be rerun.

## Cost of replication (E2 vs. E5 at N = 1)

| Setup | Throughput | End-to-end latency P50 / P95 / P99 (ms) |
|---|---|---|
| 1 broker | 203k events/s | 172 / 285 / 350 |
| 3 brokers, replication factor 3 | 195k events/s | 181 / 320 / 437 |

Keeping three copies of every message costs about 4% throughput and 9 ms of median latency. The 99th percentile grows more, by 87 ms.

## Change from 5 s to 1 s

The same experiments were first run with a 5 s allowance on 21 November. Those runs used the code from before the 3-broker cluster change (#118). The 1 s runs use current `main` with only the allowance changed.

| | 5 s | 1 s |
|---|---|---|
| E1 correctness | passed, 0 unexplained, 5,506 events dropped | identical |
| E2 throughput | 191,437 events/s | 202,969 events/s |
| E2 end-to-end latency P50 / P95 / P99 (ms) | 181 / 301 / 367 | 172 / 285 / 350 |
| E2 watermark wait P50 | 14 ms | 6 ms |
| E2 wait included P50 | 194 ms | 178 ms |

Short runs, median in ms:

| Rate | Watermark wait, 5 s → 1 s | Wait included, 5 s → 1 s |
|---|---|---|
| 5k | 701 → 513 | 871 → 708 |
| 10k | 351 → 257 | 539 → 464 |
| 25k | 140 → 103 | 287 → 279 |
| 50k | 68 → 51 | 227 → 204 |
| 100k | 35 → 25 | 201 → 176 |
| 150k | 23 → 11 | 187 → 173 |

- **Correctness doesn't change.** No real trade is more than 264 ms late, so both allowances drop exactly the same events.
- **End-to-end latency doesn't change.** It starts at the closing trade, after the watermark wait, so the allowance isn't part of it. The small differences, in latency and in throughput, are within what we'd expect between single runs on different code versions.
- **The watermark wait gets shorter, but by less than expected.** A five times smaller allowance cuts the wait by only a quarter to a half. Most of the wait falls in the first second after a window ends, so removing the other four seconds saves less. The difference only matters at low rates: from 50k events/s up, the wait is under 70 ms with either allowance.

## Caveats

- Everything runs on one laptop, so the producer, the brokers and Flink compete for the same CPUs.
- E2 and E3 are a single run per setting. E4 and E5 have three runs per N.
- The replays run much faster than real time. In a live feed the watermark wait would be about 1 s of real time. End-to-end latency doesn't include it.
- E4 is the best case for Flink: the data is already in the topics, and no producer competes for CPU.
