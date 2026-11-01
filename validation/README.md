# Validation dataset

## Usage examples

```sh
python3 validation/generate_validation_data.py                 # full dataset, ~2 min
python3 validation/generate_validation_data.py --limit-rows 100000 --no-md5 --out /tmp/vout
python3 validation/generate_validation_data.py --ema-init first-close --out validation/out/first-close
python3 validation/generate_validation_data.py --keep-late-events --out /tmp/vout   # no 70 s watermark drop
python3 validation/validation_generator_test.py                # test suite
```

## Late events

Like the streaming processor (`forBoundedOutOfOrderness(70 s)`, one watermark
over all symbols, no allowed lateness), the generator drops a price event once
the watermark (max trade time seen - 70 s - 1 ms) has passed the end of its
5-minute window. Dropped events are only counted
(`rows.dropped_late_by_watermark` in `summary.json`); they add no window and
cannot become a symbol's first window. Use `--out-of-orderness-seconds N` to
change the delay or `--keep-late-events` to turn the rule off.

One divergence: the generator's watermark advances with every event, Flink's
only every 200 ms, so a late event arriving right after the event that
closed its window may still be kept by Flink.
`diagnostics.drops_pending_watermark_emission` counts these events and
`drops_pending_watermark_emission.csv` extracts them, when any, for separate
live tests.

## Outputs

- `ema_windows.csv` - one row per (symbol, window with price events): the
  window's close (price, micros, source row), EMA(38)/EMA(100) and the previous
  emitted pair (micros, 9 decimals), the advisory, and diagnostics.

- `advisories.csv` - one row per BUY/SELL advisory.

- `drops_pending_watermark_emission.csv` - written only when such events
  exist: the drops whose outcome depends on Flink's periodic watermark
  emission timing (see Late events), for separate live tests.

- `summary.json` - input checksum, row accounting (skipped/rejected/accepted/
  dropped late),
  symbol and window statistics, advisory counts, diagnostics, and the recorded
  spec and interpretation notes.

## Flow validation

End-to-end validation of the live pipeline: produce a CSV through the Go
producer, let the Flink processor run over it bounded, drain the results
into PostgreSQL with the materializer, then check the result three ways.

1. **Processor vs shadow.** Shadow-replay the published `trade-events` in
   Python and compare the processor's `ema-windows` and `trade-advisories`
   against the recomputation. This catches processor bugs (windowing, EMA,
   crossover, serialization).
2. **Shadow vs CSV reference.** Compare that same replay with the offline
   reference the generator computes straight from the CSV. This catches
   producer bugs (rows dropped, parsed differently, or reordered on the way
   to `trade-events`), which the first check cannot see because the shadow
   is built from whatever the producer published. No Flink timing is
   involved, so this comparison is exact.
3. **Database vs topics.** Compare the materializer's PostgreSQL tables
   (`symbols`, `ema_windows`, `trade_advisories`) with the records of the
   two result topics, exactly. This catches materializer bugs (decoding,
   upserts, offset handling), including the window the materializer also
   stores from every advisory, which check 1 never inspects.

Together they pin the database to the CSV: a row the producer mangles fails
check 2, a window the processor mangles fails check 1, a row the
materializer mangles or loses fails check 3.

Requirements: Docker (Redpanda, Flink, PostgreSQL), Go with a C compiler
(the producer uses cgo for librdkafka), Java 17 + sbt (the processor),
a `.env` (`cp .env.example .env` once; it
holds the database URL and the materializer's settings), and the Python
dependencies (the system python3 lacks confluent-kafka/protobuf/psycopg):

```sh
pip3 install -r validation/requirements.txt
```

One target runs the whole flow (it composes `generate-reference`,
`infra-up`, `reset-flow`, `run-data-producer`, `run-bounded-processor`,
`drain-materializer`, `compare-flow`):

```sh
make validate-flow CSV=dataset/debs2022-gc-trading-day-08-11-21.csv [RATE=0] [MAX_EVENTS=n] [DB_CHECK=0]
```

`RATE` (events per second, `0` = unlimited) is passed to the producer;
results do not depend on it, only the wall-clock time of the production
step does. `MAX_EVENTS` publishes only the first n events, which makes
experiments at low rates quick; the CSV reference needs the whole file
(the producer counts events, the generator counts rows), so a `MAX_EVENTS`
run skips check 2 (checks 1 and 3 still run). `DB_CHECK=0` skips the
materializer and check 3 for machines without PostgreSQL or a `.env`.
`BOUNDED_TIMEOUT` (seconds, default 3600) bounds the wait for the bounded
Flink run to reach FINISHED, and `REFERENCE_DIR` (default
`validation/out/reference`) is where the reference is written and read.
The steps can also be run individually:

```sh
make generate-reference CSV=dataset/...                         # offline Python reference, ~3 min per day
make infra-up                                                   # Redpanda + Flink + topics
make reset-flow                                                 # recreate the flow topics empty, clear consumer offsets and the DB, stop jobs
make run-data-producer ARGS="-csvPath=dataset/... -rate=0 -dlqEnabled"
make run-bounded-processor BOUNDED_TIMEOUT=3600                 # process trade-events, then stop
make drain-materializer                                         # run the materializer until it has consumed both result topics, then stop it
make compare-flow REFERENCE_DIR=validation/out/reference        # all checks; omit REFERENCE_DIR to skip 2, DB_CHECK=0 to skip 3
```

`drain-materializer` (`scripts/drain-materializer.py`) waits until the
materializer's consumer group has committed every record of `ema-windows`
and `trade-advisories` (it commits only after the database transaction), so
the database is complete when it returns; a timeout (`DRAIN_TIMEOUT_S` in
`validation/config.py`), an early exit or a non-zero shutdown fails.
`compare-flow` (`validation/verify_flow.py`) writes a JSON report to
`validation/out/flow-report.json` (`--report` overrides the path; a
`reference` block is added when `REFERENCE_DIR` is given and a `database`
block when the database is checked) and exits non-zero when the flow fails.
Check 2 passes iff every window of the reference exists in the topic with
the same event count, close, EMA state and advisory (against the shadow's
drop-model replay, the generator's late-event rule), and vice versa. Check
3 passes iff every table holds exactly the rows the topics imply, by value
(`1.50` equals `1.5`), and each advisory's embedded window equals its
`ema-windows` record. In check 1 every window is classified as one of four
mismatch classes:

- `match` - actual output equals the shadow's keep-all replay.
- `lateness-explained` - the drop-model shadow reproduces the actual
  output exactly (watermark drops account for the difference).
- `lateness-related` - the window differs, the drop model does not
  reproduce it, but the drop model's expected row for the window differs
  from the keep-all row (a dropped event in this window or an upstream
  one); periodic watermark timing can drop a subset, so the residual is
  reported but does not fail the run.
- `unexplained` - anything else; this class fails the run.

`at_risk_events` in the report counts the events the drop model drops;
on near-chronological data (days 08-09 of the bundled dataset) it is
zero, while day 10 contains genuine late events — the run passes either
way when every mismatch is reproduced by the drop model.

