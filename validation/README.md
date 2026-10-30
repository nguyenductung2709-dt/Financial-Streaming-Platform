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
producer, let the Flink processor run over it bounded, then shadow-replay
the published trade events in Python and compare the processor's
`ema-windows` and `trade-advisories` against the recomputation.

Install the Python dependencies first (the system python3 lacks
confluent-kafka/protobuf):

```sh
pip3 install -r validation/requirements.txt
```

One target runs the whole flow (it composes `infra-up`, `reset-flow`,
`run-data-producer`, `run-bounded-processor`, `compare-flow`):

```sh
make validate-flow CSV=dataset/debs2022-gc-trading-day-08-11-21.csv [RATE=0]
```

`RATE` (events per second, `0` = unlimited) is passed to the producer;
`BOUNDED_TIMEOUT` (seconds, default 3600) bounds the wait for the
bounded Flink run to reach FINISHED. The steps can also be run
individually:

```sh
make infra-up                                                   # Redpanda + Flink + topics
make reset-flow                                                 # clear the flow topics, stop jobs
make run-data-producer ARGS="-csvPath=dataset/... -rate=0 -dlqEnabled"
make run-bounded-processor BOUNDED_TIMEOUT=3600                 # process trade-events, then stop
make compare-flow                                               # compare against the shadow
```

`compare-flow` (`validation/verify_flow.py`) writes a JSON report to
`validation/out/flow-report.json` (`--report` overrides the path) and
exits non-zero when the flow fails. Every window is classified as one
of four mismatch classes:

- `match` - actual output equals the shadow's keep-all replay.
- `lateness-explained` - the drop-model shadow reproduces the actual
  output exactly (watermark drops account for the difference).
- `lateness-related` - the window differs and the drop model dropped at
  least one of its events; periodic watermark timing can drop a subset,
  so the residual is reported but does not fail the run.
- `unexplained` - anything else; this class fails the run.

`at_risk_events` in the report counts the events the drop model drops;
on near-chronological data (days 08-09 of the bundled dataset) it is
zero, while day 10 contains genuine late events — the run passes either
way when every mismatch is reproduced by the drop model.

