# Validation

We check the pipeline against a separate Python implementation of the same windowing,
watermark and EMA rules. If both arrive at the same numbers on their own, we can trust
the results.

You need Docker, Go with a C compiler, Java 17 with sbt, a `.env` file, and the Python
packages:

```
pip3 install -r validation/requirements.txt
make validate-flow CSV=dataset/debs2022-gc-trading-day-08-11-21.csv
```

`CSV` can also be a directory, or a quoted list of files that are replayed in the given
order. `RATE` limits the producer, and `MAX_EVENTS` stops it early.

`validate-flow` resets the pipeline, replays the CSV, runs the processor as a bounded
job so that every window closes, and waits for the materializer to catch up. Then it
runs three checks:

1. It replays the published `trade-events` in Python and compares the result with
   `ema-windows` and `trade-advisories`. This catches processor bugs.
2. It compares that replay with a reference computed directly from the CSV. This
   catches producer bugs. `MAX_EVENTS` skips this check.
3. It compares the PostgreSQL tables with the topics. This catches materializer bugs.
   `DB_CHECK=0` skips this check.

Checks 2 and 3 must match exactly. Check 1 sorts differences into `lateness-explained`,
`lateness-related` and `unexplained`, and only `unexplained` fails the run. The other
two come from late events that both sides dropped in slightly different places. The
report is written to `validation/out/flow-report.json`.

## Checking a normal run

`run-flow` and `measure-flow` don't run the processor bounded, so the last few windows
are still open when the input ends. To check one of these runs:

```
make compare-flow LIVE=1
```

Windows that the final watermark hasn't passed yet are reported as `open at end`
instead of failing. Anything else that's missing or wrong still fails.

## Just the reference

```
make generate-reference CSV=dataset/<file>.csv
```

This writes `ema_windows.csv`, `advisories.csv` and `summary.json` without running the
pipeline. Flink moves its watermark forward only every 200 ms, so it can keep a handful
of events that the reference drops. Those events are listed in
`drops_pending_watermark_emission.csv`.

`make test-validation` runs the unit tests.
