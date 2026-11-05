# Validation

## Reference generator

```sh
python3 validation/generate_validation_data.py [--csv PATH] [--limit-rows N] [--out DIR]
python3 validation/validation_generator_test.py
```

Writes `ema_windows.csv`, `advisories.csv` and `summary.json` (`--help` lists
the other options). Like the processor, it drops a price event once the
watermark (max trade time - 70 s) has passed its 5-minute window and counts the
drops in `summary.json`. Flink's watermark only advances every 200 ms, so it may
keep a few events the generator drops; those are listed in
`drops_pending_watermark_emission.csv`.

## Flow validation

Produces a CSV through the Go producer and the Flink processor, drains the
results into PostgreSQL with the materializer, then checks:

1. **Processor vs shadow:** a Python replay of the published `trade-events`
   against `ema-windows` and `trade-advisories`. Catches processor bugs.
2. **Shadow vs CSV reference:** exact. Catches producer bugs.
3. **Database vs topics:** exact. Catches materializer bugs.

Requirements: Docker, Go with a C compiler, Java 17 + sbt, a `.env`
(`cp .env.example .env`), and `pip3 install -r validation/requirements.txt`.

```sh
make validate-flow CSV=dataset/<file>.csv [RATE=0] [MAX_EVENTS=n] [DB_CHECK=0]
make validate-flow CSV=dataset
make validate-flow CSV="dataset/day08.csv dataset/day09.csv"
```

`CSV` accepts a file, a whitespace-separated list in supplied order, or a directory
(immediate `.csv` files, alphabetical). State and limits continue across files.
For native tools, repeat `-csvPath` / `--csv`; quote paths containing spaces.

`RATE` is the producer's events per second (`0` is unlimited). `MAX_EVENTS`
publishes only the first n events and skips check 2. `DB_CHECK=0` skips the
materializer and check 3. The report goes to `validation/out/flow-report.json`,
and the target exits non-zero on failure.

Check 1 labels each window `match`, `lateness-explained`, `lateness-related` or
`unexplained`. Only `unexplained` fails the run; the lateness labels are
differences attributable to late-event drops.
