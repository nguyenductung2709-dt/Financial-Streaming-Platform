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

## Outputs

- `ema_windows.csv` - one row per (symbol, window with price events): the
window's close (price, micros, source row), EMA(38)/EMA(100) and the previous
emitted pair (micros, 9 decimals), the advisory, and diagnostics.

- `advisories.csv` - one row per BUY/SELL advisory.

- `summary.json` - input checksum, row accounting (skipped/rejected/accepted/
  dropped late),
symbol and window statistics, advisory counts, diagnostics, and the recorded
spec and interpretation notes.

