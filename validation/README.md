# Validation dataset

## Usage examples

```sh
python3 validation/generate_validation_data.py                 # full dataset, ~2 min
python3 validation/generate_validation_data.py --limit-rows 100000 --no-md5 --out /tmp/vout
python3 validation/generate_validation_data.py --ema-init first-close --out validation/out/first-close
python3 validation/validation_generator_test.py                # test suite
```

## Outputs

- `ema_windows.csv` - one row per (symbol, window with price events): the
window's close (price, micros, source row), EMA(38)/EMA(100) and the previous
emitted pair (micros, 9 decimals), the advisory, and diagnostics.

- `advisories.csv` - one row per BUY/SELL advisory.

- `summary.json` - input checksum, row accounting (skipped/rejected/accepted),
symbol and window statistics, advisory counts, diagnostics, and the recorded
spec and interpretation notes.

