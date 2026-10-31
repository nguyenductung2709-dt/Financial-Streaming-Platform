#!/usr/bin/env python3
"""Offline ground truth for the two course project queries.

Computes, from the trading-day CSV, the expected output of Query 1 (EMA 38
and EMA 100 per symbol over 5-minute tumbling windows) and Query 2 (BUY/SELL
advisories on EMA crossovers), so the streaming system's results can be
validated.  Spec: documents/CS_E4780_Course_Project_2026.pdf.

The event stream mirrors services/data-producer (what actually reaches the
trade-events topic): preamble lines 1-11 and 13 are skipped (line 12 is the
header); a row is a price event iff `Last` is non-empty, a nonnegative
decimal with at most six fractional digits, and non-zero; trade time =
`Trading date` (falling back to the system `Date` column) + `Trading time`
parsed like Go's time.Parse with layout "02-01-2006 15:04:05.000" in a fixed
CEST zone: DD-MM-YYYY, one- or two-digit hour, two-digit minute and second,
and "." or "," before exactly three millisecond digits; instants before
0001-01-01T00:00:00Z UTC (0001-01-01 before 02:00 CEST) are rejected like the
producer's protobuf range check; prices are exact
integers in millionths (micros); rows the producer would reject are excluded
and counted as would-DLQ.

Late events follow services/streaming-processor: its watermark is
forBoundedOutOfOrderness(70 s) over all symbols (the trade-events topic has
one partition) with zero allowed lateness, so a price event whose 5-minute
window the watermark (max trade time seen - 70 s - 1 ms) has already passed is
dropped by Flink.  The script applies the same rule in file order and only
counts the dropped events (rows.dropped_late_by_watermark).
--out-of-orderness-seconds changes the delay, --keep-late-events disables the
rule.  One divergence: the watermark here advances with every event while
Flink emits it every 200 ms (auto-watermark interval), so a late event
arriving right after the event that closed its window -- 07:04 -> 07:07 ->
07:04:40 -- is dropped here but may be kept by Flink.
diagnostics.drops_pending_watermark_emission counts these (a lower bound)
and, when any, extracts them to drops_pending_watermark_emission.csv for
separate testing.

Interpretation decisions (recorded in summary.json):
  1. The EMA of the previous window is 0, so a symbol's first EMA is
     Close * 2/(1+j).  Alternatives: --ema-init first-close seeds both EMAs
     with the first Close; --ema-init zero-window is the literal
     EMA[s, w_0] = 0 reading (the same series shifted one window).
  2. Windows without price events emit no row; the previous EMA pair carries
     forward, and crossovers compare against the last emitted pair.
  3. Close = price of the event with the maximum trade time in the window;
     ties break to the later source row.
  4. Consequence of 1: every symbol's first window satisfies the BUY rule
     (EMA38 = c*2/39 > c*2/101 = EMA100 with previous pair (0, 0)), so each
     symbol starts with one BUY advisory.  This is inherent to the formulas,
     not a bug.
  5. Late events: dropped as above, so a stale update arriving long after
     its window (e.g. the end of the day-10 file) neither adds a window nor
     becomes a symbol's first window.

Validate streaming output by joining on (symbol, window): EMA tolerance
1e-3 micros (this script uses 50-digit decimal arithmetic; double-precision
noise is ~1e-7 micros), advisories exact.
"""

import argparse
import csv
import hashlib
import json
import math
import re
import sys
import time
from collections import Counter
from datetime import date
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path

import ema_replay
from ema_replay import is_late, update_close

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = REPO_ROOT / "dataset" / "debs2022-gc-trading-day-12-11-21.csv"
DEFAULT_OUT = REPO_ROOT / "validation" / "out"

WINDOW_MS = ema_replay.WindowMs  # 5-minute tumbling windows anchored at 00:00 CEST (PDF 3.1)
MS_PER_DAY = 86_400_000
PRICE_SCALE = 1_000_000    # prices are exact integers in millionths, like last_price_micros
UINT64_MAX = 2**64 - 1
# StreamingProcessor.OutOfOrdernessAllowance: forBoundedOutOfOrderness(70 s), allowed lateness 0.
OUT_OF_ORDERNESS_MS = ema_replay.OutOfOrdernessMs
# Rows extracted to drops_pending_watermark_emission.csv (the summary count is exact).
EMISSION_CASES_CAP = 1_000
DECIMAL_PREC = 50          # far tighter than double precision
EMA_PLACES = 9             # decimals emitted for EMA values in micros
J_SHORT, J_LONG = ema_replay.JShort, ema_replay.JLong  # smoothing factors (PDF 3.2, 3.3)

# First-window EMA initialization:
#   prev-zero    default; the PDF's "the EMA of the previous window is zero".
#   first-close  classic bootstrap; first EMA = Close for both factors.
#   zero-window  literal EMA[s, w_0] = 0; the recursion starts in window two.
EMA_INIT_MODES = ("prev-zero", "first-close", "zero-window")

REQUIRED_COLUMNS = ("ID", "SecType", "Last", "Trading time", "Trading date")
# parseTradeTime in event.go uses the Go layout "02-01-2006 15:04:05.000":
# Go takes a one- or two-digit hour and either "." or "," before the exactly
# three millisecond digits.
DATE_RE = re.compile(r"\A(\d{2})-(\d{2})-(\d{4})\Z")
TIME_RE = re.compile(r"\A(\d{1,2}):(\d{2}):(\d{2})[.,](\d{3})\Z")

# closes[(symbol, window_index)] = CloseState (close by trade time, tie: later
# row), mirrored by arrivals[(symbol, window_index)] = [arrival_micros,
# arrival_row] for last-arrival diagnostics (not part of the replay).
# symbols[symbol] = [security_type, last_seen_tt_ms, late_event_count]
SECTYPE, LAST_TT_MS, LATE = range(3)

INTERPRETATION_NOTES = (
    "First window: the EMA of the previous window is 0, so the first EMA is Close*2/(1+j); "
    "alternatives via --ema-init (first-close seeds with Close, zero-window emits 0).",
    "Windows without price events emit no row; the previous EMA pair carries forward and "
    "crossovers compare against the last emitted pair.",
    "Close = price of the event with the maximum trade time in the window (ties: later source row).",
    "Under prev-zero/zero-window initialization every symbol's first window fires a BUY "
    "advisory (inherent to the formulas, not a bug).",
    "Late events: like the Flink job, a price event whose 5-minute window is already behind "
    "the watermark (max trade time seen over all symbols - out_of_orderness_ms - 1 ms, allowed "
    "lateness 0) is dropped and only counted in rows.dropped_late_by_watermark; "
    "--keep-late-events disables this (out_of_orderness_ms is then null).",
)


def parse_price_micros(text):
    """Parse a `Last` value into millionths, mirroring parsePriceMicros in event.go."""
    whole, sep, frac = text.strip().partition(".")
    if whole == "" or (sep and frac == ""):
        raise ValueError("price must contain digits before and after any decimal point")
    digits = whole + frac
    if not (digits.isascii() and digits.isdigit()):
        raise ValueError("price must be a nonnegative decimal number")
    if len(frac) > 6:
        raise ValueError("price has more than six decimal places")
    fraction = int(frac.ljust(6, "0"))
    whole_int = int(whole)
    if whole_int > (UINT64_MAX - fraction) // PRICE_SCALE:  # same overflow check as event.go
        raise ValueError("price in millionths exceeds uint64 range")
    return whole_int * PRICE_SCALE + fraction


def parse_trade_dt(date_text, time_text):
    """DD-MM-YYYY + H(H):MM:SS[.,]sss -> (day_ordinal, seconds_of_day, millis).

    Mirrors parseTradeTime in event.go (fixed CEST zone): Go's time.Parse
    accepts a one-digit hour and a comma before the milliseconds, and the
    protobuf timestamp range starts at 0001-01-01T00:00:00Z (02:00 CEST).
    Window assignment only depends on the wall clock, so any fixed offset
    gives identical windows.
    """
    m, t = DATE_RE.match(date_text), TIME_RE.match(time_text)
    if m is None or t is None:
        raise ValueError(f"bad trading date/time {date_text!r} {time_text!r}")
    day = date(int(m[3]), int(m[2]), int(m[1]))  # ValueError on invalid calendar dates
    hh, mi, ss, ms = int(t[1]), int(t[2]), int(t[3]), int(t[4])
    if hh > 23 or mi > 59 or ss > 59:
        raise ValueError(f"trading time out of range {time_text!r}")
    sod = hh * 3600 + mi * 60 + ss
    if day == date.min and sod < 7200:  # before 02:00 CEST = before the protobuf range start
        raise ValueError(f"trade time before 0001-01-01T00:00:00Z UTC {date_text!r} {time_text!r}")
    return day.toordinal(), sod, ms


def scan_events(csv_path, limit_rows=None, out_of_orderness_ms=OUT_OF_ORDERNESS_MS):
    """Single pass over the CSV, mirroring the producer's row handling.

    Price events later than the watermark are dropped like the Flink job does
    (None keeps them all) and only counted.  Returns (closes, arrivals,
    symbols, stats, emission_cases) with per (symbol, window) close state
    (close by trade time, tie: later row; via update_close), per-window
    last-arrival [micros, row] for diagnostics, per symbol [sectype,
    last_tt_ms, late_count], counters, and the (source_row, symbol,
    trade_time_ms) of dropped events whose window the immediately preceding
    event just closed -- Flink's periodic watermark emission (200 ms
    auto-watermark interval) may not have fired yet, so it may keep them.
    """
    stats = Counter()
    closes, arrivals, symbols = {}, {}, {}
    columns = None
    max_tt_ms = float("-inf")  # the watermark follows the max trade time over all symbols
    prev_row_max = float("-inf")  # the same max as of the previous event row (one-event lag)
    emission_cases = []
    started = time.monotonic()
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for record in reader:
            line = reader.line_num  # physical line number, like Go's FieldPos
            if line == 12:  # header; lines 1-11 and 13 are preamble
                columns = {name.strip(): i for i, name in enumerate(record)}
                missing = [c for c in REQUIRED_COLUMNS if c not in columns]
                if missing:
                    raise ValueError(f"CSV header is missing columns {missing}")
                idx = {c: columns[c] for c in REQUIRED_COLUMNS}
                idx["Date"] = columns.get("Date", -1)
                max_required = max(columns[c] for c in REQUIRED_COLUMNS)
                continue
            if line < 12 or line == 13:
                continue
            if columns is None:
                raise ValueError("CSV header is missing at line 12")

            if limit_rows is not None and stats["data_rows"] >= limit_rows:
                stats["truncated"] = 1
                break
            stats["data_rows"] += 1
            if stats["data_rows"] % 5_000_000 == 0:
                print(f"  rows={stats['data_rows']:,} events={stats['price_events']:,} "
                      f"windows={len(closes):,} {time.monotonic() - started:.0f}s",
                      file=sys.stderr, flush=True)
            if len(record) <= max_required:
                raise ValueError(f"CSV line {line} is missing required fields")

            last_text = record[idx["Last"]].strip()
            if last_text == "":
                stats["skipped_no_last"] += 1
                continue
            stats["rows_last_nonempty"] += 1
            try:
                micros = parse_price_micros(last_text)
            except ValueError:
                stats["rejected_bad_price"] += 1
                continue
            if micros == 0:
                stats["skipped_zero_last"] += 1
                continue
            symbol = record[idx["ID"]].strip()
            sect = record[idx["SecType"]].strip()
            if not symbol or sect not in ("E", "I"):
                stats["rejected_empty_symbol" if not symbol else "rejected_bad_sectype"] += 1
                continue
            tdate = record[idx["Trading date"]].strip()
            if not tdate and 0 <= idx["Date"] < len(record):  # Date fallback (event.go)
                tdate = record[idx["Date"]].strip()
            if not tdate:
                stats["rejected_missing_date"] += 1
                continue
            try:
                ordinal, sod, ms = parse_trade_dt(tdate, record[idx["Trading time"]].strip())
            except ValueError:
                stats["rejected_bad_trade_time"] += 1
                continue

            stats["price_events"] += 1
            tt_ms = ordinal * MS_PER_DAY + sod * 1000 + ms
            lag_max, prev_row_max = prev_row_max, max_tt_ms  # one-event watermark lag
            if out_of_orderness_ms is not None and is_late(tt_ms, max_tt_ms, out_of_orderness_ms):
                stats["dropped_late"] += 1
                if not is_late(tt_ms, lag_max, out_of_orderness_ms):
                    # Only the immediately preceding event closed this window;
                    # Flink may not have emitted the watermark yet.
                    stats["drops_pending_emission"] += 1
                    if len(emission_cases) < EMISSION_CASES_CAP:
                        emission_cases.append((line, symbol, tt_ms))
                continue
            max_tt_ms = max(max_tt_ms, tt_ms)
            sym_state = symbols.get(symbol)
            if sym_state is None:
                symbols[symbol] = [sect, tt_ms, 0]
            else:
                if sym_state[SECTYPE] != sect:
                    stats["sectype_conflicts"] += 1
                if tt_ms < sym_state[LAST_TT_MS]:  # trade time went backwards, within the allowance
                    sym_state[LATE] += 1
                    stats["late_events"] += 1
                sym_state[LAST_TT_MS] = tt_ms
            key = (symbol, tt_ms // WINDOW_MS)
            closes[key] = update_close(closes.get(key), tt_ms, micros, line)
            arrivals[key] = (micros, line)
    stats["scan_seconds"] = round(time.monotonic() - started, 1)
    return closes, arrivals, symbols, stats, emission_cases


def fmt_ema(value):
    """EMA in micros with 9 decimals; fixed-point (str(Decimal) gives 0E-9)."""
    return format(value.quantize(Decimal(1).scaleb(-EMA_PLACES),
                                 rounding=ROUND_HALF_EVEN), "f")


def micros_to_price(micros):
    return f"{micros // PRICE_SCALE}.{micros % PRICE_SCALE:06d}"


def ms_to_wall_clock(ms):
    """A millisecond timestamp as a CEST wall-clock string (millisecond precision)."""
    ordinal, rem = divmod(ms, MS_PER_DAY)
    hour, rem = divmod(rem, 3_600_000)
    minute, second = divmod(rem, 60_000)
    return (f"{date.fromordinal(ordinal).isoformat()}T{hour:02d}:{minute:02d}:"
            f"{second // 1000:02d}.{second % 1000:03d}")


def window_bounds(widx):
    """[start, end) of a window as CEST wall-clock strings."""
    start = widx * WINDOW_MS
    return ms_to_wall_clock(start)[:16], ms_to_wall_clock(start + WINDOW_MS)[:16]


def compute_outputs(closes, arrivals, symbols, out_dir, ema_init="prev-zero"):
    """Replay the windows via the shared module and write ema_windows.csv / advisories.csv."""
    out_dir.mkdir(parents=True, exist_ok=True)
    ema_path, adv_path = out_dir / "ema_windows.csv", out_dir / "advisories.csv"
    stats = Counter()
    ema_header = ["symbol", "security_type", "window_start", "window_end", "window_index",
                  "event_count", "close_price", "close_micros", "close_source_row",
                  f"ema{J_SHORT}_micros", f"ema{J_LONG}_micros",
                  f"prev_ema{J_SHORT}_micros", f"prev_ema{J_LONG}_micros",
                  "advisory", "arrival_close_differs"]
    adv_header = ["symbol", "security_type", "window_start", "window_end", "window_index",
                  "advice", "is_first_window", "close_price", "close_micros",
                  "close_source_row", f"ema{J_SHORT}_micros", f"ema{J_LONG}_micros",
                  f"prev_ema{J_SHORT}_micros", f"prev_ema{J_LONG}_micros"]
    results = ema_replay.replay(closes, ema_init=ema_init)
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PREC
        with open(ema_path, "w", newline="", encoding="utf-8") as fema, \
             open(adv_path, "w", newline="", encoding="utf-8") as fadv:
            w_ema, w_adv = csv.writer(fema, lineterminator="\n"), csv.writer(fadv, lineterminator="\n")
            w_ema.writerow(ema_header)
            w_adv.writerow(adv_header)
            symbol, sect, first = None, None, False
            for res in results:
                if res.symbol != symbol:
                    symbol, sect = res.symbol, symbols[res.symbol][SECTYPE]
                    first = True
                arrival_differs = int(arrivals[(res.symbol, res.window_index)][0] != res.close_micros)
                stats["ema_rows"] += 1
                stats["arrival_close_differs"] += arrival_differs
                row = [res.symbol, sect, *window_bounds(res.window_index), res.window_index,
                       res.event_count, micros_to_price(res.close_micros), res.close_micros,
                       res.close_order, fmt_ema(res.ema38_micros), fmt_ema(res.ema100_micros),
                       fmt_ema(res.prev_ema38_micros), fmt_ema(res.prev_ema100_micros),
                       res.advisory, arrival_differs]
                w_ema.writerow(row)
                if res.advisory:
                    stats["advisories_" + res.advisory.lower()] += 1
                    if first:
                        stats["advisories_" + res.advisory.lower() + "_first_window"] += 1
                    w_adv.writerow(row[:5] + [res.advisory, int(first)] + row[6:13])
                first = False
    return stats, ema_path, adv_path


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 23), b""):
            h.update(chunk)
    return h.hexdigest()


def display_path(path):
    """Repo-relative path when inside the repo, else absolute."""
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def run(csv_path, out_dir, limit_rows=None, ema_init="prev-zero", compute_md5=True,
        out_of_orderness_ms=OUT_OF_ORDERNESS_MS):
    print(f"Input CSV : {csv_path}")
    print(f"Output dir: {out_dir}")
    closes, arrivals, symbols, stats, emission_cases = scan_events(
        csv_path, limit_rows=limit_rows, out_of_orderness_ms=out_of_orderness_ms)
    if stats["dropped_late"]:
        print(f"Dropped {stats['dropped_late']:,} late events (watermark delay "
              f"{out_of_orderness_ms / 1000:g}s)")
    print(f"Computing EMAs and advisories (init={ema_init})...")
    out_stats, ema_path, adv_path = compute_outputs(closes, arrivals, symbols, out_dir, ema_init)
    pending_path = None
    if emission_cases:
        pending_path = out_dir / "drops_pending_watermark_emission.csv"
        with open(pending_path, "w", newline="", encoding="utf-8") as fpend:
            w_pend = csv.writer(fpend, lineterminator="\n")
            w_pend.writerow(["source_row", "symbol", "trade_time", "window_start", "window_end"])
            for row, symbol, event_tt_ms in emission_cases:
                start, end = window_bounds(event_tt_ms // WINDOW_MS)
                w_pend.writerow([row, symbol, ms_to_wall_clock(event_tt_ms), start, end])

    summary = {
        "spec": {
            "source": "documents/CS_E4780_Course_Project_2026.pdf",
            "window_minutes": WINDOW_MS // 60_000,
            "smoothing_short": J_SHORT,
            "smoothing_long": J_LONG,
            "ema_init": ema_init,
            "out_of_orderness_ms": out_of_orderness_ms,
            "ema_recursion": "EMA_i = Close_i * 2/(1+j) + EMA_{i-1} * (1 - 2/(1+j))",
            "first_window_ema": {
                "prev-zero": "Close * 2/(1+j) (EMA of previous window = 0)",
                "first-close": "Close (both EMAs seeded with the first window's Close)",
                "zero-window": "0 (literal EMA[s, w_0] = 0)",
            }[ema_init],
            "buy_rule": f"EMA{J_SHORT} > EMA{J_LONG} AND prev EMA{J_SHORT} <= prev EMA{J_LONG}",
            "sell_rule": f"EMA{J_SHORT} < EMA{J_LONG} AND prev EMA{J_SHORT} >= prev EMA{J_LONG}",
            "interpretation_notes": list(INTERPRETATION_NOTES),
        },
        "input": {
            "csv_path": display_path(csv_path),
            "csv_md5": md5_file(csv_path) if compute_md5 else None,
            "truncated_by_limit": bool(stats.get("truncated")),
        },
        "rows": {
            "data_rows": stats["data_rows"],
            "price_rows_last_nonempty": stats["rows_last_nonempty"],
            "skipped_no_last": stats["skipped_no_last"],
            "skipped_zero_last": stats["skipped_zero_last"],
            "rejected_would_go_to_dlq": {
                "bad_price": stats["rejected_bad_price"],
                "empty_symbol": stats["rejected_empty_symbol"],
                "bad_sectype": stats["rejected_bad_sectype"],
                "missing_date": stats["rejected_missing_date"],
                "bad_trade_time": stats["rejected_bad_trade_time"],
            },
            "price_events_accepted": stats["price_events"],
            "dropped_late_by_watermark": stats["dropped_late"],
        },
        "symbols": {
            "with_price_events": len(symbols),
            "sectype_conflicts": stats["sectype_conflicts"],
            "windows_with_events": len(closes),
        },
        "advisories": {
            "buy": out_stats["advisories_buy"],
            "sell": out_stats["advisories_sell"],
            "buy_first_window": out_stats["advisories_buy_first_window"],
            "sell_first_window": out_stats["advisories_sell_first_window"],
        },
        "diagnostics": {
            "late_events": stats["late_events"],
            "symbols_with_late_events": sum(1 for s in symbols.values() if s[LATE]),
            "windows_where_arrival_close_differs": out_stats["arrival_close_differs"],
            "drops_pending_watermark_emission": stats["drops_pending_emission"],
        },
        "outputs": {
            "ema_windows_csv": display_path(ema_path),
            "advisories_csv": display_path(adv_path),
            "ema_rows": out_stats["ema_rows"],
            "drops_pending_watermark_emission_csv":
                display_path(pending_path) if pending_path is not None else None,
        },
        "run": {"scan_seconds": stats["scan_seconds"], "python_version": sys.version.split()[0]},
    }
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")
    print(f"Wrote {ema_path} ({summary['outputs']['ema_rows']:,} rows)")
    print(f"Wrote {adv_path} ({summary['advisories']['buy'] + summary['advisories']['sell']:,} rows)")
    if pending_path is not None:
        cap_note = "" if stats["drops_pending_emission"] == len(emission_cases) \
            else f" (first {len(emission_cases):,} extracted)"
        print(f"Wrote {pending_path} "
              f"({stats['drops_pending_emission']:,} events{cap_note})")
    print(f"Wrote {summary_path}")
    return summary


def finite_non_negative_seconds(text):
    seconds = float(text)
    if not (math.isfinite(seconds) and seconds >= 0):
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return seconds


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate the EMA/advisory validation dataset from the trading CSV.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="trading day CSV path")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory")
    parser.add_argument("--limit-rows", type=int, default=None,
                        help="stop after N data rows (smoke tests only)")
    parser.add_argument("--ema-init", choices=EMA_INIT_MODES, default="prev-zero",
                        help="first-window EMA initialization mode")
    parser.add_argument("--no-md5", action="store_true", help="skip the CSV md5 checksum")
    late = parser.add_mutually_exclusive_group()
    late.add_argument("--out-of-orderness-seconds", type=finite_non_negative_seconds,
                      default=OUT_OF_ORDERNESS_MS / 1000,
                      help="watermark delay after which Flink drops late events "
                           "(default: %(default)g, the processor's setting)")
    late.add_argument("--keep-late-events", action="store_true",
                      help="never drop late events (the Flink job would)")
    args = parser.parse_args(argv)
    out_of_orderness_ms = None if args.keep_late_events else round(args.out_of_orderness_seconds * 1000)
    run(args.csv, args.out, limit_rows=args.limit_rows, ema_init=args.ema_init,
        compute_md5=not args.no_md5, out_of_orderness_ms=out_of_orderness_ms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())