#!/usr/bin/env python3
"""Offline CSV reference vs the shadow replay of the trade-events topic.

The CSV generator (generate_validation_data.py) recomputes every window
straight from the dataset file; the shadow recomputes them from what the
producer actually published.  Comparing the two isolates the producer
(CSV -> trade-events): rows it dropped, parsed differently, or reordered
show up as a window whose close, event count, or EMA state differs.  No
Flink timing is involved on either side, so the comparison is exact.

The generator drops late events by default, so a default reference is
compared with the shadow's drop-model replay; a --keep-late-events
reference is compared with the keep-all replay.  References generated with
another EMA init or watermark delay describe a different pipeline and are
rejected.  Windows are joined on (symbol, window start in epoch ms): the
generator's CEST wall-clock window_start is converted with the producer's
fixed UTC offset.  Mismatch parts read "<name> <topic> != <reference>".
"""

import csv
import json
from datetime import datetime, timedelta, timezone
from decimal import localcontext
from pathlib import Path
from typing import NamedTuple

import compare as cmp
import config
import generate_validation_data as gvd
from ema_replay import WindowMs

EMA_CSV = "ema_windows.csv"
SUMMARY_JSON = "summary.json"
MISSING_FROM_TOPIC = "missing from the topic"
NOT_IN_REFERENCE = "not in the reference"


class ReferenceWindow(NamedTuple):
    """One generator ema_windows.csv row; EMAs stay the generator's strings."""

    event_count: int
    close_micros: int
    ema38_micros: str
    ema100_micros: str
    prev_ema38_micros: str
    prev_ema100_micros: str
    advisory: str  # "BUY" / "SELL" / ""


class Reference(NamedTuple):
    """windows keyed by (symbol, window_start_ms); None delay = late events kept."""

    windows: dict[tuple[str, int], ReferenceWindow]
    out_of_orderness_ms: int | None


class ReferenceResult(NamedTuple):
    """side is the shadow replay compared: "drop_model" or "keep_all"."""

    side: str
    reference_windows: int
    shadow_windows: int
    matched: int
    mismatches: list[cmp.Mismatch]
    passed: bool


def _window_start_ms(window_start: str) -> int:
    """The generator's CEST wall-clock 'YYYY-MM-DDTHH:MM' -> epoch milliseconds."""
    zone = timezone(timedelta(hours=config.TRADE_TIME_UTC_OFFSET_HOURS))
    wall_clock = datetime.strptime(window_start, "%Y-%m-%dT%H:%M")
    return int(wall_clock.replace(tzinfo=zone).timestamp()) * 1000


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"reference file {path} not found; create it with"
            f" `make generate-reference CSV=...`") from None


def load_reference(reference_dir) -> Reference:
    """Parse a generator output directory (ema_windows.csv + summary.json).

    Raises FileNotFoundError when either file is missing and ValueError when
    the generator ran with an EMA init or watermark delay the shadow does
    not model.
    """
    reference_dir = Path(reference_dir)
    summary = json.loads(_read(reference_dir / SUMMARY_JSON))
    spec = summary["spec"]
    if spec["ema_init"] != config.EMA_INIT:
        raise ValueError(f"reference ema_init {spec['ema_init']!r} differs from the"
                         f" shadow's {config.EMA_INIT!r}; regenerate it with the default")
    delay = spec["out_of_orderness_ms"]
    if delay is not None and delay != config.OUT_OF_ORDERNESS_MS:
        raise ValueError(f"reference out_of_orderness_ms {delay} differs from the"
                         f" processor's {config.OUT_OF_ORDERNESS_MS}; regenerate it with the default")
    ema_text = _read(reference_dir / EMA_CSV)
    windows = {}
    for row in csv.DictReader(ema_text.splitlines()):
        windows[(row["symbol"], _window_start_ms(row["window_start"]))] = ReferenceWindow(
            int(row["event_count"]), int(row["close_micros"]),
            row["ema38_micros"], row["ema100_micros"],
            row["prev_ema38_micros"], row["prev_ema100_micros"], row["advisory"])
    return Reference(windows, delay)


def _shadow_window(result) -> ReferenceWindow:
    """A shadow WindowResult in the reference's representation (generator's EMA format)."""
    with localcontext() as ctx:
        ctx.prec = config.DECIMAL_PREC
        return ReferenceWindow(
            result.event_count, result.close_micros,
            gvd.fmt_ema(result.ema38_micros), gvd.fmt_ema(result.ema100_micros),
            gvd.fmt_ema(result.prev_ema38_micros), gvd.fmt_ema(result.prev_ema100_micros),
            result.advisory)


def compare_reference(shadow, reference: Reference) -> ReferenceResult:
    """Join the shadow replay against the reference and list every difference."""
    side = "keep_all" if reference.out_of_orderness_ms is None else "drop_model"
    replay = shadow.keep_all if side == "keep_all" else shadow.drop_model
    topic = {(symbol, widx * WindowMs): _shadow_window(result)
             for (symbol, widx), result in replay.items()}
    mismatches, matched = [], 0
    for key in sorted(topic.keys() | reference.windows.keys()):
        if key not in reference.windows:
            parts = (NOT_IN_REFERENCE,)
        elif key not in topic:
            parts = (MISSING_FROM_TOPIC,)
        else:
            parts = tuple(f"{name} {actual} != {expected}"
                          for name, actual, expected
                          in zip(ReferenceWindow._fields, topic[key], reference.windows[key])
                          if actual != expected)
        if parts:
            mismatches.append(cmp.Mismatch(key[0], key[1], parts))
        else:
            matched += 1
    return ReferenceResult(side, len(reference.windows), len(topic), matched,
                           mismatches, not mismatches)


def reference_report(result: ReferenceResult, reference_dir) -> dict:
    """JSON-able summary of one reference comparison."""
    return {
        "dir": str(reference_dir),
        "side": result.side,
        "reference_windows": result.reference_windows,
        "shadow_windows": result.shadow_windows,
        "matched": result.matched,
        "mismatches": cmp._mismatch_entry(result.mismatches),
        "passed": result.passed,
    }
