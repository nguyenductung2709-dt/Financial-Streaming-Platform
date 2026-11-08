#!/usr/bin/env python3
"""Report how long the pipeline takes to produce each EMA window and trade advisory.

Run it after a flow has settled (measure-flow does): the topics persist until
reset-flow, so it also works on a run that has already finished. The Flink job
must have been started with --latency-topic latency-stamps.

Each window is followed through these wall-clock moments (epoch milliseconds):

  t0   the producer published the event that let the window close (the first
       event in trade-events whose trade_time reaches window_end + the
       watermark's out-of-orderness allowance), rebuilt from trade-events
  t1   Flink fired the window            } stamps published to latency-stamps
  t2   Flink computed the EMA            }
  t3   Flink evaluated the crossover     }
  t4e  the EMA landed on ema-windows          the topics' broker append time,
  t4a  the advisory landed on trade-advisories  so they must be LogAppendTime topics

and the stages derived from them are in STAGES. Writes latency.csv (one row per
window) and latency-summary.csv (count, mean, p50, p95, p99, max per stage) to
--out.
"""

import collections
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
import config  # noqa: E402

STAGES = ("ingest_to_fire", "ema_compute", "advisory_compute", "ema_publish",
          "advisory_publish", "end_to_end_ema", "end_to_end_advisory")
SUMMARY_KEYS = ("count", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")

# One stamp from latency-stamps; stage is "fired", "ema" or "crossover".
Stamp = collections.namedtuple("Stamp", "symbol window_end stage at advisory_emitted")


class LatencyError(RuntimeError):
    """The topics cannot be turned into a trustworthy latency report."""


def trigger_times(events, window_ms, out_of_orderness_ms):
    """{window_end_ms: publish_ms} of the event that let each window fire.

    `events` is an iterable of (trade_time_ms, publish_ms) in log order. Flink's
    watermark is the highest trade time seen minus the allowance, and a window
    ending at `e` fires once the highest trade time reaches `e + allowance`.
    Late events still raise the maximum when they exceed it, and never lower it.
    The first event triggers nothing: windows it would close have no events.
    """
    triggers = {}
    highest = None
    for trade_time, published in events:
        if highest is not None and trade_time > highest:
            end = (highest - out_of_orderness_ms) // window_ms * window_ms + window_ms
            limit = trade_time - out_of_orderness_ms
            while end <= limit:
                triggers[end] = published
                end += window_ms
        if highest is None or trade_time > highest:
            highest = trade_time
    return triggers


def percentile(sorted_values, q):
    """The q-th percentile (0..100), linearly interpolated between closest ranks."""
    rank = (len(sorted_values) - 1) * q / 100
    low = math.floor(rank)
    high = min(low + 1, len(sorted_values) - 1)
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


def summarize(values):
    ordered = sorted(values)
    if not ordered:
        return dict.fromkeys(SUMMARY_KEYS) | {"count": 0}
    return {"count": len(ordered), "mean_ms": sum(ordered) / len(ordered),
            "p50_ms": percentile(ordered, 50), "p95_ms": percentile(ordered, 95),
            "p99_ms": percentile(ordered, 99), "max_ms": ordered[-1]}


def summarize_rows(rows):
    """One {"stage": name, **summary} per stage, in STAGES order; empty stages have count 0."""
    return [{"stage": stage, **summarize([row[stage] for row in rows if row[stage] is not None])}
            for stage in STAGES]


def first_landings(landed):
    """({(symbol, window_end): landed_ms} keeping the first of each, duplicates dropped)."""
    first, duplicates = {}, 0
    for symbol, window_end, landed_ms in landed:
        if (symbol, window_end) in first:
            duplicates += 1
        else:
            first[(symbol, window_end)] = landed_ms
    return first, duplicates


def build_rows(stamps, ema_landed, advisory_landed, triggers):
    """Join stamps, landing times and trigger times into one row per measured window.

    ema_landed / advisory_landed are iterables of (symbol, window_end_ms, landed_ms).
    A window is measured when it has all three stamps, an EMA landing and a trigger;
    flushed when it has everything but a trigger (bounded runs flush their last
    windows); missing_stage when anything else is absent, including the landing of
    an advisory its crossover stamp announced. Duplicates (at-least-once delivery)
    keep the first occurrence. Returns (rows, coverage).
    """
    duplicates = 0
    by_window = {}
    for stamp in stamps:
        slots = by_window.setdefault((stamp.symbol, stamp.window_end), {})
        if stamp.stage in slots:
            duplicates += 1
        else:
            slots[stamp.stage] = stamp
    emas, extra = first_landings(ema_landed)
    advisories, more = first_landings(advisory_landed)
    duplicates += extra + more

    rows, flushed, missing = [], 0, 0
    for key in set(by_window) | set(emas):
        symbol, window_end = key
        slots = by_window.get(key, {})
        if key not in emas or not all(stage in slots for stage in ("fired", "ema", "crossover")):
            missing += 1
            continue
        emitted = slots["crossover"].advisory_emitted
        if emitted and key not in advisories:
            missing += 1
            continue
        t1, t2, t3 = slots["fired"].at, slots["ema"].at, slots["crossover"].at
        t4e = emas[key]
        t4a = advisories[key] if emitted else None
        if t4e < t2 or (t4a is not None and t4a < t3):
            raise LatencyError(
                f"{symbol} window ending {window_end}: a result landed on its topic before the "
                "processor produced it. ema-windows and trade-advisories must be created with "
                "message.timestamp.type=LogAppendTime; run `make reset-flow` to recreate the "
                "topics, then repeat the run.")
        if window_end not in triggers:
            flushed += 1
            continue
        t0 = triggers[window_end]
        rows.append({
            "symbol": symbol, "window_end_ms": window_end,
            "t0_ms": t0, "t1_ms": t1, "t2_ms": t2, "t3_ms": t3, "t4e_ms": t4e,
            "advisory_emitted": emitted, "t4a_ms": t4a,
            "ingest_to_fire": t1 - t0,
            "ema_compute": t2 - t1,
            "advisory_compute": t3 - t2,
            "ema_publish": t4e - t2,
            "advisory_publish": t4a - t3 if emitted else None,
            "end_to_end_ema": t4e - t0,
            "end_to_end_advisory": t4a - t0 if emitted else None,
        })
    rows.sort(key=lambda row: (row["window_end_ms"], row["symbol"]))
    coverage = {
        "measured": len(rows), "flushed": flushed, "missing_stage": missing,
        "duplicates": duplicates,
        "advisories": sum(1 for row in rows if row["advisory_emitted"]),
        "advisory_stamps": sum(1 for slots in by_window.values()
                               if "crossover" in slots and slots["crossover"].advisory_emitted),
        "advisory_records": len(advisories),
    }
    return rows, coverage
