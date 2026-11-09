#!/usr/bin/env python3
"""Report how long the pipeline takes to produce each EMA window and trade advisory.

Run it after a flow has settled (measure-flow does): the topics persist until
reset-flow, so it also works on a run that has already finished. The Flink job
must have been started with --latency-topic latency-stamps, and the materializer
with MATERIALIZER_STAMP_TOPIC materialize-stamps for the database stages
(measure-flow does both; without materialize stamps those stages stay empty).

Each window is followed through these wall-clock moments (epoch milliseconds):

  t0   the producer published the event that let the window close (the first
       event in trade-events whose trade_time reaches window_end + the
       watermark's out-of-orderness allowance), rebuilt from trade-events
  t1   Flink fired the window            } stamps published to latency-stamps
  t2   Flink computed the EMA            }
  t3   Flink evaluated the crossover     }
  t4e  the EMA landed on ema-windows          the topics' broker append time,
  t4a  the advisory landed on trade-advisories  so they must be LogAppendTime topics
  t5e  the EMA was committed to PostgreSQL  } stamps published to
  t5a  the advisory was committed            } materialize-stamps by the materializer

and the stages derived from them are in STAGES: end_to_end_* ends when the
result lands on its output topic; the *_materialized stages extend that
boundary through the materializer to the PostgreSQL commit. Writes latency.csv
(one row per window) and latency-summary.csv (count, mean, p50, p95, p99, max
per stage) to --out.
"""

import argparse
import collections
import csv
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
import config  # noqa: E402

STAGES = ("ingest_to_fire", "ema_compute", "advisory_compute", "ema_publish",
          "advisory_publish", "end_to_end_ema", "end_to_end_advisory",
          "ema_materialize", "advisory_materialize",
          "end_to_end_ema_materialized", "end_to_end_advisory_materialized")
SUMMARY_KEYS = ("count", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")
ROW_COLUMNS = ("symbol", "window_end_ms", "t0_ms", "t1_ms", "t2_ms", "t3_ms", "t4e_ms",
               "advisory_emitted", "t4a_ms", "t5e_ms", "t5a_ms", *STAGES)

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


def build_rows(stamps, ema_landed, advisory_landed, triggers, materialized=None):
    """Join stamps, landing times, trigger times and materialize stamps into one row per measured window.

    ema_landed / advisory_landed are iterables of (symbol, window_end_ms, landed_ms).
    A window is measured when it has all three stamps, an EMA landing and a trigger;
    flushed when it has everything but a trigger (bounded runs flush their last
    windows); missing_stage when anything else is absent, including the landing of
    an advisory its crossover stamp announced. Duplicates (at-least-once delivery)
    keep the first occurrence.

    materialized is None when the run holds no materialize stamps — the
    materializer ran without MATERIALIZER_STAMP_TOPIC, so the database stages
    stay empty — or (ema_stamps, advisory_stamps) triples shaped like
    ema_landed, published by the materializer after its PostgreSQL commit.
    Then a measured window without a stamp keeps its other stages and counts
    in coverage["missing_materialize"]. Returns (rows, coverage).
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
    materialized_emas = materialized_advisories = None
    if materialized is not None:
        materialized_emas, extra = first_landings(materialized[0])
        duplicates += extra
        materialized_advisories, extra = first_landings(materialized[1])
        duplicates += extra

    rows, flushed, missing, missing_materialize = [], 0, 0, 0
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
        t5e = t5a = None
        if materialized_emas is not None:
            t5e = materialized_emas.get(key)
            if t5e is None:
                missing_materialize += 1
            elif t5e < t4e:
                raise LatencyError(
                    f"{symbol} window ending {window_end}: a result was stamped as materialized "
                    "before it landed on its topic. The materializer can only stamp records it "
                    "consumed, so this is a clock or configuration problem.")
            if emitted:
                t5a = materialized_advisories.get(key)
                if t5a is None:
                    missing_materialize += 1
                elif t5a < t4a:
                    raise LatencyError(
                        f"{symbol} window ending {window_end}: an advisory was stamped as "
                        "materialized before it landed on its topic.")
        rows.append({
            "symbol": symbol, "window_end_ms": window_end,
            "t0_ms": t0, "t1_ms": t1, "t2_ms": t2, "t3_ms": t3, "t4e_ms": t4e,
            "advisory_emitted": emitted, "t4a_ms": t4a, "t5e_ms": t5e, "t5a_ms": t5a,
            "ingest_to_fire": t1 - t0,
            "ema_compute": t2 - t1,
            "advisory_compute": t3 - t2,
            "ema_publish": t4e - t2,
            "advisory_publish": t4a - t3 if emitted else None,
            "end_to_end_ema": t4e - t0,
            "end_to_end_advisory": t4a - t0 if emitted else None,
            "ema_materialize": t5e - t4e if t5e is not None else None,
            "advisory_materialize": t5a - t4a if t5a is not None else None,
            "end_to_end_ema_materialized": t5e - t0 if t5e is not None else None,
            "end_to_end_advisory_materialized": t5a - t0 if t5a is not None else None,
        })
    rows.sort(key=lambda row: (row["window_end_ms"], row["symbol"]))
    coverage = {
        "measured": len(rows), "flushed": flushed, "missing_stage": missing,
        "duplicates": duplicates,
        "materialized": materialized is not None,
        "missing_materialize": missing_materialize,
        "advisories": sum(1 for row in rows if row["advisory_emitted"]),
        "advisory_stamps": sum(1 for slots in by_window.values()
                               if "crossover" in slots and slots["crossover"].advisory_emitted),
        "advisory_records": len(advisories),
    }
    return rows, coverage


def timestamp_ms(timestamp):
    """google.protobuf.Timestamp -> epoch milliseconds."""
    return timestamp.seconds * 1000 + timestamp.nanos // 1_000_000


def read_run(brokers, stream=None, topic_partitions=None):
    """(stamps, ema_landed, advisory_landed, materialized, triggers) for build_rows, read from the topics.

    `stream(brokers, topic)` yields (offset, value, timestamp_ms) like
    kafka_topics.stream_records, and `topic_partitions(brokers, topic)` reports
    the partitions of a topic (both the defaults). materialized is None when
    the materialize-stamps topic is absent or empty — no materializer stamps —
    or the (ema, advisory) stamp triples otherwise. trade-events, by far the
    largest topic, is streamed straight into the trigger scan and never held in
    memory; it is read last, after the cheap checks, so an empty latency-stamps
    topic fails at once.
    """
    import bindings
    import kafka_topics

    stream = stream or kafka_topics.stream_records
    topic_partitions = topic_partitions or kafka_topics.topic_partitions
    stage_names = {bindings.STAGE_WINDOW_FIRED: "fired", bindings.STAGE_EMA_COMPUTED: "ema",
                   bindings.STAGE_CROSSOVER_EVALUATED: "crossover"}
    stamps = []
    for _, value, _ in stream(brokers, config.TOPIC_LATENCY_STAMPS):
        message = bindings.StageStamp.FromString(value)
        if message.stage not in stage_names:
            raise LatencyError(f"{config.TOPIC_LATENCY_STAMPS} holds a stamp with the "
                               f"unrecognized stage {message.stage}")
        stamps.append(Stamp(message.symbol, timestamp_ms(message.window_end),
                            stage_names[message.stage], timestamp_ms(message.at),
                            message.advisory_emitted))
    if not stamps:
        raise LatencyError(
            f"{config.TOPIC_LATENCY_STAMPS} is empty. Was the Flink job started with "
            f"--latency-topic {config.TOPIC_LATENCY_STAMPS}? (measure-flow does this itself.)")

    ema_landed = []
    for _, value, landed in stream(brokers, config.TOPIC_EMA_WINDOWS):
        window = bindings.EmaWindow.FromString(value)
        ema_landed.append((window.symbol, timestamp_ms(window.window_end), landed))
    advisory_landed = []
    for _, value, landed in stream(brokers, config.TOPIC_TRADE_ADVISORIES):
        window = bindings.TradeAdvisory.FromString(value).ema_window
        advisory_landed.append((window.symbol, timestamp_ms(window.window_end), landed))

    materialized = None
    if topic_partitions(brokers, config.TOPIC_MATERIALIZE_STAMPS):
        stage_targets = {bindings.STAGE_EMA_MATERIALIZED: [], bindings.STAGE_ADVISORY_MATERIALIZED: []}
        for _, value, _ in stream(brokers, config.TOPIC_MATERIALIZE_STAMPS):
            message = bindings.StageStamp.FromString(value)
            target = stage_targets.get(message.stage)
            if target is None:
                raise LatencyError(f"{config.TOPIC_MATERIALIZE_STAMPS} holds a stamp with the "
                                   f"unrecognized stage {message.stage}")
            target.append((message.symbol, timestamp_ms(message.window_end), timestamp_ms(message.at)))
        if any(stage_targets.values()):
            materialized = (stage_targets[bindings.STAGE_EMA_MATERIALIZED],
                            stage_targets[bindings.STAGE_ADVISORY_MATERIALIZED])

    def published_events():
        for _, value, published in stream(brokers, config.TOPIC_EVENTS):
            yield timestamp_ms(bindings.PriceEvent.FromString(value).trade_time), published

    triggers = trigger_times(published_events(), config.WINDOW_MS, config.OUT_OF_ORDERNESS_MS)
    return stamps, ema_landed, advisory_landed, materialized, triggers


def cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return str(round(value, 3))
    return str(value)


def format_table(summary, coverage):
    """The stage table and a coverage footer, for the console and summary.txt."""
    def number(value):
        return "-" if value is None else (f"{value:.1f}" if isinstance(value, float) else str(value))

    stage_width = max([len("stage")] + [len(stage) for stage in STAGES])
    lines = ["Latency (ms)",
             f"{'stage':<{stage_width}}{'count':>9}" + "".join(f"{name:>10}" for name in
                                                      ("mean", "p50", "p95", "p99", "max"))]
    for item in summary:
        lines.append(f"{item['stage']:<{stage_width}}{item['count']:>9}" + "".join(
            f"{number(item[key]):>10}" for key in SUMMARY_KEYS[1:]))
    footer = (f"windows: measured {coverage['measured']}, flushed {coverage['flushed']}, "
              f"missing a stage {coverage['missing_stage']}, duplicates {coverage['duplicates']}")
    if coverage["materialized"]:
        footer += f", missing a materialize stamp {coverage['missing_materialize']}"
    footer += f"; advisories {coverage['advisories']}"
    lines.append(footer)
    if not coverage["materialized"]:
        lines.append(
            "no materialize stamps: the database stages are empty (the materializer ran "
            "without MATERIALIZER_STAMP_TOPIC; measure-flow sets it)")
    if coverage["advisory_stamps"] != coverage["advisory_records"]:
        lines.append(
            f"WARNING: advisory count mismatch: {coverage['advisory_stamps']} crossover stamps "
            f"announced an advisory but {coverage['advisory_records']} landed on "
            f"{config.TOPIC_TRADE_ADVISORIES}")
    return "\n".join(lines)


def write_outputs(out_dir, rows, summary, coverage):
    """Write latency.csv and latency-summary.csv to out_dir; returns the console table."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "latency.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(ROW_COLUMNS)
        for row in rows:
            writer.writerow([cell(row[column]) for column in ROW_COLUMNS])
    with open(out_dir / "latency-summary.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("stage", *SUMMARY_KEYS))
        for item in summary:
            writer.writerow([item["stage"], *[cell(item[key]) for key in SUMMARY_KEYS]])
    return format_table(summary, coverage)


def collect(brokers, out_dir, stream=None, topic_partitions=None):
    """Read the run, write the outputs; returns (summary, coverage, console_table)."""
    stamps, ema_landed, advisory_landed, materialized, triggers = read_run(brokers, stream, topic_partitions)
    rows, coverage = build_rows(stamps, ema_landed, advisory_landed, triggers, materialized)
    summary = summarize_rows(rows)
    return summary, coverage, write_outputs(Path(out_dir), rows, summary, coverage)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="output directory")
    parser.add_argument("--brokers", default=config.BROKERS,
                        help="Redpanda address reachable from this host (default %(default)s)")
    args = parser.parse_args(argv)
    try:
        _, _, table = collect(args.brokers, args.out)
    except (RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    print(table)
    print(f"Results in {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
