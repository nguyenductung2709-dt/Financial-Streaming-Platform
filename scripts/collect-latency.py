#!/usr/bin/env python3
"""Report how long the pipeline takes to produce each EMA window and trade advisory.

Run it after a flow has settled (measure-flow does): the topics persist until
reset-flow, so it also works on a run that has already finished. The Flink job
must have been started with --latency-topic latency-stamps, and the materializer
with MATERIALIZER_STAMP_TOPIC for the database stages (measure-flow does both).

Each window is followed through these wall-clock moments (epoch milliseconds):

  tw   the stream reached window_end: the producer published the first event in
       trade-events whose trade_time reaches window_end, rebuilt from trade-events
  t0   the producer published the event that let the window close (the first
       event in trade-events whose trade_time reaches window_end + the
       watermark's out-of-orderness allowance), rebuilt from trade-events
  t1   Flink fired the window            } stamps published to latency-stamps
  t2   Flink computed the EMA            }
  t3   Flink evaluated the crossover     }
  t4e  the EMA landed on ema-windows          the topics' broker append time,
  t4a  the advisory landed on trade-advisories  so they must be LogAppendTime topics
  t5e  the EMA was committed to PostgreSQL  } stamps published to
  t5a  the advisory was committed            } materialize-stamps

and the stages derived from them are in STAGES. The end_to_end_* stages start at
t0 and leave out the wait for the out-of-orderness allowance; the window_end_to_*
stages start at tw and include it, like a latency counted from the window's end.
Writes latency.csv (one row per window) and latency-summary.csv (count, mean, p50,
p95, p99, max per stage) to --out.
"""

import argparse
import collections
import csv
import heapq
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
import config  # noqa: E402

STAGES = ("ingest_to_fire", "ema_compute", "advisory_compute", "ema_publish",
          "advisory_publish", "end_to_end_ema", "end_to_end_advisory",
          "ema_materialize", "advisory_materialize",
          "end_to_end_ema_materialized", "end_to_end_advisory_materialized",
          "watermark_wait", "window_end_to_ema", "window_end_to_ema_published",
          "window_end_to_advisory_published", "window_end_to_ema_materialized",
          "window_end_to_advisory_materialized")
SUMMARY_KEYS = ("count", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")
ROW_COLUMNS = ("symbol", "window_end_ms", "tw_ms", "t0_ms", "t1_ms", "t2_ms", "t3_ms", "t4e_ms",
               "advisory_emitted", "t4a_ms", "t5e_ms", "t5a_ms", *STAGES)

# One stamp from latency-stamps; stage is "fired", "ema" or "crossover".
Stamp = collections.namedtuple("Stamp", "symbol window_end stage at advisory_emitted")


class LatencyError(RuntimeError):
    """The topics cannot be turned into a trustworthy latency report."""


def crossing_times(events, window_ms, allowances, partitions=1):
    """One {window_end_ms: publish_ms} per allowance, in one pass over `events`.

    `events` is an iterable of (trade_time_ms, publish_ms) or
    (trade_time_ms, publish_ms, partition) in publish order; pairs are
    partition 0. Flink keeps one watermark per partition (its highest trade time
    minus the out-of-orderness allowance) and the job's watermark is the
    minimum over the `partitions` partitions, defined once each has had an
    event. For each allowance, the event recorded for a window ending at `e` is
    the first that takes that minimum to `e + allowance` or beyond: with the
    out-of-orderness allowance the event that let the window fire, with 0 the
    event at which the stream reached the window's end. With one partition the
    minimum is simply the highest trade time seen. Late events raise their
    partition's maximum when they exceed it, and never lower it. The event that
    first defines the minimum records nothing: windows it would cross have no
    events.
    """
    crossings = [{} for _ in allowances]
    partition_max = {}
    base = None
    for event in events:
        trade_time, published, partition = (*event, 0) if len(event) == 2 else event
        if trade_time <= partition_max.get(partition, trade_time - 1):
            continue
        partition_max[partition] = trade_time
        if len(partition_max) < partitions:
            continue
        new_base = min(partition_max.values())
        if base is not None and new_base > base:
            for allowance, crossed in zip(allowances, crossings):
                end = (base - allowance) // window_ms * window_ms + window_ms
                limit = new_base - allowance
                while end <= limit:
                    crossed[end] = published
                    end += window_ms
        base = new_base
    return crossings


def trigger_times(events, window_ms, out_of_orderness_ms):
    """{window_end_ms: publish_ms} of the event that let each window fire."""
    return crossing_times(events, window_ms, (out_of_orderness_ms,))[0]


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


def elapsed(start, end):
    """end - start, or None when either moment is unknown."""
    return end - start if start is not None and end is not None else None


def first_landings(landed):
    """({(symbol, window_end): landed_ms} keeping the first of each, duplicates dropped)."""
    first, duplicates = {}, 0
    for symbol, window_end, landed_ms in landed:
        if (symbol, window_end) in first:
            duplicates += 1
        else:
            first[(symbol, window_end)] = landed_ms
    return first, duplicates


def build_rows(stamps, ema_landed, advisory_landed, triggers, materialized=None,
               window_ends=None):
    """Join stamps, landing times and trigger times into one row per measured window.

    ema_landed / advisory_landed are iterables of (symbol, window_end_ms, landed_ms).
    A window is measured when it has all three stamps, an EMA landing and a trigger;
    flushed when it has everything but a trigger (bounded runs flush their last
    windows); missing_stage when anything else is absent, including the landing of
    an advisory its crossover stamp announced. Duplicates (at-least-once delivery)
    keep the first occurrence. materialized is None when the run has no
    materialize stamps (the database stages stay empty), else (ema, advisory)
    stamp triples shaped like ema_landed; a measured window without one counts
    in coverage["missing_materialize"]. window_ends is {window_end_ms: tw} (the
    moment the stream reached each window's end); without it the window_end_to_*
    stages stay empty. Returns (rows, coverage).
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
        # The stream reached window_end no later than t0. tw is missing only for a window that
        # lies wholly behind the first event (filled by late events); its window_end_to_* stay empty.
        tw = window_ends.get(window_end) if window_ends is not None else None
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
            "tw_ms": tw, "t0_ms": t0, "t1_ms": t1, "t2_ms": t2, "t3_ms": t3, "t4e_ms": t4e,
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
            "watermark_wait": elapsed(tw, t0),
            "window_end_to_ema": elapsed(tw, t2),
            "window_end_to_ema_published": elapsed(tw, t4e),
            "window_end_to_advisory_published": elapsed(tw, t4a),
            "window_end_to_ema_materialized": elapsed(tw, t5e),
            "window_end_to_advisory_materialized": elapsed(tw, t5a),
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


def read_run(brokers, stream=None, topic_partitions=None, stream_partition=None):
    """(stamps, ema_landed, advisory_landed, materialized, triggers, window_ends) for build_rows.

    `stream(brokers, topic)` yields (offset, value, timestamp_ms) like
    kafka_topics.stream_records and `topic_partitions` is
    kafka_topics.topic_partitions (both the defaults). materialized is None
    without materialize stamps. trade-events, by far the largest topic, is
    streamed straight into the trigger scan and never held in memory; it is read
    last, after the cheap checks, so an empty latency-stamps topic fails at once.
    """
    import bindings
    import kafka_topics

    stream = stream or kafka_topics.stream_records
    topic_partitions = topic_partitions or kafka_topics.topic_partitions
    stream_partition = stream_partition or kafka_topics.stream_partition
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

    event_partitions = sorted(topic_partitions(brokers, config.TOPIC_EVENTS)) or [0]

    def published_events():
        if len(event_partitions) == 1:
            for _, value, published in stream(brokers, config.TOPIC_EVENTS):
                yield timestamp_ms(bindings.PriceEvent.FromString(value).trade_time), published
            return
        # One producer publishes in CSV order, so merging the partitions by
        # source row restores the publish order across them.
        def decoded(partition):
            for _, value, published in stream_partition(brokers, config.TOPIC_EVENTS, partition):
                event = bindings.PriceEvent.FromString(value)
                yield event.source_row, timestamp_ms(event.trade_time), published, partition
        for _, trade_time, published, partition in heapq.merge(
                *(decoded(p) for p in event_partitions)):
            yield trade_time, published, partition

    window_ends, triggers = crossing_times(published_events(), config.WINDOW_MS,
                                           (0, config.OUT_OF_ORDERNESS_MS),
                                           partitions=len(event_partitions))
    return stamps, ema_landed, advisory_landed, materialized, triggers, window_ends


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
    stamps, ema_landed, advisory_landed, materialized, triggers, window_ends = read_run(
        brokers, stream, topic_partitions)
    rows, coverage = build_rows(stamps, ema_landed, advisory_landed, triggers, materialized,
                                window_ends)
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
