#!/usr/bin/env python3
"""Shadow replay of a trade-event stream: keep-all, drop model, at-risk set.

Recomputes what the Flink processor should have emitted from a stream of
price events, in arrival order, so Tasks 5-6 can join it against the
actual `ema-windows`/`trade-advisories` topics.  Spec:
docs/superpowers/plans/2026-10-30-full-flow-validation.md.

Watermark model (PDF 3.1; the design's per-event idealization is an upper
  bound on Flink's periodic watermarking):
  - The watermark is GLOBAL across symbols (single partition, parallelism
    1): max trade time seen so far over all events minus OutOfOrdernessMs
    and Flink's extra 1 ms (BoundedOutOfOrdernessWatermarks emits
    `maxTimestamp - delay - 1`).
  - Watermarks are generated at the source, before the window operator:
    the running max is updated from every event BEFORE its drop test, so
    an event the model drops still advances the watermark (an event that
    raises the max is never itself late: its window's last timestamp is
    at or after it).
  - Drop rule: is_late (shared with the CSV generator) - an event is
    dropped iff its window's maxTimestamp is <= the watermark plus any
    allowed lateness (note the `<=`: exactly at the watermark is late,
    one ms above is kept).
  - At-risk: the drop rule's victims.  A window in which the model
    drops at least one event may mismatch the keep-all shadow for
    watermark reasons (periodic watermarking can only drop fewer),
    and so may any later window of the same symbol: a drop shifts
    the prev-pair EMA state that cascades through the replay.
    compare.py drives its lateness_related class from the wider
    keep-all/drop-model row divergence set, which covers both the
    directly-hit windows and their cascades.  at_risk_windows reports
    only the directly-hit windows; at_risk_events counts the dropped
    events.

Partitions (a topic with several partitions, Flink parallelism to match):
  Flink keeps one watermark per partition (its max trade time - delay - 1)
  and the job's watermark is the minimum over the partitions.  Records keep
  their order within a partition, but Flink reads partitions concurrently,
  so the interleaving across partitions is not fixed.  The drop model
  therefore tests each event against its OWN partition's running max: the
  job watermark can never be ahead of it, so this is a superset of what
  Flink can drop, and actual stays between keep-all and the drop model as
  the classification expects.  With one partition it is exactly the global
  rule above.  final_watermark_max_tt_ms is the minimum over the partitions
  of their max trade time, the value Flink's final watermark derives from.

Memory is O(windows): per-(symbol, window) close states and window keys
are accumulated in a single pass; events are never retained.
"""

from typing import Iterable, NamedTuple

from ema_replay import (
    CloseState,
    OutOfOrdernessMs,
    WindowMs,
    WindowResult,
    is_late,
    replay,
    update_close,
)


class ShadowEvent(NamedTuple):
    """One price event as it arrived on the stream (arrival order matters
    within a partition; order breaks close ties within a symbol, whose
    events all share one partition)."""

    symbol: str
    tt_ms: int
    micros: int
    order: int
    partition: int = 0


class ShadowOutput(NamedTuple):
    """Replay results keyed by (symbol, window_index).

    keep_all:       every event folded in.
    drop_model:     only surviving events (per-event watermark idealization).
    at_risk_windows: windows the drop model dropped at least one event
                     for; at_risk_events counts those dropped events
                     (expected zero on near-chronological data).
    final_max_tt_ms: max event time over all events (None for zero events).
    final_watermark_max_tt_ms: min over the partitions of their max event
                     time, from which the final watermark follows (None for
                     zero events); equals final_max_tt_ms with one partition.
    """

    keep_all: dict[tuple[str, int], WindowResult]
    drop_model: dict[tuple[str, int], WindowResult]
    at_risk_windows: frozenset[tuple[str, int]]
    at_risk_events: int
    total_events: int
    final_max_tt_ms: int | None
    final_watermark_max_tt_ms: int | None = None


def shadow_replay(events: Iterable[ShadowEvent]) -> ShadowOutput:
    """One pass in arrival order -> keep-all and drop-model replays + at-risk.

    The running max (over ALL events, watermark generation at the source)
    is updated before each event's drop test, so an event the model drops
    still advances the watermark (an event that raises the max is never
    itself late: its window's last timestamp is at or after it);
    keep-all accumulates every event, the drop model only survivors;
    at-risk tracks the windows the drop test rejected events for.
    """
    keep_closes: dict[tuple[str, int], CloseState] = {}
    drop_closes: dict[tuple[str, int], CloseState] = {}
    dropped: dict[tuple[str, int], int] = {}
    total_events = 0
    max_tt: int | None = None
    partition_max: dict[int, int] = {}
    for ev in events:
        total_events += 1
        if max_tt is None or ev.tt_ms > max_tt:
            max_tt = ev.tt_ms
        own_max = partition_max.get(ev.partition)
        if own_max is None or ev.tt_ms > own_max:
            own_max = partition_max[ev.partition] = ev.tt_ms
        key = (ev.symbol, ev.tt_ms // WindowMs)
        keep_closes[key] = update_close(
            keep_closes.get(key), ev.tt_ms, ev.micros, ev.order)
        if not is_late(ev.tt_ms, own_max, OutOfOrdernessMs):
            drop_closes[key] = update_close(
                drop_closes.get(key), ev.tt_ms, ev.micros, ev.order)
        else:
            dropped[key] = dropped.get(key, 0) + 1
    keep_all = {(r.symbol, r.window_index): r for r in replay(keep_closes)}
    drop_model = {(r.symbol, r.window_index): r for r in replay(drop_closes)}
    at_risk_windows = frozenset(dropped)
    at_risk_events = sum(dropped.values())
    watermark_max = min(partition_max.values()) if partition_max else None
    return ShadowOutput(keep_all, drop_model, at_risk_windows,
                        at_risk_events, total_events, max_tt, watermark_max)
