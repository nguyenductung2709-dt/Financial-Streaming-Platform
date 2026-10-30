#!/usr/bin/env python3
"""Shadow replay of a trade-event stream: keep-all, drop model, at-risk set.

Recomputes what the Flink processor should have emitted from a stream of
price events, in arrival order, so Tasks 5-6 can join it against the
actual `ema-windows`/`trade-advisories` topics.  Spec:
docs/superpowers/plans/2026-10-30-full-flow-validation.md.

Watermark model (PDF 3.1; the design's per-event idealization is an upper
bound on Flink's periodic watermarking):
  - The watermark is GLOBAL across symbols (single partition, parallelism
    1): max trade time seen so far over all events minus OUT_OF_ORDERNESS.
  - Watermarks are generated at the source, before the window operator:
    the running max is updated from every event BEFORE its drop test, so
    an event the model drops still advances the watermark.
  - Drop rule: an event is dropped iff its window's maxTimestamp
    `(tt_ms // WindowMs + 1) * WindowMs - 1` is <= running max - 70_000
    (note the `<=`: exactly at the watermark is late).
  - At-risk: the drop rule's victims.  A window in which the model
    drops at least one event may mismatch the keep-all shadow for
    watermark reasons (periodic watermarking can only drop fewer);
    any other window cannot, so a mismatch there is a definite
    failure.  at_risk_events counts the dropped events.

Memory is O(windows): per-(symbol, window) close states and window keys
are accumulated in a single pass; events are never retained.
"""

from typing import Iterable, NamedTuple

from ema_replay import (
    CloseState,
    WindowMs,
    WindowResult,
    replay,
    update_close,
)

OutOfOrdernessMs = 70_000  # event-time lateness bound (watermark lag)


class ShadowEvent(NamedTuple):
    """One price event as it arrived on the stream (arrival order matters)."""

    symbol: str
    tt_ms: int
    micros: int
    order: int


class ShadowOutput(NamedTuple):
    """Replay results keyed by (symbol, window_index).

    keep_all:       every event folded in.
    drop_model:     only surviving events (per-event watermark idealization).
    at_risk_windows: windows the drop model dropped at least one event
                     for; at_risk_events counts those dropped events
                     (expected zero on near-chronological data).
    final_max_tt_ms: max event time over all events (None for zero events).
    """

    keep_all: dict[tuple[str, int], WindowResult]
    drop_model: dict[tuple[str, int], WindowResult]
    at_risk_windows: frozenset[tuple[str, int]]
    at_risk_events: int
    total_events: int
    final_max_tt_ms: int | None


def _window_max_tt(widx: int) -> int:
    """A window's maxTimestamp: the last event time it can accept."""
    return (widx + 1) * WindowMs - 1


def shadow_replay(events: Iterable[ShadowEvent]) -> ShadowOutput:
    """One pass in arrival order -> keep-all and drop-model replays + at-risk.

    The running max (over ALL events, watermark generation at the source)
    is updated before each event's drop test; keep-all accumulates every
    event, the drop model only survivors; at-risk tracks the windows
    the drop test rejected events for.
    """
    keep_closes: dict[tuple[str, int], CloseState] = {}
    drop_closes: dict[tuple[str, int], CloseState] = {}
    dropped: dict[tuple[str, int], int] = {}
    total_events = 0
    max_tt: int | None = None
    for ev in events:
        total_events += 1
        if max_tt is None or ev.tt_ms > max_tt:
            max_tt = ev.tt_ms
        key = (ev.symbol, ev.tt_ms // WindowMs)
        keep_closes[key] = update_close(
            keep_closes.get(key), ev.tt_ms, ev.micros, ev.order)
        if _window_max_tt(key[1]) > max_tt - OutOfOrdernessMs:
            drop_closes[key] = update_close(
                drop_closes.get(key), ev.tt_ms, ev.micros, ev.order)
        else:
            dropped[key] = dropped.get(key, 0) + 1
    keep_all = {(r.symbol, r.window_index): r for r in replay(keep_closes)}
    drop_model = {(r.symbol, r.window_index): r for r in replay(drop_closes)}
    at_risk_windows = frozenset(dropped)
    at_risk_events = sum(dropped.values())
    return ShadowOutput(keep_all, drop_model, at_risk_windows,
                        at_risk_events, total_events, max_tt)
