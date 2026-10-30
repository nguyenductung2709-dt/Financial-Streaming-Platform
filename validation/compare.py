#!/usr/bin/env python3
"""Actual-side decoding, comparison against the shadow, and classification.

Decodes the Flink processor's real protobuf output (`ema-windows`,
`trade-advisories`) into plain tuples, joins it against the shadow replay
on (symbol, window_start_ms), and classifies every mismatching window in
the design's order:
  match              actual equals the keep-all shadow for that window;
  lateness_explained the drop-model shadow reproduces actual exactly
                     (what Flink should emit given watermark drops);
  lateness_related   a window the drop model dropped events for
                     (at-risk), but the drop model does not reproduce;
  unexplained        anything else; `passed` iff this class is empty.

Set inequality in both directions is a mismatch: a shadow window with no
actual window (all its events dropped) and an actual window with no
shadow window (should not happen; defensive).  Malformed records (empty
or unparseable EMA strings, unspecified advisory action) become
"unparseable"/action mismatch parts instead of crashes.  Spec:
docs/superpowers/plans/2026-10-30-full-flow-validation.md.
"""

from decimal import Decimal, InvalidOperation
from typing import NamedTuple

from bindings import ADVISORY_ACTION_BUY, ADVISORY_ACTION_SELL
from ema_replay import WindowMs

MICROS = Decimal(10**6)       # shadow EMAs are micros; actuals are price strings
MAX_REPORT_MISMATCHES = 100   # per-class cap in the JSON report


class ActualEmaWindow(NamedTuple):
    """One decoded `ema-windows` record; None EMA = empty/unparseable string."""

    symbol: str
    window_start_ms: int
    closing_price_micros: int
    ema38: Decimal | None
    ema100: Decimal | None


class ActualAdvisory(NamedTuple):
    """One decoded `trade-advisories` record; action "" = unspecified/invalid."""

    symbol: str
    window_start_ms: int
    action: str
    previous_ema38: Decimal | None
    previous_ema100: Decimal | None


class Mismatch(NamedTuple):
    """One window that differs, with a human-readable string per aspect."""

    symbol: str
    window_start_ms: int
    parts: tuple[str, ...]


class CompareResult(NamedTuple):
    """Classification of every window in the actual | keep-all union."""

    total_windows: int
    matched: int
    lateness_explained: list[Mismatch]
    lateness_related: list[Mismatch]
    unexplained: list[Mismatch]
    passed: bool


def _parse_price(text: str) -> Decimal | None:
    """Decimal price string -> Decimal; None when empty/unparseable/non-finite."""
    if not text:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _start_ms(timestamp) -> int:
    """google.protobuf.Timestamp -> epoch milliseconds."""
    return timestamp.seconds * 1000 + timestamp.nanos // 1_000_000


def decode_ema_window(msg) -> ActualEmaWindow:
    """Decode an EmaWindow protobuf message."""
    return ActualEmaWindow(
        msg.symbol,
        _start_ms(msg.window_start),
        msg.closing_price_micros,
        _parse_price(msg.ema38),
        _parse_price(msg.ema100),
    )


def decode_advisory(msg) -> ActualAdvisory:
    """Decode a TradeAdvisory protobuf message (key from the embedded window)."""
    action = {ADVISORY_ACTION_BUY: "BUY",
              ADVISORY_ACTION_SELL: "SELL"}.get(msg.action, "")
    return ActualAdvisory(
        msg.ema_window.symbol,
        _start_ms(msg.ema_window.window_start),
        action,
        _parse_price(msg.previous_ema38),
        _parse_price(msg.previous_ema100),
    )


def _ema_part(name: str, actual: Decimal | None, shadow_micros: Decimal,
              tolerance: Decimal) -> str | None:
    """One mismatch part for an EMA price pair, or None when within tolerance."""
    if actual is None:
        return f"{name} unparseable"
    shadow_price = shadow_micros / MICROS
    if abs(actual - shadow_price) > tolerance:
        return f"{name} {actual} != {shadow_price}"
    return None


def _advisory_parts(actual: ActualAdvisory | None, shadow_row,
                    tolerance: Decimal) -> list[str]:
    """Mismatch parts for advisory presence, action, and previous-EMA pair."""
    if shadow_row.advisory == "":
        return [] if actual is None else ["unexpected advisory"]
    if actual is None:
        return [f"missing advisory (expected {shadow_row.advisory})"]
    parts = []
    if actual.action != shadow_row.advisory:
        parts.append(f"advisory action {actual.action!r} != {shadow_row.advisory!r}")
    for name, value, micros in (
        ("previous_ema38", actual.previous_ema38, shadow_row.prev_ema38_micros),
        ("previous_ema100", actual.previous_ema100, shadow_row.prev_ema100_micros),
    ):
        part = _ema_part(name, value, micros, tolerance)
        if part:
            parts.append(part)
    return parts


def _window_diff(window: ActualEmaWindow | None, advisory: ActualAdvisory | None,
                 shadow_row, tolerance: Decimal) -> tuple[str, ...]:
    """All mismatch parts of one window vs one shadow row (empty = identical)."""
    if shadow_row is None:
        if window is None and advisory is None:
            return ()  # both sides empty: the drop model may lack dropped windows
        return ("window unexpected in actual",)
    if window is None:
        return ("window missing from actual",)
    parts = []
    if window.closing_price_micros != shadow_row.close_micros:
        parts.append(f"closing_price_micros {window.closing_price_micros}"
                     f" != {shadow_row.close_micros}")
    for name, value, micros in (
        ("ema38", window.ema38, shadow_row.ema38_micros),
        ("ema100", window.ema100, shadow_row.ema100_micros),
    ):
        part = _ema_part(name, value, micros, tolerance)
        if part:
            parts.append(part)
    parts.extend(_advisory_parts(advisory, shadow_row, tolerance))
    return tuple(parts)


def compare(ema_actual: list[ActualEmaWindow],
            advisories_actual: list[ActualAdvisory],
            shadow, tolerance: Decimal = Decimal("1e-9")) -> CompareResult:
    """Join actual windows/advisories against the shadow and classify mismatches.

    The union is keyed over (symbol, window_start_ms); a shadow window's
    start is window_index * WindowMs.  Classification order: keep-all
    match, then drop-model reproduction (lateness_explained), then
    at-risk membership (lateness_related), else unexplained.  `passed`
    iff unexplained is empty.
    """
    windows = {(w.symbol, w.window_start_ms): w for w in ema_actual}
    advisories = {(a.symbol, a.window_start_ms): a for a in advisories_actual}
    keys = sorted(set(windows) | set(advisories)
                  | {(s, widx * WindowMs) for s, widx in shadow.keep_all})
    explained, related, unexplained = [], [], []
    matched = 0
    for key in keys:
        shadow_key = (key[0], key[1] // WindowMs)
        window = windows.get(key)
        advisory = advisories.get(key)
        parts = _window_diff(window, advisory,
                             shadow.keep_all.get(shadow_key), tolerance)
        if not parts:
            matched += 1
            continue
        mismatch = Mismatch(key[0], key[1], parts)
        if not _window_diff(window, advisory,
                            shadow.drop_model.get(shadow_key), tolerance):
            explained.append(mismatch)
        elif shadow_key in shadow.at_risk_windows:
            related.append(mismatch)
        else:
            unexplained.append(mismatch)
    return CompareResult(len(keys), matched, explained, related, unexplained,
                         not unexplained)


def _mismatch_entry(mismatches: list[Mismatch]) -> dict:
    """One mismatch class: count, truncated flag, and up to 100 windows."""
    return {
        "count": len(mismatches),
        "truncated": len(mismatches) > MAX_REPORT_MISMATCHES,
        "windows": [
            {"symbol": m.symbol,
             "window_start_ms": m.window_start_ms,
             "parts": list(m.parts)}
            for m in mismatches[:MAX_REPORT_MISMATCHES]
        ],
    }


def build_report(shadow, result: CompareResult, tolerance: Decimal,
                 topics: dict[str, str]) -> dict:
    """JSON-able summary of one comparison run for the flow-validation CLI."""
    return {
        "topics": dict(topics),
        "tolerance": str(tolerance),
        "total_events": shadow.total_events,
        "total_windows": result.total_windows,
        "matched": result.matched,
        "at_risk_windows": len(shadow.at_risk_windows),
        "at_risk_events": shadow.at_risk_events,
        "final_max_tt_ms": shadow.final_max_tt_ms,
        "lateness_explained": _mismatch_entry(result.lateness_explained),
        "lateness_related": _mismatch_entry(result.lateness_related),
        "unexplained": _mismatch_entry(result.unexplained),
        "passed": result.passed,
    }
