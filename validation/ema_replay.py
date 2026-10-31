#!/usr/bin/env python3
"""Shared EMA replay for the two course project queries.

Pure replay of per-(symbol, window) close states through the EMA 38/100
recursion and the BUY/SELL crossover rules (PDF 3.1-3.3), independent of
how the close states were produced (CSV scan or event stream).  Spec:
documents/CS_E4780_Course_Project_2026.pdf.

Also the processor's late-event rule (is_late), shared by the CSV
generator and the stream shadow: services/streaming-processor uses
forBoundedOutOfOrderness(70 s) with allowed lateness 0, so the watermark
is `max trade time seen - OutOfOrdernessMs - 1 ms` and an event is
dropped once its window's last timestamp has reached that watermark.

Arithmetic (must stay byte-for-byte identical to the CSV generator):
EMA_i = (Close_i * 2 + EMA_{i-1} * (j-1)) / (j+1) as one division under a
50-digit decimal context; advisories compare the exact Decimals.

First-window EMA initialization modes:
  prev-zero    default; the PDF's "the EMA of the previous window is 0".
  first-close  classic bootstrap; first EMA = Close for both factors.
  zero-window  literal EMA[s, w_0] = 0; the recursion starts in window two.
"""

from collections import defaultdict
from decimal import Decimal, localcontext
from typing import NamedTuple

WindowMs = 300_000         # 5-minute tumbling windows anchored at 00:00 CEST (PDF 3.1)
JShort, JLong = 38, 100    # smoothing factors (PDF 3.2, 3.3)
DECIMAL_PREC = 50          # far tighter than double precision
# StreamingProcessor.OutOfOrdernessAllowance: forBoundedOutOfOrderness(70 s), allowed lateness 0.
OutOfOrdernessMs = 70_000


def is_late(tt_ms: int, max_tt_ms: int, out_of_orderness_ms: int) -> bool:
    """Whether Flink drops an event: the watermark (max trade time seen -
    out_of_orderness - 1 ms) has reached its window's last timestamp."""
    window_last_ms = (tt_ms // WindowMs + 1) * WindowMs - 1
    return window_last_ms <= max_tt_ms - out_of_orderness_ms - 1


class CloseState(NamedTuple):
    """Per-(symbol, window) close: max trade time wins, ties to the later order."""

    close_tt_ms: int
    close_micros: int
    close_order: int
    event_count: int


class WindowResult(NamedTuple):
    """One emitted window: close identity, EMA pair, previous EMA pair, advisory."""

    symbol: str
    window_index: int
    close_micros: int
    close_order: int
    event_count: int
    ema38_micros: Decimal
    ema100_micros: Decimal
    prev_ema38_micros: Decimal
    prev_ema100_micros: Decimal
    advisory: str  # "BUY" / "SELL" / ""


def update_close(state: CloseState | None, tt_ms: int, micros: int, order: int) -> CloseState:
    """Fold one price event into a window's state (None = the first event).

    The close is replaced iff tt_ms is greater, or equal with a greater
    order (the later source row wins ties); event_count always increments.
    """
    if state is None:
        return CloseState(tt_ms, micros, order, 1)
    event_count = state.event_count + 1
    if tt_ms > state.close_tt_ms or (tt_ms == state.close_tt_ms and order > state.close_order):
        return CloseState(tt_ms, micros, order, event_count)
    return state._replace(event_count=event_count)


def replay(closes: dict[tuple[str, int], CloseState], ema_init: str = "prev-zero") -> list[WindowResult]:
    """Replay the windows in (symbol, window_index) order into WindowResult rows.

    The previous EMA pair carries forward across windows with no events;
    crossovers compare against the last emitted pair.
    """
    per_symbol = defaultdict(dict)
    for (symbol, widx), state in closes.items():
        per_symbol[symbol][widx] = state
    results = []
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PREC
        for symbol in sorted(per_symbol):
            prev_short = prev_long = Decimal(0)
            first = True
            for widx in sorted(per_symbol[symbol]):
                state = per_symbol[symbol][widx]
                close = Decimal(state.close_micros)
                if first and ema_init == "first-close":
                    prev_short = prev_long = close  # seed: first EMA = Close
                if first and ema_init == "zero-window":
                    ema_short = ema_long = Decimal(0)  # literal EMA[s, w_0] = 0
                else:
                    # Close*a + prev*(1-a) as one division: exact at the
                    # first-close seed (a pre-rounded a adds noise that can
                    # flip the EMA38==EMA100 comparison).
                    ema_short = (close * 2 + prev_short * (JShort - 1)) / (JShort + 1)
                    ema_long = (close * 2 + prev_long * (JLong - 1)) / (JLong + 1)
                advisory = ""
                if ema_short > ema_long and prev_short <= prev_long:
                    advisory = "BUY"
                elif ema_short < ema_long and prev_short >= prev_long:
                    advisory = "SELL"
                results.append(WindowResult(
                    symbol, widx, state.close_micros, state.close_order,
                    state.event_count, ema_short, ema_long,
                    prev_short, prev_long, advisory))
                prev_short, prev_long = ema_short, ema_long
                first = False
    return results
