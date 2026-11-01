#!/usr/bin/env python3
"""The materializer's PostgreSQL read models vs the topic contents.

The materializer upserts every `ema-windows` record into `ema_windows` and
every `trade-advisories` record into `trade_advisories`, plus the window
embedded in the advisory.  expected_state() recomputes both tables from the
topic records (last record per symbol and window start wins, like the
upserts) and compare_db() checks them, and the `symbols` table, against
what read_db() finds.  Decimals compare by value, so `1.50` equals `1.5`;
there is no tolerance because the materializer stores the topic strings
unchanged.

An advisory's embedded window must equal the `ema-windows` record of the
same window, and must have one: otherwise the stored row would depend on
which topic the materializer read last, so expected_state() reports it as a
conflict (a mismatch of table `trade_advisories`) instead of picking one.
Mismatch parts read "<field> <db> != <topic>".  psycopg is imported only by
connect(), so everything else runs without a database.
"""

import calendar
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

import bindings
import config

MISSING_FROM_DB = "missing from the DB"
NOT_IN_TOPICS = "not in the topics"

SQL_SYMBOLS = "SELECT symbol FROM symbols"
SQL_WINDOWS = ("SELECT symbol, window_start, window_end, closing_price_micros, ema38, ema100"
               " FROM ema_windows")
SQL_ADVISORIES = ("SELECT symbol, window_start, action, previous_ema38, previous_ema100"
                  " FROM trade_advisories")


class WindowRow(NamedTuple):
    """One ema_windows row; a None EMA is an unparseable topic string."""

    window_end_ms: int
    closing_price_micros: int
    ema38: Decimal | None
    ema100: Decimal | None


class AdvisoryRow(NamedTuple):
    """One trade_advisories row (action "" = unspecified in the topic)."""

    action: str
    previous_ema38: Decimal | None
    previous_ema100: Decimal | None


class DbState(NamedTuple):
    """The three tables, rows keyed by (symbol, window_start_ms)."""

    symbols: frozenset[str]
    windows: dict[tuple[str, int], WindowRow]
    advisories: dict[tuple[str, int], AdvisoryRow]


class DbMismatch(NamedTuple):
    """One difference; window_start_ms is None for the symbols table."""

    table: str
    symbol: str
    window_start_ms: int | None
    parts: tuple[str, ...]


class ExpectedState(NamedTuple):
    """What the tables must hold, plus topic-side inconsistencies."""

    state: DbState
    conflicts: list[DbMismatch]


class DbResult(NamedTuple):
    expected_windows: int
    db_windows: int
    matched_windows: int
    expected_advisories: int
    db_advisories: int
    matched_advisories: int
    expected_symbols: int
    db_symbols: int
    mismatches: list[DbMismatch]
    passed: bool


def _decimal(text: str) -> Decimal | None:
    """Decimal price string -> Decimal; None when empty/unparseable/non-finite."""
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _timestamp_ms(timestamp) -> int:
    """google.protobuf.Timestamp -> epoch milliseconds."""
    return timestamp.seconds * 1000 + timestamp.nanos // 1_000_000


def _datetime_ms(moment) -> int:
    """Timezone-aware datetime -> epoch milliseconds."""
    return calendar.timegm(moment.utctimetuple()) * 1000 + moment.microsecond // 1000


def _window_row(msg) -> WindowRow:
    return WindowRow(_timestamp_ms(msg.window_end), msg.closing_price_micros,
                     _decimal(msg.ema38), _decimal(msg.ema100))


def _parse(msg_type, value: bytes):
    msg = msg_type()
    msg.ParseFromString(value)
    return msg


def _differences(want, got, fields) -> tuple[str, ...]:
    """Parts for every field where got != want ("<field> <got> != <want>")."""
    return tuple(f"{name} {actual} != {expected}"
                 for name, expected, actual in zip(fields, want, got) if expected != actual)


def expected_state(ema_records, advisory_records) -> ExpectedState:
    """Recompute the tables from (offset, bytes) records of the two topics."""
    windows, advisories, embedded = {}, {}, {}
    for _, value in ema_records:
        msg = _parse(bindings.EmaWindow, value)
        windows[(msg.symbol, _timestamp_ms(msg.window_start))] = _window_row(msg)
    topic_windows = dict(windows)
    action_names = {bindings.ADVISORY_ACTION_BUY: "BUY", bindings.ADVISORY_ACTION_SELL: "SELL"}
    for _, value in advisory_records:
        msg = _parse(bindings.TradeAdvisory, value)
        window = msg.ema_window
        key = (window.symbol, _timestamp_ms(window.window_start))
        embedded[key] = _window_row(window)
        advisories[key] = AdvisoryRow(action_names.get(msg.action, ""),
                                      _decimal(msg.previous_ema38), _decimal(msg.previous_ema100))
    conflicts = []
    for key, row in embedded.items():
        if key not in topic_windows:
            windows[key] = row      # the materializer stores it from the advisory
            parts = ("embedded window has no ema-windows record",)
        else:
            parts = tuple(f"embedded window {part}"
                          for part in _differences(topic_windows[key], row, WindowRow._fields))
        if parts:
            conflicts.append(DbMismatch("trade_advisories", key[0], key[1], parts))
    symbols = frozenset(symbol for symbol, _ in windows)
    return ExpectedState(DbState(symbols, windows, advisories), conflicts)


def connect(url: str, connector=None):
    """A read-only connection; psycopg is imported here and nowhere else."""
    if connector is None:
        import psycopg
        connector = psycopg.connect
    conn = connector(url)
    conn.read_only = True
    return conn


def read_db(conn) -> DbState:
    """Read the three tables through any psycopg-style connection."""
    symbols = frozenset(symbol for (symbol,) in conn.execute(SQL_SYMBOLS).fetchall())
    windows = {}
    for symbol, start, end, close, ema38, ema100 in conn.execute(SQL_WINDOWS).fetchall():
        windows[(symbol, _datetime_ms(start))] = WindowRow(_datetime_ms(end), int(close), ema38, ema100)
    advisories = {}
    for symbol, start, action, previous38, previous100 in conn.execute(SQL_ADVISORIES).fetchall():
        advisories[(symbol, _datetime_ms(start))] = AdvisoryRow(action, previous38, previous100)
    return DbState(symbols, windows, advisories)


def _compare_table(table, want, got, fields, mismatches) -> int:
    """Append one mismatch per differing key to `mismatches`; returns the match count."""
    matched = 0
    for key in sorted(want.keys() | got.keys()):
        if key not in got:
            parts = (MISSING_FROM_DB,)
        elif key not in want:
            parts = (NOT_IN_TOPICS,)
        else:
            parts = _differences(want[key], got[key], fields)
        if parts:
            mismatches.append(DbMismatch(table, key[0], key[1], parts))
        else:
            matched += 1
    return matched


def compare_db(expected: ExpectedState, actual: DbState) -> DbResult:
    """Every difference between the expected tables and the database."""
    mismatches = []
    for symbol in sorted(expected.state.symbols ^ actual.symbols):
        parts = (MISSING_FROM_DB,) if symbol in expected.state.symbols else (NOT_IN_TOPICS,)
        mismatches.append(DbMismatch("symbols", symbol, None, parts))
    matched_windows = _compare_table("ema_windows", expected.state.windows, actual.windows,
                                     WindowRow._fields, mismatches)
    matched_advisories = _compare_table("trade_advisories", expected.state.advisories,
                                        actual.advisories, AdvisoryRow._fields, mismatches)
    mismatches.extend(expected.conflicts)
    return DbResult(len(expected.state.windows), len(actual.windows), matched_windows,
                    len(expected.state.advisories), len(actual.advisories), matched_advisories,
                    len(expected.state.symbols), len(actual.symbols), mismatches, not mismatches)


def db_report(result: DbResult) -> dict:
    """JSON-able summary of one database comparison."""
    cap = config.MAX_REPORT_MISMATCHES
    return {
        "expected_windows": result.expected_windows,
        "db_windows": result.db_windows,
        "matched_windows": result.matched_windows,
        "expected_advisories": result.expected_advisories,
        "db_advisories": result.db_advisories,
        "matched_advisories": result.matched_advisories,
        "expected_symbols": result.expected_symbols,
        "db_symbols": result.db_symbols,
        "mismatches": {
            "count": len(result.mismatches),
            "truncated": len(result.mismatches) > cap,
            "windows": [{"table": m.table, "symbol": m.symbol,
                         "window_start_ms": m.window_start_ms, "parts": list(m.parts)}
                        for m in result.mismatches[:cap]],
        },
        "passed": result.passed,
    }
