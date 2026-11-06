#!/usr/bin/env python3
"""Tests for db_check.py.

The materializer upserts every `ema-windows` record and every
`trade-advisories` record (plus the window embedded in the advisory) into
PostgreSQL; db_check recomputes what those tables must hold from the topic
records and compares them exactly.  Expected state: the last record per
(symbol, window start) wins, advisories add their embedded window, and an
embedded window that disagrees with (or has no) `ema-windows` record is
reported as its own mismatch because the table content would then depend on
which topic the materializer happened to read last.  Cases: a faithful
database matches; every kind of difference (missing row, unexpected row,
changed value, wrong symbol set) is reported against the right table and
window; numerically equal decimals with different scales match; read_db
turns psycopg-style rows into the same state shape; the report is
JSON-able and caps the listed windows.
"""

import json
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bindings  # noqa: E402
import config  # noqa: E402
import db_check  # noqa: E402

AAA, BBB = "AAA.NL", "BBB.FR"
W0, W1 = 0, 300_000          # two adjacent 5-minute windows (epoch ms)
ACTION = {"BUY": bindings.ADVISORY_ACTION_BUY, "SELL": bindings.ADVISORY_ACTION_SELL}


def ts_ms(ms):
    ts = Timestamp()
    ts.seconds, ts.nanos = ms // 1000, (ms % 1000) * 1_000_000
    return ts


def ema_msg(symbol, start_ms, close=19_260_000, ema38="1.5", ema100="1.25", end_ms=None):
    return bindings.EmaWindow(
        symbol=symbol, window_start=ts_ms(start_ms),
        window_end=ts_ms(start_ms + 300_000 if end_ms is None else end_ms),
        closing_price_micros=close, ema38=ema38, ema100=ema100)


def ema_record(offset, msg):
    return offset, msg.SerializeToString()


def advisory_record(offset, window, action="BUY", prev38="0", prev100="0"):
    msg = bindings.TradeAdvisory(ema_window=window, action=ACTION[action],
                                 previous_ema38=prev38, previous_ema100=prev100)
    return offset, msg.SerializeToString()


def expected(ema_records, advisory_records):
    return db_check.expected_state(ema_records, advisory_records)


def scenario():
    """Two AAA windows (the first with a BUY advisory) and one BBB window."""
    a0 = ema_msg(AAA, W0, 10_000_000, "1.5", "1.25")
    a1 = ema_msg(AAA, W1, 11_000_000, "2.5", "2.25")
    b0 = ema_msg(BBB, W0, 5_000_000, "0.5", "0.25")
    ema = [ema_record(0, a0), ema_record(1, b0), ema_record(2, a1)]
    adv = [advisory_record(0, a0, "BUY", "0", "0")]
    return ema, adv


def copy_state(state):
    return db_check.DbState(state.symbols, dict(state.windows), dict(state.advisories))


class ExpectedStateTest(unittest.TestCase):
    def test_collects_windows_advisories_and_symbols(self):
        ema, adv = scenario()
        result = expected(ema, adv)
        self.assertEqual(result.state.symbols, frozenset({AAA, BBB}))
        self.assertEqual(len(result.state.windows), 3)
        self.assertEqual(result.state.windows[(AAA, W0)], db_check.WindowRow(
            300_000, 10_000_000, Decimal("1.5"), Decimal("1.25")))
        self.assertEqual(result.state.advisories,
                         {(AAA, W0): db_check.AdvisoryRow("BUY", Decimal("0"), Decimal("0"))})
        self.assertEqual(result.conflicts, [])

    def test_last_duplicate_wins(self):
        first = ema_msg(AAA, W0, 1_000_000, "1", "1")
        second = ema_msg(AAA, W0, 2_000_000, "2", "2")
        result = expected([ema_record(0, first), ema_record(1, second)], [])
        self.assertEqual(result.state.windows[(AAA, W0)].closing_price_micros, 2_000_000)

    def test_advisory_embedded_window_that_disagrees_is_a_conflict(self):
        ema, _ = scenario()
        wrong = ema_msg(AAA, W0, 10_000_000, "9.9", "1.25")      # ema38 differs from the topic
        result = expected(ema, [advisory_record(0, wrong)])
        self.assertEqual(len(result.conflicts), 1)
        conflict = result.conflicts[0]
        self.assertEqual((conflict.table, conflict.symbol, conflict.window_start_ms),
                         ("trade_advisories", AAA, W0))
        self.assertTrue(any("ema38" in part for part in conflict.parts), conflict.parts)

    def test_advisory_window_without_an_ema_windows_record_is_a_conflict(self):
        orphan = ema_msg(BBB, W1)
        result = expected([], [advisory_record(0, orphan)])
        self.assertEqual([c.parts for c in result.conflicts],
                         [("embedded window has no ema-windows record",)])
        self.assertIn((BBB, W1), result.state.windows)           # the materializer still stores it


class CompareDbTest(unittest.TestCase):
    def setUp(self):
        ema, adv = scenario()
        self.expected = expected(ema, adv)
        self.actual = copy_state(self.expected.state)

    def compare(self):
        return db_check.compare_db(self.expected, self.actual)

    def parts(self, table, symbol, start_ms):
        found = [m.parts for m in self.compare().mismatches
                 if (m.table, m.symbol, m.window_start_ms) == (table, symbol, start_ms)]
        self.assertEqual(len(found), 1, found)
        return found[0]

    def test_faithful_database_passes(self):
        result = self.compare()
        self.assertTrue(result.passed)
        self.assertEqual(result.mismatches, [])
        self.assertEqual((result.expected_windows, result.db_windows, result.matched_windows),
                         (3, 3, 3))
        self.assertEqual((result.expected_advisories, result.db_advisories,
                          result.matched_advisories), (1, 1, 1))
        self.assertEqual((result.expected_symbols, result.db_symbols), (2, 2))

    def test_missing_window_row(self):
        del self.actual.windows[(BBB, W0)]
        self.assertEqual(self.parts("ema_windows", BBB, W0), ("missing from the DB",))
        self.assertFalse(self.compare().passed)

    def test_unexpected_window_row(self):
        self.actual.windows[("CCC.DE", W0)] = db_check.WindowRow(
            300_000, 1, Decimal(1), Decimal(1))
        self.assertEqual(self.parts("ema_windows", "CCC.DE", W0), ("not in the topics",))

    def test_changed_window_values_are_named(self):
        row = self.actual.windows[(AAA, W1)]
        self.actual.windows[(AAA, W1)] = row._replace(
            closing_price_micros=row.closing_price_micros + 1, ema100=Decimal("2.26"),
            window_end_ms=0)
        parts = self.parts("ema_windows", AAA, W1)
        self.assertEqual(sorted(p.split(" ")[0] for p in parts),
                         ["closing_price_micros", "ema100", "window_end_ms"])
        self.assertIn("ema100 2.26 != 2.25", parts)

    def test_decimals_equal_in_value_match_whatever_their_scale(self):
        row = self.actual.windows[(AAA, W0)]
        self.actual.windows[(AAA, W0)] = row._replace(ema38=Decimal("1.500000000"))
        self.assertTrue(self.compare().passed)

    def test_missing_advisory_row(self):
        del self.actual.advisories[(AAA, W0)]
        self.assertEqual(self.parts("trade_advisories", AAA, W0), ("missing from the DB",))

    def test_unexpected_advisory_row(self):
        self.actual.advisories[(BBB, W0)] = db_check.AdvisoryRow("SELL", Decimal(1), Decimal(1))
        self.assertEqual(self.parts("trade_advisories", BBB, W0), ("not in the topics",))

    def test_changed_advisory_values_are_named(self):
        self.actual.advisories[(AAA, W0)] = db_check.AdvisoryRow("SELL", Decimal("0"), Decimal("1"))
        parts = self.parts("trade_advisories", AAA, W0)
        self.assertEqual(sorted(p.split(" ")[0] for p in parts), ["action", "previous_ema100"])

    def test_symbol_sets_must_match(self):
        symbols = set(self.actual.symbols)
        symbols.discard(BBB)
        symbols.add("ZZZ.US")
        self.actual = db_check.DbState(frozenset(symbols), self.actual.windows, self.actual.advisories)
        result = self.compare()
        found = {(m.table, m.symbol): m.parts for m in result.mismatches}
        self.assertEqual(found[("symbols", BBB)], ("missing from the DB",))
        self.assertEqual(found[("symbols", "ZZZ.US")], ("not in the topics",))
        self.assertIsNone(result.mismatches[0].window_start_ms)

    def test_topic_conflicts_fail_the_check_even_when_the_database_matches_the_ema_topic(self):
        ema, _ = scenario()
        wrong = ema_msg(AAA, W0, 10_000_000, "9.9", "1.25")
        self.expected = expected(ema, [advisory_record(0, wrong)])
        self.actual = copy_state(self.expected.state)
        result = self.compare()
        self.assertFalse(result.passed)
        self.assertEqual([m.table for m in result.mismatches], ["trade_advisories"])


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    """psycopg-like connection: execute(sql) -> cursor, rows chosen by table name."""

    def __init__(self, symbols, windows, advisories):
        self.tables = {"symbols": symbols, "ema_windows": windows, "trade_advisories": advisories}
        self.sql = []
        self.closed = False

    def execute(self, sql):
        self.sql.append(sql)
        for table in ("trade_advisories", "ema_windows", "symbols"):
            if f"FROM {table}" in sql:
                return FakeCursor(self.tables[table])
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        self.closed = True


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class ReadDbTest(unittest.TestCase):
    def test_rows_become_the_same_state_shape_as_the_topics(self):
        conn = FakeConn(
            symbols=[(AAA,), (BBB,)],
            windows=[(AAA, utc(W0), utc(W0 + 300_000), Decimal("10000000"), Decimal("1.50"), Decimal("1.25")),
                     (BBB, utc(W1), utc(W1 + 300_000), Decimal("5000000"), Decimal("0.5"), Decimal("0.25"))],
            advisories=[(AAA, utc(W0), "BUY", Decimal("0"), Decimal("0.0"))])
        state = db_check.read_db(conn)
        self.assertEqual(state.symbols, frozenset({AAA, BBB}))
        self.assertEqual(state.windows[(AAA, W0)], db_check.WindowRow(
            300_000, 10_000_000, Decimal("1.5"), Decimal("1.25")))
        self.assertIsInstance(state.windows[(AAA, W0)].closing_price_micros, int)
        self.assertEqual(state.windows[(BBB, W1)].window_end_ms, W1 + 300_000)
        self.assertEqual(state.advisories[(AAA, W0)], db_check.AdvisoryRow("BUY", Decimal(0), Decimal(0)))
        self.assertEqual(len(conn.sql), 3)

    def test_round_trip_with_the_expected_state_matches(self):
        ema, adv = scenario()
        exp = expected(ema, adv)
        conn = FakeConn(
            symbols=[(s,) for s in exp.state.symbols],
            windows=[(sym, utc(start), utc(row.window_end_ms), Decimal(row.closing_price_micros),
                      row.ema38, row.ema100) for (sym, start), row in exp.state.windows.items()],
            advisories=[(sym, utc(start), row.action, row.previous_ema38, row.previous_ema100)
                        for (sym, start), row in exp.state.advisories.items()])
        self.assertTrue(db_check.compare_db(exp, db_check.read_db(conn)).passed)


class ConnectTest(unittest.TestCase):
    def test_connect_uses_the_url_and_marks_the_session_read_only(self):
        seen = {}

        class Conn:
            read_only = False

        def connector(url):
            seen["url"] = url
            return Conn()

        conn = db_check.connect("postgresql://u:p@h:1/db", connector=connector)
        self.assertEqual(seen["url"], "postgresql://u:p@h:1/db")
        self.assertTrue(conn.read_only)


class ReportTest(unittest.TestCase):
    def test_report_is_json_able_and_caps_the_listed_windows(self):
        ema, adv = scenario()
        exp = expected(ema, adv)
        empty = db_check.DbState(frozenset(), {}, {})
        result = db_check.compare_db(exp, empty)
        report = db_check.db_report(result)
        json.dumps(report)
        self.assertFalse(report["passed"])
        self.assertEqual((report["expected_windows"], report["db_windows"]), (3, 0))
        self.assertEqual(report["mismatches"]["count"], 3 + 1 + 2)   # windows + advisory + symbols
        self.assertLessEqual(len(report["mismatches"]["windows"]), config.MAX_REPORT_MISMATCHES)
        entry = report["mismatches"]["windows"][0]
        self.assertEqual(set(entry), {"table", "symbol", "window_start_ms", "parts"})

    def test_passing_report(self):
        ema, adv = scenario()
        exp = expected(ema, adv)
        report = db_check.db_report(db_check.compare_db(exp, copy_state(exp.state)))
        self.assertTrue(report["passed"])
        self.assertEqual(report["matched_windows"], 3)
        self.assertEqual(report["mismatches"], {"count": 0, "truncated": False, "windows": []})


if __name__ == "__main__":
    unittest.main()
