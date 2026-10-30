#!/usr/bin/env python3
"""Tests for compare.py.

Actual-side protobuf decoding, the (symbol, window_start_ms) join
against the shadow, per-aspect mismatch parts, the four-way
classification (match / lateness_explained / lateness_related /
unexplained), both directions of window-set inequality, the tolerance
boundary (exactly 1e-9 matches, 1.1e-9 does not), malformed records
(empty/unparseable EMA strings, unspecified advisory action) surviving
as unexplained mismatches instead of crashes, and the JSON report
builder with its per-class cap of 100 and `truncated` flag.

Shadow side is always real: either `shadow_replay` over crafted event
streams or hand-built `ShadowOutput`/`WindowResult` tuples; actual side
is real protobuf messages built via `bindings` and round-tripped
through the decoders under test.
"""

import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bindings  # noqa: E402
import compare as cmp  # noqa: E402
import ema_replay as er  # noqa: E402
import shadow as sh  # noqa: E402

MICROS = Decimal(10**6)
SYM = "AAA.NL"
OTHER = "BB.DE"
TOPICS = {"events": "trade-events", "ema": "ema-windows",
          "advisories": "trade-advisories"}
ACTION = {"BUY": bindings.ADVISORY_ACTION_BUY, "SELL": bindings.ADVISORY_ACTION_SELL}


def make_event(symbol, tt_ms, micros, order):
    return sh.ShadowEvent(symbol, tt_ms, micros, order)


def ts_ms(ms):
    ts = Timestamp()
    ts.seconds, ts.nanos = ms // 1000, (ms % 1000) * 1_000_000
    return ts


def proto_window(symbol, start_ms, close_micros, ema38, ema100):
    return bindings.EmaWindow(symbol=symbol, window_start=ts_ms(start_ms),
                              closing_price_micros=close_micros,
                              ema38=ema38, ema100=ema100)


def proto_advisory(window, action, prev38, prev100):
    return bindings.TradeAdvisory(ema_window=window, action=action,
                                  previous_ema38=prev38,
                                  previous_ema100=prev100)


def price_str(ema_micros):
    return str(ema_micros / MICROS)


def actual_from_shadow(result, table="keep_all"):
    """Protobuf messages rebuilt from one shadow table, run through the
    decoders under test -> (windows, advisories)."""
    windows, advisories = [], []
    for (symbol, widx), row in getattr(result, table).items():
        window = proto_window(symbol, widx * er.WindowMs, row.close_micros,
                              price_str(row.ema38_micros),
                              price_str(row.ema100_micros))
        windows.append(cmp.decode_ema_window(window))
        if row.advisory:
            advisories.append(cmp.decode_advisory(proto_advisory(
                window, ACTION[row.advisory],
                price_str(row.prev_ema38_micros),
                price_str(row.prev_ema100_micros))))
    return windows, advisories


def tweak(windows, start_ms, **changes):
    return [w._replace(**changes) if w.window_start_ms == start_ms else w
            for w in windows]


class DecodeEmaWindowTest(unittest.TestCase):
    def test_round_trip(self):
        window = proto_window(SYM, 1_636_696_800_999, 57_117_900,
                              "123.456789", "122.987654")
        self.assertEqual(
            cmp.decode_ema_window(window),
            cmp.ActualEmaWindow(SYM, 1_636_696_800_999, 57_117_900,
                                Decimal("123.456789"),
                                Decimal("122.987654")))

    def test_empty_and_unparseable_ema_strings_are_none(self):
        window = proto_window(SYM, 0, 1, "", "not-a-number")
        decoded = cmp.decode_ema_window(window)
        self.assertIsNone(decoded.ema38)
        self.assertIsNone(decoded.ema100)

    def test_non_finite_ema_string_is_none(self):
        window = proto_window(SYM, 0, 1, "NaN", "1.0")
        self.assertIsNone(cmp.decode_ema_window(window).ema38)


class DecodeAdvisoryTest(unittest.TestCase):
    def test_buy_and_sell_actions(self):
        window = proto_window(SYM, 300_000, 15_000_000, "10.0", "9.5")
        decoded = cmp.decode_advisory(proto_advisory(
            window, bindings.ADVISORY_ACTION_BUY, "9.0", "9.5"))
        self.assertEqual(decoded,
                         cmp.ActualAdvisory(SYM, 300_000, "BUY",
                                            Decimal("9.0"), Decimal("9.5")))
        sell = cmp.decode_advisory(proto_advisory(
            window, bindings.ADVISORY_ACTION_SELL, "", ""))
        self.assertEqual(sell.action, "SELL")
        self.assertIsNone(sell.previous_ema38)
        self.assertIsNone(sell.previous_ema100)

    def test_unspecified_action_is_empty(self):
        window = proto_window(SYM, 0, 1, "1.0", "1.0")
        advisory = proto_advisory(window, 0, "1.0", "1.0")
        self.assertEqual(cmp.decode_advisory(advisory).action, "")


class FullMatchTest(unittest.TestCase):
    def test_actual_built_from_keep_all_passes(self):
        result = sh.shadow_replay([
            make_event(SYM, 10_000, 15_000_000, 1),    # w0
            make_event(SYM, 700_000, 25_000_000, 2),   # w2
            make_event(OTHER, 50_000, 11_000_000, 3),  # w0
        ])
        compared = cmp.compare(*actual_from_shadow(result), result)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.total_windows, 3)
        self.assertEqual(compared.matched, 3)
        self.assertEqual(compared.lateness_explained, [])
        self.assertEqual(compared.lateness_related, [])
        self.assertEqual(compared.unexplained, [])


class LatenessExplainedTest(unittest.TestCase):
    """A window the drop model reproduces exactly -> lateness_explained."""

    def late_stream(self):
        # w0's second event arrives after w3 pushed the watermark past
        # w0: keep-all folds it (and it wins the close), the model drops it.
        return sh.shadow_replay([
            make_event(SYM, 100_000, 10_000_000, 1),   # w0 keep
            make_event(SYM, 1_000_000, 50_000_000, 2),  # w3 keep; max -> 1_000_000
            make_event(SYM, 150_000, 9_000_000, 3),    # w0 late -> model drops
        ])

    def test_close_price_mismatch_reproduced_by_drop_model(self):
        result = self.late_stream()
        compared = cmp.compare(*actual_from_shadow(result, "drop_model"), result)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.total_windows, 2)
        # lateness also shifts the advisory sequence: keep-all's w3 has no
        # crossover (prev pair from w0), the drop model's w3 is a first
        # window -> BUY; both windows are reproduced by the drop model
        self.assertEqual(compared.matched, 0)
        self.assertEqual(compared.lateness_related, [])
        self.assertEqual(compared.unexplained, [])
        self.assertEqual(len(compared.lateness_explained), 2)
        by_start = {m.window_start_ms: m for m in compared.lateness_explained}
        self.assertTrue(any("closing_price_micros" in p
                            for p in by_start[0].parts))
        self.assertTrue(any(p.startswith("ema38") for p in by_start[0].parts))
        # w3's EMA pair shifts too: keep-all's w3 carries w0's previous pair
        # (close 9M), the drop model's w0 kept its own earlier event (10M)
        self.assertEqual(len(by_start[900_000].parts), 2)
        self.assertTrue(any(p.startswith("ema38") for p in by_start[900_000].parts))
        self.assertTrue(any(p.startswith("ema100") for p in by_start[900_000].parts))

    def test_window_missing_from_actual_and_drop_model(self):
        # every w0 event is late: the model has no w0 row, and neither does
        # actual -> the drop model reproduces actual (both empty) exactly
        result = sh.shadow_replay([
            make_event(SYM, 1_000_000, 50_000_000, 1),  # w3
            make_event(SYM, 150_000, 9_000_000, 2),     # w0, late, dropped
        ])
        windows, advisories = actual_from_shadow(result)
        windows = [w for w in windows if w.window_start_ms != 0]
        advisories = [a for a in advisories if a.window_start_ms != 0]
        compared = cmp.compare(windows, advisories, result)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.matched, 1)
        self.assertEqual(compared.lateness_explained,
                         [cmp.Mismatch(SYM, 0, ("window missing from actual",))])
        self.assertEqual(compared.unexplained, [])


class LatenessRelatedTest(unittest.TestCase):
    def test_mismatch_on_at_risk_window_not_reproduced_by_model(self):
        result = sh.shadow_replay([
            make_event(SYM, 100_000, 10_000_000, 1),    # w0 keep
            make_event(SYM, 1_000_000, 50_000_000, 2),  # w3 keep; max -> 1_000_000
            make_event(SYM, 150_000, 9_000_000, 3),     # w0 late -> model drops
        ])
        self.assertEqual(result.at_risk_windows, {(SYM, 0)})
        windows, advisories = actual_from_shadow(result)
        # close matches neither keep-all (9M) nor drop-model (10M)
        windows = tweak(windows, 0, closing_price_micros=11_000_000)
        compared = cmp.compare(windows, advisories, result)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.lateness_explained, [])
        self.assertEqual(compared.unexplained, [])
        self.assertEqual(compared.lateness_related, [
            cmp.Mismatch(SYM, 0, ("closing_price_micros 11000000 != 9000000",))])


class UnexplainedTest(unittest.TestCase):
    def single_window_shadow(self):
        # clean window: no drops possible, not at-risk
        result = sh.shadow_replay([make_event(SYM, 10_000, 15_000_000, 1)])
        self.assertEqual(result.at_risk_windows, frozenset())
        return result

    def test_close_mismatch_off_at_risk_set(self):
        result = self.single_window_shadow()
        windows, advisories = actual_from_shadow(result)
        windows = tweak(windows, 0, closing_price_micros=16_000_000)
        compared = cmp.compare(windows, advisories, result)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.matched, 0)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 0, ("closing_price_micros 16000000 != 15000000",))])

    def test_window_missing_from_actual(self):
        result = self.single_window_shadow()
        compared = cmp.compare([], [], result)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 0, ("window missing from actual",))])

    def test_window_unexpected_in_actual(self):
        result = self.single_window_shadow()
        windows, advisories = actual_from_shadow(result)
        windows.append(cmp.ActualEmaWindow(SYM, 600_000, 1_000_000,
                                           Decimal("1.0"), Decimal("1.0")))
        compared = cmp.compare(windows, advisories, result)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.total_windows, 2)
        self.assertEqual(compared.matched, 1)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 600_000, ("window unexpected in actual",))])


class AdvisoryComparisonTest(unittest.TestCase):
    """w0 emits BUY (prev-zero first window); w1 has no crossover -> ""."""

    def two_window_shadow(self):
        result = sh.shadow_replay([
            make_event(SYM, 10_000, 15_000_000, 1),   # w0 -> BUY
            make_event(SYM, 310_000, 15_000_000, 2),  # w1 -> ""
        ])
        self.assertEqual(result.at_risk_windows, frozenset())
        return result

    def test_wrong_action(self):
        result = self.two_window_shadow()
        windows, advisories = actual_from_shadow(result)
        advisories = [a._replace(action="SELL") for a in advisories]
        compared = cmp.compare(windows, advisories, result)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 0, ("advisory action 'SELL' != 'BUY'",))])

    def test_missing_advisory(self):
        result = self.two_window_shadow()
        windows, _ = actual_from_shadow(result)
        compared = cmp.compare(windows, [], result)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 0, ("missing advisory (expected BUY)",))])

    def test_extra_advisory(self):
        result = self.two_window_shadow()
        windows, advisories = actual_from_shadow(result)
        row = result.keep_all[(SYM, 1)]
        window = proto_window(SYM, 300_000, row.close_micros,
                              price_str(row.ema38_micros),
                              price_str(row.ema100_micros))
        advisories.append(cmp.decode_advisory(proto_advisory(
            window, bindings.ADVISORY_ACTION_BUY, "0", "0")))
        compared = cmp.compare(windows, advisories, result)
        self.assertEqual(compared.matched, 1)
        self.assertEqual(compared.unexplained, [
            cmp.Mismatch(SYM, 300_000, ("unexpected advisory",))])

    def test_previous_pair_within_tolerance_matches(self):
        result = sh.shadow_replay([make_event(SYM, 10_000, 15_000_000, 1)])
        row = result.keep_all[(SYM, 0)]
        window = proto_window(SYM, 0, row.close_micros,
                              price_str(row.ema38_micros),
                              price_str(row.ema100_micros))
        compared = cmp.compare(
            [cmp.decode_ema_window(window)],
            [cmp.decode_advisory(proto_advisory(
                window, bindings.ADVISORY_ACTION_BUY, "0.0000000005", "0"))],
            result)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.matched, 1)


class ToleranceBoundaryTest(unittest.TestCase):
    """EMA diff exactly 1e-9 matches; 1.1e-9 does not."""

    # 18 significant digits: exact under the default 28-digit context
    PRICE38 = Decimal("123.456789123456789")

    def shadow_with_price(self):
        row = er.WindowResult(SYM, 0, 15_000_000, 1, 1,
                              self.PRICE38 * MICROS,
                              Decimal("987.654321") * MICROS,
                              Decimal(0), Decimal(0), "BUY")
        return sh.ShadowOutput({(SYM, 0): row}, {(SYM, 0): row},
                               frozenset(), 0, 1, 10_000)

    def compare_with_ema38(self, ema38):
        window = cmp.ActualEmaWindow(SYM, 0, 15_000_000, ema38,
                                     Decimal("987.654321"))
        advisory = cmp.ActualAdvisory(SYM, 0, "BUY", Decimal(0), Decimal(0))
        return cmp.compare([window], [advisory], self.shadow_with_price())

    def test_diff_exactly_tolerance_matches(self):
        compared = self.compare_with_ema38(self.PRICE38 + Decimal("1e-9"))
        self.assertTrue(compared.passed)
        self.assertEqual(compared.matched, 1)
        self.assertEqual(compared.unexplained, [])

    def test_diff_just_above_tolerance_mismatches(self):
        compared = self.compare_with_ema38(self.PRICE38 + Decimal("1.1e-9"))
        self.assertFalse(compared.passed)
        self.assertEqual(compared.unexplained, [cmp.Mismatch(
            SYM, 0, (f"ema38 {self.PRICE38 + Decimal('1.1e-9')} != {self.PRICE38}",))])


class MalformedRecordTest(unittest.TestCase):
    def test_unparseable_ema_and_unspecified_action_classify_unexplained(self):
        row = er.WindowResult(SYM, 0, 15_000_000, 1, 1,
                              Decimal("100.5") * MICROS, Decimal("99.5") * MICROS,
                              Decimal(0), Decimal(0), "BUY")
        shadow = sh.ShadowOutput({(SYM, 0): row}, {(SYM, 0): row},
                                 frozenset(), 0, 1, 10_000)
        window = proto_window(SYM, 0, 15_000_000, "", "not-a-number")
        advisory = proto_advisory(window, 0, "", "NaN")  # action unspecified
        compared = cmp.compare([cmp.decode_ema_window(window)],
                               [cmp.decode_advisory(advisory)], shadow)
        self.assertFalse(compared.passed)
        mismatch = compared.unexplained[0]
        self.assertIn("ema38 unparseable", mismatch.parts)
        self.assertIn("ema100 unparseable", mismatch.parts)
        self.assertIn("advisory action '' != 'BUY'", mismatch.parts)
        self.assertIn("previous_ema38 unparseable", mismatch.parts)
        self.assertIn("previous_ema100 unparseable", mismatch.parts)


class EmptyComparisonTest(unittest.TestCase):
    def test_empty_actual_and_empty_shadow_passes(self):
        shadow = sh.ShadowOutput({}, {}, frozenset(), 0, 0, None)
        compared = cmp.compare([], [], shadow)
        self.assertTrue(compared.passed)
        self.assertEqual(compared.total_windows, 0)
        self.assertEqual(compared.matched, 0)
        self.assertEqual(compared.unexplained, [])


class BuildReportTest(unittest.TestCase):
    def test_report_shape_and_json_round_trip(self):
        result = sh.shadow_replay([make_event(SYM, 10_000, 15_000_000, 1)])
        compared = cmp.compare(*actual_from_shadow(result), result)
        report = cmp.build_report(result, compared, Decimal("1e-9"), TOPICS)
        self.assertEqual(report["topics"], TOPICS)
        self.assertEqual(report["tolerance"], str(Decimal("1e-9")))
        self.assertEqual(report["total_events"], 1)
        self.assertEqual(report["total_windows"], 1)
        self.assertEqual(report["matched"], 1)
        self.assertEqual(report["at_risk_windows"], 0)
        self.assertEqual(report["at_risk_events"], 0)
        self.assertEqual(report["final_max_tt_ms"], 10_000)
        self.assertTrue(report["passed"])
        for cls in ("lateness_explained", "lateness_related", "unexplained"):
            self.assertEqual(report[cls], {"count": 0, "truncated": False,
                                           "windows": []})
        self.assertEqual(json.loads(json.dumps(report)), report)

    def test_mismatch_lists_capped_at_100_with_truncated_flag(self):
        # 105 in-order windows: none dropped; the final watermark puts
        # windows 0..102 (closes before final max - 70_000) at-risk, the
        # two latest windows outside the set.
        events = [make_event(SYM, i * er.WindowMs + 100, 1_000_000 + i, i)
                  for i in range(105)]
        result = sh.shadow_replay(events)
        windows, advisories = actual_from_shadow(result)
        windows = [w._replace(closing_price_micros=w.closing_price_micros + 1)
                   for w in windows]
        compared = cmp.compare(windows, advisories, result)
        report = cmp.build_report(result, compared, Decimal("1e-9"), TOPICS)
        related = report["lateness_related"]
        self.assertEqual(related["count"], 103)
        self.assertTrue(related["truncated"])
        self.assertEqual(len(related["windows"]), 100)
        self.assertEqual(related["windows"][0]["window_start_ms"], 0)
        self.assertEqual(related["windows"][-1]["window_start_ms"], 99 * er.WindowMs)
        unexplained = report["unexplained"]
        self.assertEqual(unexplained["count"], 2)
        self.assertFalse(unexplained["truncated"])
        self.assertEqual(len(unexplained["windows"]), 2)
        # the unexplained class holds the two latest windows (103, 104),
        # sorted by window start
        self.assertEqual(unexplained["windows"][0]["window_start_ms"],
                         103 * er.WindowMs)
        self.assertEqual(unexplained["windows"][0]["parts"],
                         ["closing_price_micros 1000104 != 1000103"])
        self.assertFalse(report["passed"])
        json.loads(json.dumps(report))


if __name__ == "__main__":
    unittest.main(verbosity=2)
