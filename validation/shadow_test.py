#!/usr/bin/env python3
"""Tests for shadow.py.

Keep-all vs drop-model replay, the out-of-orderness drop rule (with its
`<=` boundary), at-risk classification against the final max, and EMA
cross-checks against an independent exact-rational model
(fractions.Fraction), mirroring ema_replay_test.py.

Timelines are chosen so window maxTimestamps land exactly on
`max_tt_so_far - 70_000` boundaries: (w + 1) * 300_000 - 1 + 70_000 =
(w + 1) * 300_000 + 69_999, so a max of 369_999 makes window 0's
maxTimestamp (299_999) exactly equal to the watermark (299_999).
"""

import random
import sys
import unittest
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ema_replay as er  # noqa: E402
import shadow as sh  # noqa: E402


def model_series(closes_micros):
    """Independent exact-rational model per emitted window (prev-zero):
    (ema38, ema100, prev_ema38, prev_ema100, advisory)."""
    ps, pl, out = Fraction(0), Fraction(0), []
    for c in map(Fraction, closes_micros):
        es = (2 * c + 37 * ps) / 39
        el = (2 * c + 99 * pl) / 101
        adv = "BUY" if (es > el and ps <= pl) else "SELL" if (es < el and ps >= pl) else ""
        out.append((es, el, ps, pl, adv))
        ps, pl = es, el
    return out


def frac_to_dec(x):
    with localcontext() as ctx:
        ctx.prec = 60
        return Decimal(x.numerator) / Decimal(x.denominator)


TOL = Decimal("1e-25")  # replay computes at prec 50; the model is exact

SYM = "AAA.NL"


def make_event(symbol, tt_ms, micros, order):
    return sh.ShadowEvent(symbol, tt_ms, micros, order)


def keys(result_dict):
    return set(result_dict)


def assert_rows_match_model(test, rows_in_order, closes_micros):
    """rows_in_order: a symbol's WindowResults sorted by window index."""
    model = model_series(closes_micros)
    test.assertEqual(len(rows_in_order), len(model))
    for i, (row, (es, el, ps, pl, adv)) in enumerate(zip(rows_in_order, model)):
        with test.subTest(window=i):
            test.assertEqual(row.advisory, adv)
            test.assertLess(abs(row.ema38_micros - frac_to_dec(es)), TOL)
            test.assertLess(abs(row.ema100_micros - frac_to_dec(el)), TOL)
            test.assertLess(abs(row.prev_ema38_micros - frac_to_dec(ps)), TOL)
            test.assertLess(abs(row.prev_ema100_micros - frac_to_dec(pl)), TOL)


class LateEventTest(unittest.TestCase):
    """One late event: keep-all keeps it; the drop model skips it."""

    def test_late_event_kept_by_keep_all_dropped_by_model(self):
        events = [
            make_event(SYM, 1_000_000, 10_000_000, 1),   # w3
            make_event(SYM, 150_000, 9_000_000, 2),      # w0, late
        ]
        result = sh.shadow_replay(events)
        # drop rule: w0 maxTimestamp 299_999 <= 1_000_000 - 70_000 = 930_000
        self.assertEqual(keys(result.keep_all), {(SYM, 0), (SYM, 3)})
        self.assertEqual(keys(result.drop_model), {(SYM, 3)})
        self.assertEqual(result.keep_all[(SYM, 0)].close_micros, 9_000_000)
        self.assertEqual(result.keep_all[(SYM, 0)].event_count, 1)
        self.assertEqual(result.total_events, 2)
        self.assertEqual(result.final_max_tt_ms, 1_000_000)
        # the fully-dropped window has no surviving events, so it is not
        # at-risk (at-risk re-tests only surviving events)
        self.assertEqual(result.at_risk_windows, frozenset())
        self.assertEqual(result.at_risk_events, 0)


class BoundaryTest(unittest.TestCase):
    """Window maxTimestamp exactly at the watermark is dropped (<=)."""

    def test_exact_equality_is_dropped(self):
        # max 369_999 -> watermark 299_999; w0 maxTimestamp is exactly 299_999
        events = [
            make_event(SYM, 369_999, 10_000_000, 1),  # w1
            make_event(SYM, 100_000, 9_000_000, 2),   # w0, maxTimestamp == watermark
        ]
        result = sh.shadow_replay(events)
        self.assertEqual(keys(result.keep_all), {(SYM, 0), (SYM, 1)})
        self.assertEqual(keys(result.drop_model), {(SYM, 1)})

    def test_one_ms_above_is_kept(self):
        # max 369_998 -> watermark 299_998; w0 maxTimestamp 299_999 is above
        events = [
            make_event(SYM, 369_998, 10_000_000, 1),
            make_event(SYM, 100_000, 9_000_000, 2),
        ]
        result = sh.shadow_replay(events)
        self.assertEqual(keys(result.drop_model), {(SYM, 0), (SYM, 1)})
        self.assertEqual(result.at_risk_windows, frozenset())  # both clean


class WatermarkAdvanceTest(unittest.TestCase):
    """A dropped event still feeds the running max for later drop tests.

    The running max comes from ALL events (watermarks are generated at
    the source, before the window operator) and is updated before each
    event's drop test, so a chain of drops lands on the formula
    `wmax <= max_so_far - 70_000` against the all-events max.
    """

    def test_dropped_events_advance_watermark_for_subsequent_decisions(self):
        events = [
            make_event(SYM, 1_000_000, 50_000_000, 1),  # w3 keep; max -> 1_000_000
            make_event(SYM, 150_000, 1_000_000, 2),     # w0 drop (299_999 <= 930_000)
            make_event(SYM, 280_000, 2_000_000, 3),     # w0 drop (same window, same max)
            make_event(SYM, 2_000_000, 60_000_000, 4),  # w6 keep; max -> 2_000_000
            make_event(SYM, 1_530_000, 3_000_000, 5),   # w5 drop (1_799_999 <= 1_930_000)
            make_event(SYM, 1_700_000, 4_000_000, 6),   # w5 drop
            make_event(SYM, 1_000_100, 5_000_000, 7),   # w3 drop (1_199_999 <= 1_930_000)
        ]
        result = sh.shadow_replay(events)
        self.assertEqual(keys(result.keep_all),
                         {(SYM, 0), (SYM, 3), (SYM, 5), (SYM, 6)})
        self.assertEqual(keys(result.drop_model), {(SYM, 3), (SYM, 6)})
        # w3 survives with only its kept event; keep-all folds both (the
        # dropped late event even wins the close there)
        self.assertEqual(result.keep_all[(SYM, 3)].event_count, 2)
        self.assertEqual(result.keep_all[(SYM, 3)].close_micros, 5_000_000)
        self.assertEqual(result.drop_model[(SYM, 3)].event_count, 1)
        self.assertEqual(result.drop_model[(SYM, 3)].close_micros, 50_000_000)
        # w5 has only dropped events: keep-all row, no drop-model row
        self.assertEqual(result.keep_all[(SYM, 5)].event_count, 2)
        # kept-early windows can still be at-risk against the FINAL max
        self.assertEqual(result.at_risk_windows, {(SYM, 3)})  # 1_199_999 <= 1_930_000
        self.assertEqual(result.at_risk_events, 1)
        self.assertEqual(result.total_events, 7)
        self.assertEqual(result.final_max_tt_ms, 2_000_000)


class EmptyWindowCarryForwardTest(unittest.TestCase):
    """A fully-dropped window emits no row; the prev pair carries over it."""

    def test_dropped_window_carries_prev_pair(self):
        events = [
            make_event(SYM, 10_000, 15_000_000, 1),    # w0 keep
            make_event(SYM, 700_000, 25_000_000, 2),   # w2 keep; max -> 700_000
            make_event(SYM, 320_000, 99_000_000, 3),   # w1 drop (599_999 <= 630_000)
            make_event(SYM, 760_000, 5_000_000, 4),     # w2 keep
            make_event(SYM, 1_020_000, 7_000_000, 5),  # w3 keep; max -> 1_020_000
        ]
        result = sh.shadow_replay(events)
        self.assertEqual(keys(result.keep_all),
                         {(SYM, 0), (SYM, 1), (SYM, 2), (SYM, 3)})
        self.assertEqual(keys(result.drop_model), {(SYM, 0), (SYM, 2), (SYM, 3)})
        keep_rows = sorted(result.keep_all.values(), key=lambda r: r.window_index)
        drop_rows = sorted(result.drop_model.values(), key=lambda r: r.window_index)
        # keep-all sees w1's 99_000_000 close; the drop model skips it
        assert_rows_match_model(self, keep_rows,
                                [15_000_000, 99_000_000, 5_000_000, 7_000_000])
        assert_rows_match_model(self, drop_rows,
                                [15_000_000, 5_000_000, 7_000_000])
        # the carry is visible directly: w2's prev pair == w0's ema pair
        self.assertEqual(drop_rows[1].prev_ema38_micros, drop_rows[0].ema38_micros)
        self.assertEqual(drop_rows[1].prev_ema100_micros, drop_rows[0].ema100_micros)
        # at-risk: w0 (1 event) and w2 (2 events) close before 1_020_000 - 70_000
        self.assertEqual(result.at_risk_windows, {(SYM, 0), (SYM, 2)})
        self.assertEqual(result.at_risk_events, 3)
        self.assertEqual(result.total_events, 5)


class AtRiskSetTest(unittest.TestCase):
    """At-risk flags surviving events whose windows close before final_max - 70_000."""

    def test_at_risk_flags_clean_absent_and_counts_right(self):
        events = [
            make_event("BB.DE", 50_000, 11_000_000, 1),    # T w0 keep
            make_event(SYM, 100_000, 15_000_000, 2),      # S w0 keep
            make_event(SYM, 1_000_000, 25_000_000, 3),    # S w3 keep; max -> 1_000_000
            make_event(SYM, 120_000, 5_000_000, 4),       # S w0 drop (299_999 <= 930_000)
            make_event(SYM, 2_000_000, 9_000_000, 5),     # S w6 keep; max -> 2_000_000
            make_event("BB.DE", 2_050_000, 13_000_000, 6),  # T w6 keep; max -> 2_050_000
        ]
        result = sh.shadow_replay(events)
        # final max 2_050_000 -> deadline 1_980_000
        self.assertEqual(result.final_max_tt_ms, 2_050_000)
        self.assertEqual(result.at_risk_windows,
                         {(SYM, 0), (SYM, 3), ("BB.DE", 0)})
        self.assertEqual(result.at_risk_events, 3)  # one event per at-risk window
        self.assertEqual(result.total_events, 6)
        # clean windows are absent: both w6 windows close after the deadline
        for clean in [(SYM, 6), ("BB.DE", 6)]:
            self.assertNotIn(clean, result.at_risk_windows)
        # the dropped S w0 event re-appears in keep-all but counts nowhere else
        self.assertEqual(result.keep_all[(SYM, 0)].event_count, 2)
        self.assertEqual(result.drop_model[(SYM, 0)].event_count, 1)


class EmaCrossCheckTest(unittest.TestCase):
    """EMA/advisory values in both models vs the Fraction model."""

    CLOSES = [15_000_000, 25_000_000, 5_000_000, 12_000_000, 30_000_000,
              2_000_000, 8_000_000, 22_000_000, 1_000_000, 40_000_000]

    def events_with_late_w1(self):
        # windows in order, but w1's event arrives last (after w9) -> dropped
        events = []
        order = 0
        for widx, micros in enumerate(self.CLOSES):
            if widx == 1:
                continue
            events.append(make_event(SYM, widx * er.WindowMs + 100, micros, order))
            order += 1
        events.append(make_event(SYM, 1 * er.WindowMs + 50_000, self.CLOSES[1], order))
        return events

    def rows(self, result_dict):
        return sorted(result_dict.values(), key=lambda r: r.window_index)

    def test_keep_all_and_drop_model_match_their_models(self):
        result = sh.shadow_replay(self.events_with_late_w1())
        assert_rows_match_model(self, self.rows(result.keep_all), self.CLOSES)
        assert_rows_match_model(self, self.rows(result.drop_model),
                                self.CLOSES[:1] + self.CLOSES[2:])
        # keep-all emits w1; the drop model does not
        self.assertEqual(keys(result.keep_all),
                         {(SYM, i) for i in range(10)})
        self.assertEqual(keys(result.drop_model),
                         {(SYM, i) for i in range(10) if i != 1})


class RandomizedAgainstSpecFormulaTest(unittest.TestCase):
    """A pseudo-random multi-symbol stream vs a literal spec transcription.

    Reference: update max from ALL events first, then drop iff
    `(tt // WindowMs + 1) * WindowMs - 1 <= max_so_far - 70_000`; at-risk
    is the same test against the final max, over surviving events only.
    """

    def test_matches_reference_decisions_and_counts(self):
        rng = random.Random(20261030)
        symbols = ["AAA.NL", "BB.DE", "CC.FR"]
        events = []
        tts = [rng.randrange(0, 20 * er.WindowMs) for _ in range(300)]
        rng.shuffle(tts)  # out-of-order arrival forces drops
        for i, tt in enumerate(tts):
            events.append(make_event(symbols[i % 3], tt,
                              rng.randrange(1, 40_000_000), i))

        # reference fold
        max_tt = None
        ref_keep, ref_drop = {}, {}  # key -> (close_tt, close_order, micros, count)

        def fold(table, key, ev):
            close_tt, close_order, micros, count = table.get(
                key, (None, None, None, 0))
            count += 1
            if close_tt is None or ev.tt_ms > close_tt or (
                    ev.tt_ms == close_tt and ev.order > close_order):
                table[key] = (ev.tt_ms, ev.order, ev.micros, count)
            else:
                table[key] = (close_tt, close_order, micros, count)

        for ev in events:
            max_tt = ev.tt_ms if max_tt is None or ev.tt_ms > max_tt else max_tt
            widx = ev.tt_ms // er.WindowMs
            fold(ref_keep, (ev.symbol, widx), ev)
            wmax = (widx + 1) * er.WindowMs - 1
            if wmax > max_tt - sh.OutOfOrdernessMs:
                fold(ref_drop, (ev.symbol, widx), ev)

        result = sh.shadow_replay(events)
        self.assertEqual(result.total_events, len(events))
        self.assertEqual(result.final_max_tt_ms, max(tts))
        self.assertEqual(keys(result.keep_all), set(ref_keep))
        self.assertEqual(keys(result.drop_model), set(ref_drop))
        for table, result_dict in ((ref_keep, result.keep_all),
                                  (ref_drop, result.drop_model)):
            for key, (close_tt, close_order, micros, count) in table.items():
                row = result_dict[key]
                self.assertEqual(row.close_micros, micros, key)
                self.assertEqual(row.close_order, close_order, key)
                self.assertEqual(row.event_count, count, key)
        deadline = max(tts) - sh.OutOfOrdernessMs
        ref_at_risk = frozenset(
            key for key in ref_drop
            if (key[1] + 1) * er.WindowMs - 1 <= deadline)
        self.assertEqual(result.at_risk_windows, ref_at_risk)
        self.assertEqual(result.at_risk_events,
                         sum(ref_drop[key][3] for key in ref_at_risk))
        self.assertTrue(result.at_risk_windows)  # the stream actually exercises it
        self.assertTrue(ref_drop.keys() != ref_keep.keys())  # drops happened


class EmptyInputTest(unittest.TestCase):
    def test_empty_input(self):
        result = sh.shadow_replay([])
        self.assertEqual(result.keep_all, {})
        self.assertEqual(result.drop_model, {})
        self.assertEqual(result.at_risk_windows, frozenset())
        self.assertEqual(result.at_risk_events, 0)
        self.assertEqual(result.total_events, 0)
        self.assertIsNone(result.final_max_tt_ms)


class GeneratorInputTest(unittest.TestCase):
    """shadow_replay makes a single pass: a generator works."""

    def test_generator_input(self):
        events = [make_event(SYM, 1_000_000, 10_000_000, 1), make_event(SYM, 150_000, 9_000_000, 2)]
        result = sh.shadow_replay(iter(events))
        self.assertEqual(result.total_events, 2)
        self.assertEqual(keys(result.drop_model), {(SYM, 3)})


if __name__ == "__main__":
    unittest.main(verbosity=2)