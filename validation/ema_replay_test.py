#!/usr/bin/env python3
"""Tests for ema_replay.py.

Cross-checked against an independent exact-rational model
(fractions.Fraction), mirroring validation_generator_test.py.
"""

import sys
import unittest
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ema_replay as er  # noqa: E402


def model_series(closes_micros, init="prev-zero"):
    """Independent exact-rational model per window:
    (ema38, ema100, prev_ema38, prev_ema100, advisory)."""
    ps, pl, out, first = Fraction(0), Fraction(0), [], True
    for c in map(Fraction, closes_micros):
        if first and init == "first-close":
            ps = pl = c
        if first and init == "zero-window":
            es = el = Fraction(0)
        else:
            es = (2 * c + 37 * ps) / 39
            el = (2 * c + 99 * pl) / 101
        adv = "BUY" if (es > el and ps <= pl) else "SELL" if (es < el and ps >= pl) else ""
        out.append((es, el, ps, pl, adv))
        ps, pl, first = es, el, False
    return out


def frac_to_dec(x):
    with localcontext() as ctx:
        ctx.prec = 60
        return Decimal(x.numerator) / Decimal(x.denominator)


TOL = Decimal("1e-25")  # replay computes at prec 50; the model is exact


def build_closes(series):
    """{symbol: [close micros per window]} -> {(symbol, widx): CloseState}.

    Windows with None have no events (gap).  States are built with
    update_close so the fold itself is exercised.
    """
    closes = {}
    for symbol, micros_list in series.items():
        for widx, micros in enumerate(micros_list):
            if micros is None:
                continue
            state = None
            for k in range(3):  # three events per window; close = max trade time
                state = er.update_close(state, widx * er.WindowMs + 100 + k,
                                        micros + (k % 2), order=widx * 10 + k)
            closes[(symbol, widx)] = state
    return closes


class ReplayAgainstModelTest(unittest.TestCase):
    """Multi-symbol, multi-window replay vs the Fraction model (prev-zero)."""

    TOL = TOL

    SERIES = {
        "AAA.NL": [15_000_000, 25_000_000, 5_000_000] + [1] * 40,  # rise then crash
        "ZZZ.DE": [1] * 3 + [20_000_000, 1_000_000, 500_000],       # rise then fall
    }

    def test_sorted_by_symbol_and_window(self):
        closes = build_closes(self.SERIES)
        rows = er.replay(closes)
        keys = [(r.symbol, r.window_index) for r in rows]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(set(keys), set(closes))

    def test_emas_prev_pairs_and_advisories_match_model(self):
        closes = build_closes(self.SERIES)
        rows = er.replay(closes)
        advisories = set()
        for symbol, micros_list in self.SERIES.items():
            sym_rows = [r for r in rows if r.symbol == symbol]
            model = model_series(micros_list)
            self.assertEqual(len(sym_rows), len(model))
            for i, (row, (es, el, ps, pl, adv)) in enumerate(zip(sym_rows, model)):
                with self.subTest(symbol=symbol, window=i):
                    self.assertEqual(row.advisory, adv)
                    self.assertLess(abs(row.ema38_micros - frac_to_dec(es)), self.TOL)
                    self.assertLess(abs(row.ema100_micros - frac_to_dec(el)), self.TOL)
                    self.assertLess(abs(row.prev_ema38_micros - frac_to_dec(ps)), self.TOL)
                    self.assertLess(abs(row.prev_ema100_micros - frac_to_dec(pl)), self.TOL)
                    # prev pair is the previous window's EMA pair (0 at first)
                    if i:
                        self.assertEqual(row.prev_ema38_micros, sym_rows[i - 1].ema38_micros)
                        self.assertEqual(row.prev_ema100_micros, sym_rows[i - 1].ema100_micros)
                    else:
                        self.assertEqual(row.prev_ema38_micros, Decimal(0))
                        self.assertEqual(row.prev_ema100_micros, Decimal(0))
                    advisories.add(row.advisory)
        self.assertIn("BUY", advisories)
        self.assertIn("SELL", advisories)

    def test_prev_zero_first_window_buy_is_inherent(self):
        rows = er.replay(build_closes({"AAA.NL": [10_000_000, 12_000_000]}))
        self.assertEqual(rows[0].advisory, "BUY")  # artifact vs previous pair (0, 0)
        self.assertLess(abs(rows[0].ema38_micros - frac_to_dec(Fraction(10_000_000 * 2) / 39)),
                        self.TOL)
        self.assertLess(abs(rows[0].ema100_micros - frac_to_dec(Fraction(10_000_000 * 2) / 101)),
                        self.TOL)

    def test_close_identity_carried_through(self):
        closes = build_closes({"AAA.NL": [15_000_000, 25_000_000]})
        rows = er.replay(closes)
        self.assertEqual([r.close_micros for r in rows], [15_000_000, 25_000_000])
        self.assertEqual([r.event_count for r in rows], [3, 3])  # events folded in
        self.assertEqual([r.close_order for r in rows], [2, 12])  # last event wins

    def test_default_ema_init_is_prev_zero(self):
        closes = build_closes({"AAA.NL": [15_000_000, 25_000_000]})
        self.assertEqual(er.replay(closes), er.replay(closes, ema_init="prev-zero"))


class WindowGapTest(unittest.TestCase):
    """A window with no events emits no row; the prev pair carries forward."""

    def test_gap_carries_prev_pair(self):
        closes = build_closes({"GAP.X": [15_000_000, None, 25_000_000, None, None, 1]})
        rows = er.replay(closes)
        self.assertEqual([r.window_index for r in rows], [0, 2, 5])  # no rows for 1, 3, 4
        for gap_from, gap_to in ((0, 1), (1, 2)):
            self.assertEqual(rows[gap_to].prev_ema38_micros, rows[gap_from].ema38_micros)
            self.assertEqual(rows[gap_to].prev_ema100_micros, rows[gap_from].ema100_micros)
        # the gap is transparent to the model: it sees the closes in order
        model = model_series([15_000_000, 25_000_000, 1])
        for row, (es, el, ps, pl, adv) in zip(rows, model):
            self.assertEqual(row.advisory, adv)
            self.assertLess(abs(row.ema38_micros - frac_to_dec(es)), TOL)
            self.assertLess(abs(row.ema100_micros - frac_to_dec(el)), TOL)


class UpdateCloseTest(unittest.TestCase):
    """Close selection: max trade time wins, ties to the later order; count grows."""

    def test_first_event(self):
        self.assertEqual(er.update_close(None, 1_000, 5_000_000, 12),
                         er.CloseState(1_000, 5_000_000, 12, 1))

    def test_later_trade_time_wins(self):
        state = er.CloseState(1_000, 5_000_000, 12, 1)
        self.assertEqual(er.update_close(state, 2_000, 7_000_000, 13),
                         er.CloseState(2_000, 7_000_000, 13, 2))

    def test_equal_trade_time_later_order_wins(self):
        state = er.CloseState(1_000, 5_000_000, 12, 1)
        self.assertEqual(er.update_close(state, 1_000, 9_000_000, 20),
                         er.CloseState(1_000, 9_000_000, 20, 2))

    def test_equal_trade_time_lower_order_loses(self):
        state = er.CloseState(1_000, 9_000_000, 20, 3)
        self.assertEqual(er.update_close(state, 1_000, 5_000_000, 12),
                         er.CloseState(1_000, 9_000_000, 20, 4))

    def test_earlier_trade_time_loses(self):
        state = er.CloseState(2_000, 7_000_000, 13, 2)
        self.assertEqual(er.update_close(state, 1_000, 9_000_000, 99),
                         er.CloseState(2_000, 7_000_000, 13, 3))

    def test_event_count_accumulates_through_replacement(self):
        state = None
        for i, micros in enumerate([5, 9, 7, 6]):
            state = er.update_close(state, 1_000, micros, 10 + i)
        self.assertEqual(state, er.CloseState(1_000, 6, 13, 4))


class EmaInitModesTest(unittest.TestCase):
    """The ema_init modes share the recursion; only the first window differs."""

    CLOSES = [10_000_000, 12_000_000] + [1] * 35  # rise, then a long 1-micro crash

    def closes(self):
        return build_closes({"MOD.FR": self.CLOSES})

    def check_model(self, rows, mode):
        for row, (es, el, ps, pl, adv) in zip(rows, model_series(self.CLOSES, init=mode)):
            self.assertEqual(row.advisory, adv)
            self.assertLess(abs(row.ema38_micros - frac_to_dec(es)), Decimal("1e-25"))
            self.assertLess(abs(row.ema100_micros - frac_to_dec(el)), Decimal("1e-25"))
            self.assertLess(abs(row.prev_ema38_micros - frac_to_dec(ps)), Decimal("1e-25"))
            self.assertLess(abs(row.prev_ema100_micros - frac_to_dec(pl)), Decimal("1e-25"))

    def test_prev_zero_default(self):
        rows = er.replay(self.closes(), ema_init="prev-zero")
        self.check_model(rows, "prev-zero")
        self.assertEqual(rows[0].advisory, "BUY")
        self.assertGreater(sum(1 for r in rows if r.advisory == "SELL"), 0)

    def test_first_close(self):
        rows = er.replay(self.closes(), ema_init="first-close")
        self.check_model(rows, "first-close")
        self.assertEqual(rows[0].ema38_micros, Decimal(10_000_000))  # seed = Close
        self.assertEqual(rows[0].ema100_micros, Decimal(10_000_000))
        self.assertEqual(rows[0].advisory, "")     # equal EMAs cannot cross
        self.assertEqual(rows[1].advisory, "BUY")  # price rose 10 -> 12

    def test_zero_window(self):
        rows = er.replay(self.closes(), ema_init="zero-window")
        self.check_model(rows, "zero-window")
        self.assertEqual(rows[0].ema38_micros, Decimal(0))  # literal EMA[s, w_0] = 0
        self.assertEqual(rows[0].advisory, "")
        self.assertEqual(rows[1].advisory, "BUY")  # artifact shifted to window two
        # After the all-zero first window, zero-window is prev-zero over CLOSES[1:].
        for row, (es, el, ps, pl, adv) in zip(rows[1:], model_series(self.CLOSES[1:])):
            self.assertEqual(row.advisory, adv)
            self.assertLess(abs(row.ema38_micros - frac_to_dec(es)), Decimal("1e-25"))
            self.assertLess(abs(row.ema100_micros - frac_to_dec(el)), Decimal("1e-25"))

    def test_modes_share_the_same_recursion(self):
        # Only window 0 is special; from window 1 on, every mode applies the
        # same prev-pair formula.  zero-window emits 0 for window 0, so its
        # tail is prev-zero replayed over CLOSES[1:] (EMA identity only; the
        # close order shifts with the window index).
        def ema_pairs(rows):
            return [(r.ema38_micros, r.ema100_micros,
                     r.prev_ema38_micros, r.prev_ema100_micros, r.advisory) for r in rows]

        zero_window = er.replay(self.closes(), ema_init="zero-window")
        tail = er.replay(build_closes({"MOD.FR": self.CLOSES[1:]}), ema_init="prev-zero")
        self.assertEqual(ema_pairs(zero_window[1:]), ema_pairs(tail))


if __name__ == "__main__":
    unittest.main(verbosity=2)
