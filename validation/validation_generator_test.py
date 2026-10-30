#!/usr/bin/env python3
"""Tests for generate_validation_data.py.

Parser cases mirror services/data-producer/event_test.go (a representative
subset; the full tables live in the Go tests).  End-to-end scenarios are
checked against an independent exact-rational model (fractions.Fraction).
"""

import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_validation_data as gvd  # noqa: E402

# The exact header line from the dataset (line 12 of the CSV).
HEADER = (
    "ID,SecType,Date,Time,Ask,Ask volume,Bid,Bid volume,Ask time,"
    "Day's high ask,Close,Currency,Day's high ask time,Day's high,ISIN,"
    "Auction price,Day's low ask,Day's low,Day's low ask time,Open,"
    "Nominal value,Last,Last volume,Trading time,Total volume,Mid price,"
    "Trading date,Profit,Current price,Related indices,Day high bid time,"
    " Day low bid time,Open Time, Last trade time, Close Time, Day high Time,"
    " Day low Time,Bid time,Auction Time"
)
N_FIELDS = 40   # data rows carry one more trailing field than the 39 header names
PRICE_SCALE = 1_000_000


def data_row(symbol, sect, time_s, last, tdate="", date_col="12-11-2021"):
    fields = [""] * N_FIELDS
    fields[0], fields[1], fields[2], fields[3] = symbol, sect, date_col, time_s
    fields[21], fields[23], fields[26] = last, time_s, tdate
    return ",".join(fields)


def stale_tail_rows():
    """The day-10 shape: genuine events, then a stale 01:00 update at the end."""
    return [data_row("EEE.FR", "I", "10:00:30.000", "5.000000"),
            data_row("EEE.FR", "I", "10:10:00.000", "6.000000"),
            data_row("EEE.FR", "I", "01:00:00.000", "99.000000")]


def build_csv(rows):
    """CSV shaped like the dataset: preamble 1-11, header at 12, units at 13."""
    lines = [f"# preamble {i}" for i in range(1, 12)] + [HEADER, "#units"] + rows
    return "\r\n".join(lines) + "\r\n"


def tt(hour, minute, second=0, ms=0, day=date(2021, 11, 12)):
    """Trade time in the generator's ms scale (ordinal-day based, wall clock)."""
    return day.toordinal() * gvd.MS_PER_DAY + ((hour * 60 + minute) * 60 + second) * 1000 + ms


def model_series(closes_micros, init="prev-zero"):
    """Independent exact-rational model: (ema38, ema100, advisory, first) per window."""
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
        out.append((es, el, adv, first))
        ps, pl, first = es, el, False
    return out


def frac_to_dec(x):
    with localcontext() as ctx:
        ctx.prec = 60
        return Decimal(x.numerator) / Decimal(x.denominator)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class ParsePriceMicrosTest(unittest.TestCase):
    def test_accepted(self):
        for text, micros in {
            "1": 1_000_000, "123.45": 123_450_000, "57.117900": 57_117_900,
            "0.000001": 1, "00057.117900": 57_117_900, " 57.117900\t": 57_117_900,
            "18446744073709.551615": 2**64 - 1,
        }.items():
            with self.subTest(text=text):
                self.assertEqual(gvd.parse_price_micros(text), micros)

    def test_rejected(self):
        for text in ["-1", "-0.000001", "+1", "NaN", "1e3", ".5", "1.", "1.2.3",
                     "1,000", "1 2", "1.0000001", "18446744073709.551616",
                     "18446744073710", "18446744073709551616"]:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    gvd.parse_price_micros(text)

    def test_zero_parses_but_is_skipped_upstream(self):
        # event.go returns skip=true for zero, not an error.
        self.assertEqual(gvd.parse_price_micros("0"), 0)
        self.assertEqual(gvd.parse_price_micros("0.000000"), 0)


class ParseTradeDtTest(unittest.TestCase):
    def test_accepted(self):
        for d, t in [("12-11-2021", "07:00:00.123"), ("29-02-2020", "23:59:59.999")]:
            with self.subTest(date=d, time=t):
                ordinal, sod, ms = gvd.parse_trade_dt(d, t)
                dt = datetime.strptime(d + " " + t, "%d-%m-%Y %H:%M:%S.%f")
                self.assertEqual(date.fromordinal(ordinal), dt.date())
                self.assertEqual(sod, dt.hour * 3600 + dt.minute * 60 + dt.second)
                self.assertEqual(ms, dt.microsecond // 1000)

    def test_rejected(self):
        for d, t in [("", "07:00:00.000"), ("12-11-2021", ""),
                     ("2021-11-12", "07:00:00.000"), ("31-11-2021", "07:00:00.000"),
                     ("29-02-2021", "07:00:00.000"), ("12-11-2021", "24:00:00.000"),
                     ("12-11-2021", "07:60:00.000"), ("12-11-2021", "07:00:60.000"),
                     ("12-11-2021", "07:00:00"), ("12-11-2021", "07:00:00.1234"),
                     ("12-11-2021", "7:00:00.000"), ("1-11-2021", "07:00:00.000")]:
            with self.subTest(date=d, time=t):
                with self.assertRaises(ValueError):
                    gvd.parse_trade_dt(d, t)


class HelpersTest(unittest.TestCase):
    def test_micros_to_price(self):
        self.assertEqual(gvd.micros_to_price(57_117_900), "57.117900")
        self.assertEqual(gvd.micros_to_price(1), "0.000001")
        self.assertEqual(gvd.micros_to_price(0), "0.000000")

    def test_window_bounds(self):
        base = date(2021, 11, 12).toordinal() * 288
        start, end = gvd.window_bounds(base + 96)
        self.assertTrue(start.endswith("T08:00") and end.endswith("T08:05"))
        start, end = gvd.window_bounds(base + 287)
        self.assertTrue(start.endswith("T23:55"))
        self.assertEqual(end, "2021-11-13T00:00")  # last window rolls to the next date


class IsLateTest(unittest.TestCase):
    """Flink drops an event once the watermark (max trade time - delay - 1 ms)
    reaches its window's last timestamp (allowed lateness 0)."""

    DELAY = 70_000  # StreamingProcessor.OutOfOrdernessAllowance

    def test_late_exactly_when_watermark_reaches_window_end(self):
        event = tt(8, 3)  # window 08:00-08:05, last timestamp 08:04:59.999
        self.assertFalse(gvd.is_late(event, tt(8, 6, 9, 999), self.DELAY))
        self.assertTrue(gvd.is_late(event, tt(8, 6, 10), self.DELAY))

    def test_window_start_belongs_to_the_new_window(self):
        event = tt(8, 5)  # first millisecond of window 08:05-08:10
        self.assertFalse(gvd.is_late(event, tt(8, 11, 9, 999), self.DELAY))
        self.assertTrue(gvd.is_late(event, tt(8, 11, 10), self.DELAY))

    def test_event_ahead_of_the_watermark_is_not_late(self):
        self.assertFalse(gvd.is_late(tt(9, 0), tt(8, 0), self.DELAY))


class SyntheticCsvTestCase(unittest.TestCase):
    """Runs the generator on a CSV built from `rows` into a temp directory."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out_dir = Path(self.tmp.name) / "out"

    def write_csv(self, rows):
        csv_path = Path(self.tmp.name) / "synthetic.csv"
        csv_path.write_text(build_csv(rows), newline="", encoding="utf-8")
        return csv_path

    def run_csv(self, rows, **kwargs):
        return gvd.run(csv_path=self.write_csv(rows), out_dir=self.out_dir,
                       compute_md5=False, **kwargs)


class EndToEndTest(SyntheticCsvTestCase):
    """Full pipeline on synthetic CSVs, checked against model_series."""

    def test_scenario_against_rational_model(self):
        rows, closes = [], []
        # Window 08:00-08:05 closes at 15 (last price by trade time, line 16);
        # its first event has an empty Trading date (system Date fallback).
        rows += [data_row("AAA.NL", "E", "08:01:00.000", "10.000000"),
                 data_row("AAA.NL", "E", "08:02:00.000", "20.000000", tdate="12-11-2021"),
                 data_row("AAA.NL", "E", "08:03:00.000", "15.000000")]
        closes.append(15_000_000)
        # Rows the producer skips or would send to the DLQ topic.
        rows += [data_row("BBB.FR", "I", "09:00:00.000", ""),
                 data_row("BBB.FR", "I", "09:01:00.000", "0.000000"),
                 data_row("BBB.FR", "I", "09:02:00.000", "-1.000000"),
                 data_row("BBB.FR", "I", "09:03:00.000", "1.0000001"),
                 data_row("BBB.FR", "X", "09:04:00.000", "1.000000"),
                 data_row("BBB.FR", "I", "09:00:00", "1.000000"),
                 data_row("BBB.FR", "I", "09:06:00.000", "1.000000", date_col=""),
                 data_row("", "I", "09:07:00.000", "1.000000")]
        # Window 08:15-08:20: out-of-order arrival; Close is 25 (max trade
        # time at 08:17), not the last-arrived 30 -> arrival_close_differs=1.
        rows += [data_row("AAA.NL", "E", "08:17:00.000", "25.000000"),
                 data_row("AAA.NL", "E", "08:16:00.000", "30.000000")]
        closes.append(25_000_000)
        # Crash: 5.0, then 1 micro for 40 windows -> exactly one SELL crossover.
        rows.append(data_row("AAA.NL", "E", "08:31:00.000", "5.000000"))
        closes.append(5_000_000)
        base = datetime(2021, 11, 12, 8, 31)
        for i in range(1, 41):
            t = base + timedelta(minutes=5 * i)
            rows.append(data_row("AAA.NL", "E", t.strftime("%H:%M:%S.000"), "0.000001"))
            closes.append(1)

        summary = self.run_csv(rows)
        self.assertEqual(summary["rows"], {
            "data_rows": len(rows), "price_rows_last_nonempty": 53,
            "skipped_no_last": 1, "skipped_zero_last": 1,
            "rejected_would_go_to_dlq": {"bad_price": 2, "empty_symbol": 1,
                                         "bad_sectype": 1, "missing_date": 1,
                                         "bad_trade_time": 1},
            "price_events_accepted": 46,
            "dropped_late_by_watermark": 0,
        })
        ema_rows = read_csv(self.out_dir / "ema_windows.csv")
        adv_rows = read_csv(self.out_dir / "advisories.csv")
        self.assertEqual([r["close_micros"] for r in ema_rows], [str(c) for c in closes])
        self.assertTrue(ema_rows[0]["window_start"].endswith("T08:00"))

        # Close selection and arrival-order divergence.
        self.assertEqual(ema_rows[0]["close_source_row"], "16")
        self.assertEqual(ema_rows[1]["close_micros"], "25000000")
        self.assertEqual(ema_rows[1]["close_source_row"], "25")  # 08:17 row
        self.assertEqual(ema_rows[1]["arrival_close_differs"], "1")
        self.assertEqual(ema_rows[0]["arrival_close_differs"], "0")

        # EMAs, advisories, and previous pairs match the rational model.
        for i, (row, (es, el, adv, first)) in enumerate(zip(ema_rows, model_series(closes))):
            with self.subTest(window=i):
                self.assertEqual(row["advisory"], adv)
                self.assertLess(abs(Decimal(row["ema38_micros"]) - frac_to_dec(es)), Decimal("1e-6"))
                self.assertLess(abs(Decimal(row["ema100_micros"]) - frac_to_dec(el)), Decimal("1e-6"))
                self.assertEqual(
                    row["prev_ema38_micros"],
                    "0.000000000" if first else ema_rows[i - 1]["ema38_micros"])

        # One first-window BUY artifact, exactly one SELL, files consistent.
        self.assertEqual([r["advice"] for r in adv_rows],
                         [r["advisory"] for r in ema_rows if r["advisory"]])
        self.assertEqual(adv_rows[0]["advice"], "BUY")
        self.assertEqual(adv_rows[0]["is_first_window"], "1")
        self.assertEqual(len([r for r in adv_rows if r["advice"] == "SELL"]), 1)

    def test_boundaries_ties_and_multi_day(self):
        rows = [  # same trade time twice: tie breaks to the later source row
            data_row("CCC.ETR", "E", "07:59:59.999", "1.000000"),
            data_row("CCC.ETR", "E", "07:59:59.999", "2.000000"),
            data_row("CCC.ETR", "E", "08:00:00.000", "3.000000"),  # 1 ms -> next window
            data_row("CCC.ETR", "E", "23:57:30.000", "4.000000"),
            data_row("CCC.ETR", "E", "00:01:00.000", "5.000000",
                     tdate="13-11-2021", date_col="13-11-2021"),  # next trading day
        ]
        summary = self.run_csv(rows)
        rows_out = read_csv(self.out_dir / "ema_windows.csv")
        self.assertEqual(len(rows_out), 4)
        self.assertEqual(rows_out[0]["close_micros"], "2000000")
        self.assertEqual(rows_out[0]["close_source_row"], "15")  # tie -> later row
        self.assertTrue(rows_out[0]["window_start"].endswith("T07:55"))
        self.assertTrue(rows_out[1]["window_start"].endswith("T08:00"))
        self.assertEqual(rows_out[2]["window_end"], "2021-11-13T00:00")
        self.assertEqual(rows_out[3]["window_start"], "2021-11-13T00:00")
        self.assertEqual(summary["diagnostics"]["late_events"], 0)


class LateEventDropTest(SyntheticCsvTestCase):
    """Mirrors StreamingProcessor: one global watermark (max trade time seen
    - 70 s - 1 ms) across all symbols, no allowed lateness, late events dropped."""

    def window_starts(self):
        return [r["window_start"][-5:] for r in read_csv(self.out_dir / "ema_windows.csv")]

    def test_other_symbol_advances_the_watermark_at_the_exact_boundary(self):
        summary = self.run_csv([
            data_row("AAA.NL", "E", "08:01:00.000", "10.000000"),
            data_row("BBB.NL", "E", "08:06:09.999", "1.000000"),   # watermark 08:04:59.998
            data_row("AAA.NL", "E", "08:02:00.000", "20.000000"),  # window 08:00 still open
            data_row("BBB.NL", "E", "08:06:10.000", "2.000000"),   # watermark 08:04:59.999
            data_row("AAA.NL", "E", "08:03:00.000", "30.000000"),  # window 08:00 closed: dropped
        ])
        aaa = read_csv(self.out_dir / "ema_windows.csv")[0]
        self.assertEqual(aaa["symbol"], "AAA.NL")
        self.assertEqual((aaa["event_count"], aaa["close_micros"], aaa["close_source_row"]),
                         ("2", "20000000", "16"))
        self.assertEqual(summary["rows"]["price_events_accepted"], 5)
        self.assertEqual(summary["rows"]["dropped_late_by_watermark"], 1)

    def test_stale_tail_event_does_not_become_the_first_window(self):
        summary = self.run_csv(stale_tail_rows())
        self.assertEqual(self.window_starts(), ["10:00", "10:10"])
        adv_rows = read_csv(self.out_dir / "advisories.csv")
        self.assertEqual((adv_rows[0]["window_start"][-5:], adv_rows[0]["is_first_window"]),
                         ("10:00", "1"))
        self.assertEqual(summary["rows"]["dropped_late_by_watermark"], 1)

    def test_symbol_with_only_late_events_has_no_output(self):
        summary = self.run_csv([data_row("EEE.FR", "I", "10:00:30.000", "5.000000"),
                                data_row("GGG.FR", "I", "01:00:00.000", "7.000000")])
        self.assertEqual({r["symbol"] for r in read_csv(self.out_dir / "ema_windows.csv")},
                         {"EEE.FR"})
        self.assertEqual(summary["symbols"]["with_price_events"], 1)
        self.assertEqual(summary["rows"]["dropped_late_by_watermark"], 1)

    def test_disabled_keeps_the_stale_event_as_first_window(self):
        summary = self.run_csv(stale_tail_rows(), out_of_orderness_ms=None)
        self.assertEqual(self.window_starts(), ["01:00", "10:00", "10:10"])
        adv_rows = read_csv(self.out_dir / "advisories.csv")
        self.assertEqual((adv_rows[0]["window_start"][-5:], adv_rows[0]["is_first_window"]),
                         ("01:00", "1"))
        self.assertEqual(summary["rows"]["dropped_late_by_watermark"], 0)
        self.assertIsNone(summary["spec"]["out_of_orderness_ms"])


class LateEventDropCliTest(SyntheticCsvTestCase):
    def run_cli(self, *flags):
        csv_path = self.write_csv(stale_tail_rows())
        gvd.main(["--csv", str(csv_path), "--out", str(self.out_dir), "--no-md5", *flags])
        windows = [r["window_start"][-5:] for r in read_csv(self.out_dir / "ema_windows.csv")]
        with open(self.out_dir / "summary.json", encoding="utf-8") as f:
            return windows, json.load(f)

    def parse_error(self, *flags):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            self.run_cli(*flags)
        return stderr.getvalue()

    def test_default_follows_the_processor_70_seconds(self):
        windows, summary = self.run_cli()
        self.assertEqual(windows, ["10:00", "10:10"])
        self.assertEqual(summary["spec"]["out_of_orderness_ms"], 70_000)

    def test_keep_late_events(self):
        windows, summary = self.run_cli("--keep-late-events")
        self.assertEqual(windows, ["01:00", "10:00", "10:10"])
        self.assertIsNone(summary["spec"]["out_of_orderness_ms"])

    def test_custom_out_of_orderness_seconds(self):
        windows, summary = self.run_cli("--out-of-orderness-seconds", "86400")  # 9 h late < 24 h
        self.assertEqual(windows, ["01:00", "10:00", "10:10"])
        self.assertEqual(summary["spec"]["out_of_orderness_ms"], 86_400_000)

    def test_negative_out_of_orderness_is_rejected(self):
        self.assertIn("finite number >= 0", self.parse_error("--out-of-orderness-seconds", "-1"))

    def test_keep_late_events_conflicts_with_out_of_orderness_seconds(self):
        self.assertIn("not allowed with argument",
                      self.parse_error("--keep-late-events", "--out-of-orderness-seconds", "70"))


class EmaInitModesTest(unittest.TestCase):
    """The --ema-init modes share the recursion; only the first window differs."""

    CLOSES = [10_000_000, 12_000_000] + [1] * 35  # rise, then a long 1-micro crash

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def run_mode(self, mode):
        base = datetime(2021, 11, 12, 10, 1)
        rows = [data_row("MOD.FR", "I",
                         (base + timedelta(minutes=6 * i)).strftime("%H:%M:%S.000"),
                         f"{c // PRICE_SCALE}.{c % PRICE_SCALE:06d}")
                for i, c in enumerate(self.CLOSES)]
        csv_path = Path(self.tmp.name) / f"{mode}.csv"
        csv_path.write_text(build_csv(rows), newline="", encoding="utf-8")
        out_dir = Path(self.tmp.name) / mode
        summary = gvd.run(csv_path=csv_path, out_dir=out_dir, compute_md5=False, ema_init=mode)
        return read_csv(out_dir / "ema_windows.csv"), summary

    def check_model(self, rows, mode):
        for row, (es, el, adv, _) in zip(rows, model_series(self.CLOSES, init=mode)):
            self.assertEqual(row["advisory"], adv)
            self.assertLess(abs(Decimal(row["ema38_micros"]) - frac_to_dec(es)), Decimal("1e-6"))
            self.assertLess(abs(Decimal(row["ema100_micros"]) - frac_to_dec(el)), Decimal("1e-6"))

    def test_prev_zero_default(self):
        rows, summary = self.run_mode("prev-zero")
        self.check_model(rows, "prev-zero")
        self.assertEqual(rows[0]["ema38_micros"], "512820.512820513")  # 10e6*2/39
        self.assertEqual(rows[0]["advisory"], "BUY")  # artifact vs previous pair (0, 0)
        self.assertGreater(summary["advisories"]["sell"], 0)  # crash tail crosses down

    def test_first_close(self):
        rows, summary = self.run_mode("first-close")
        self.check_model(rows, "first-close")
        self.assertEqual(rows[0]["ema38_micros"], "10000000.000000000")  # seed = Close
        self.assertEqual(rows[0]["advisory"], "")     # equal EMAs cannot cross
        self.assertEqual(rows[1]["advisory"], "BUY")  # price rose 10 -> 12
        self.assertEqual(summary["advisories"]["buy_first_window"], 0)
        self.assertGreater(summary["advisories"]["sell"], 0)

    def test_zero_window(self):
        rows, summary = self.run_mode("zero-window")
        self.check_model(rows, "zero-window")
        self.assertEqual(rows[0]["ema38_micros"], "0.000000000")
        self.assertEqual(rows[0]["advisory"], "")
        self.assertEqual(rows[1]["ema38_micros"], "615384.615384615")  # 12e6*2/39
        self.assertEqual(rows[1]["advisory"], "BUY")  # artifact shifted to window two
        self.assertEqual(summary["advisories"]["buy_first_window"], 0)
        # After the all-zero first window, zero-window is prev-zero over CLOSES[1:].
        for row, (es, el, adv, _) in zip(rows[1:], model_series(self.CLOSES[1:])):
            self.assertEqual(row["advisory"], adv)
            self.assertLess(abs(Decimal(row["ema38_micros"]) - frac_to_dec(es)), Decimal("1e-6"))


if __name__ == "__main__":
    unittest.main(verbosity=2)