#!/usr/bin/env python3
"""Tests for reference.py.

The reference is the CSV generator's offline output (ema_windows.csv +
summary.json); reference.compare_reference checks the shadow replay of the
trade-events topic against it, which isolates the producer (CSV -> topic)
from the processor.  The fixtures run the generator's real run() on a small
synthetic CSV and replay the same events through the shadow, so a passing
case is a contract test between the two sides, not a hand-built table.
Cases: a faithful topic matches on the drop-model side (and on the
keep-all side for a --keep-late-events reference); a changed close price,
a missing event, a missing/extra window, and a downstream EMA shift are
each reported against the right window; a tampered advisory is caught;
references the shadow cannot be compared with (other EMA init, other
watermark delay, absent files) are rejected up front; the report dict
carries the counts and caps the listed windows.
"""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402
import generate_validation_data as gvd  # noqa: E402
import reference as ref  # noqa: E402
import shadow as sh  # noqa: E402
from validation_generator_test import build_csv, data_row  # noqa: E402

CEST = timezone(timedelta(hours=2))
AAA, BBB = "AAA.NL", "BBB.FR"


def epoch_ms(hour, minute, second=0):
    """Trade time on 2021-11-12 (CEST wall clock) as the shadow sees it: epoch ms."""
    moment = datetime(2021, 11, 12, hour, minute, second, tzinfo=CEST)
    return int(moment.timestamp()) * 1000


def hhmmss(hour, minute, second=0):
    return f"{hour:02d}:{minute:02d}:{second:02d}.000"


# (symbol, sectype, (h, m, s), price) in CSV/arrival order.  Source rows start at
# CSV line 14 (the generator's `line`), which only matters for tie-breaks.
SCENARIO = [
    (AAA, "E", (8, 1), "10.000000"),
    (AAA, "E", (8, 2), "20.000000"),
    (AAA, "E", (8, 3), "15.000000"),      # window 08:00 closes at 15 (3 events)
    (AAA, "E", (8, 17), "25.000000"),     # window 08:15: out-of-order arrival,
    (AAA, "E", (8, 16), "30.000000"),     # close is 25 (max trade time)
    (AAA, "E", (8, 31), "40.000000"),     # window 08:30 (before 09:00 moves the watermark past it)
    (BBB, "I", (9, 0), "1.500000"),
]
# 10:10 pushes the watermark to 10:08:50, so the 10:01 arrival is late: the
# generator drops it, the topic still carries it.
LATE_TAIL = [
    (BBB, "I", (10, 0, 30), "5.000000"),
    (BBB, "I", (10, 10), "6.000000"),
    (BBB, "I", (10, 1), "7.000000"),
]


def shadow_events(specs, first_row=14):
    """The ShadowEvents the producer would publish for these specs, order = row."""
    return [sh.ShadowEvent(symbol, epoch_ms(*when), int(round(float(price) * 1_000_000)),
                           first_row + i)
            for i, (symbol, _sect, when, price) in enumerate(specs)]


class ReferenceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def generate(self, specs, **kwargs):
        """Run the real generator on a synthetic CSV; returns its output dir."""
        rows = [data_row(symbol, sect, hhmmss(*when), price)
                for symbol, sect, when, price in specs]
        csv_path = self.dir / "synthetic.csv"
        csv_path.write_text(build_csv(rows), newline="", encoding="utf-8")
        out_dir = self.dir / "out"
        with contextlib.redirect_stdout(io.StringIO()):
            gvd.run(csv_path=csv_path, out_dir=out_dir, compute_md5=False, **kwargs)
        return out_dir

    def compare(self, reference_dir, events):
        return ref.compare_reference(sh.shadow_replay(events), ref.load_reference(reference_dir))


class FaithfulTopicTest(ReferenceTestCase):
    def test_scenario_matches_on_the_drop_model_side(self):
        result = self.compare(self.generate(SCENARIO), shadow_events(SCENARIO))
        self.assertEqual(result.side, "drop_model")
        self.assertEqual((result.reference_windows, result.shadow_windows, result.matched),
                         (4, 4, 4))
        self.assertEqual(result.mismatches, [])
        self.assertTrue(result.passed)

    def test_dropped_late_event_is_reproduced_by_the_drop_model(self):
        specs = SCENARIO + LATE_TAIL
        result = self.compare(self.generate(specs), shadow_events(specs))
        self.assertTrue(result.passed, result.mismatches)

    def test_keep_late_events_reference_compares_on_the_keep_all_side(self):
        specs = SCENARIO + LATE_TAIL
        out_dir = self.generate(specs, out_of_orderness_ms=None)
        result = self.compare(out_dir, shadow_events(specs))
        self.assertEqual(result.side, "keep_all")
        self.assertTrue(result.passed, result.mismatches)


class UnfaithfulTopicTest(ReferenceTestCase):
    def setUp(self):
        super().setUp()
        self.out_dir = self.generate(SCENARIO)

    def mismatches(self, events):
        result = self.compare(self.out_dir, events)
        self.assertFalse(result.passed)
        return {(m.symbol, m.window_start_ms): m.parts for m in result.mismatches}

    def test_changed_close_price_is_reported_on_its_window(self):
        events = shadow_events(SCENARIO)
        events[2] = events[2]._replace(micros=15_000_001)                # the 08:03 close
        found = self.mismatches(events)
        parts = found[(AAA, epoch_ms(8, 0))]
        self.assertTrue(any(p.startswith("close_micros") for p in parts), parts)

    def test_missing_event_changes_the_event_count(self):
        events = shadow_events(SCENARIO)
        del events[0]                                                    # 08:01 never published
        parts = self.mismatches(events)[(AAA, epoch_ms(8, 0))]
        self.assertEqual([p for p in parts if p.startswith("event_count")],
                         ["event_count 2 != 3"])

    def test_window_missing_from_the_topic(self):
        events = [e for e in shadow_events(SCENARIO) if e.symbol != BBB]
        parts = self.mismatches(events)[(BBB, epoch_ms(9, 0))]
        self.assertEqual(parts, ("missing from the topic",))

    def test_window_unknown_to_the_reference(self):
        events = shadow_events(SCENARIO) + [sh.ShadowEvent("CCC.DE", epoch_ms(11, 0), 1_000_000, 99)]
        parts = self.mismatches(events)[("CCC.DE", epoch_ms(11, 0))]
        self.assertEqual(parts, ("not in the reference",))

    def test_downstream_ema_shift_without_a_close_change(self):
        events = shadow_events(SCENARIO)
        events[2] = events[2]._replace(micros=16_000_000)                # 08:00 close 15 -> 16
        found = self.mismatches(events)
        later = found[(AAA, epoch_ms(8, 15))]                            # same close, shifted EMA
        self.assertTrue(any(p.startswith("ema38_micros") for p in later), later)
        self.assertFalse(any(p.startswith("close_micros") for p in later), later)

    def test_tampered_reference_advisory_is_reported(self):
        reference = ref.load_reference(self.out_dir)
        key = next(k for k, w in reference.windows.items() if w.advisory)
        reference.windows[key] = reference.windows[key]._replace(advisory="SELL")
        result = ref.compare_reference(sh.shadow_replay(shadow_events(SCENARIO)), reference)
        self.assertFalse(result.passed)
        self.assertTrue(any(p.startswith("advisory") for m in result.mismatches for p in m.parts))


class LoadReferenceTest(ReferenceTestCase):
    def test_loads_windows_and_the_spec_flags(self):
        reference = ref.load_reference(self.generate(SCENARIO))
        self.assertEqual(len(reference.windows), 4)
        self.assertEqual(reference.out_of_orderness_ms, 70_000)
        window = reference.windows[(AAA, epoch_ms(8, 15))]
        self.assertEqual((window.event_count, window.close_micros), (2, 25_000_000))

    def test_rejects_a_reference_generated_with_another_ema_init(self):
        out_dir = self.generate(SCENARIO, ema_init="first-close")
        with self.assertRaisesRegex(ValueError, "ema_init"):
            ref.load_reference(out_dir)

    def test_rejects_a_reference_generated_with_another_watermark_delay(self):
        out_dir = self.generate(SCENARIO, out_of_orderness_ms=1_000)
        with self.assertRaisesRegex(ValueError, "out_of_orderness_ms"):
            ref.load_reference(out_dir)

    def test_missing_files_point_at_the_generator(self):
        with self.assertRaisesRegex(FileNotFoundError, "generate-reference"):
            ref.load_reference(self.dir / "nowhere")


class ReportTest(ReferenceTestCase):
    def test_report_carries_counts_and_pass_flag(self):
        out_dir = self.generate(SCENARIO)
        result = self.compare(out_dir, shadow_events(SCENARIO))
        report = ref.reference_report(result, out_dir)
        self.assertEqual(report["dir"], str(out_dir))
        self.assertEqual(report["side"], "drop_model")
        self.assertEqual((report["reference_windows"], report["shadow_windows"], report["matched"]),
                         (4, 4, 4))
        self.assertEqual(report["mismatches"]["count"], 0)
        self.assertTrue(report["passed"])
        json.dumps(report)                                               # JSON-able

    def test_report_caps_the_listed_windows(self):
        out_dir = self.generate(SCENARIO)
        result = self.compare(out_dir, [])                               # empty topic: all missing
        report = ref.reference_report(result, out_dir)
        self.assertEqual(report["mismatches"]["count"], 4)
        self.assertFalse(report["passed"])
        self.assertLessEqual(len(report["mismatches"]["windows"]), config.MAX_REPORT_MISMATCHES)


if __name__ == "__main__":
    unittest.main()
