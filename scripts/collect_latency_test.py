#!/usr/bin/env python3
"""Tests for collect-latency.py: the trigger scan, the join and the statistics."""

import contextlib
import csv
import importlib.util
import inspect
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from google.protobuf.timestamp_pb2 import Timestamp

SCRIPT = Path(__file__).resolve().parent / "collect-latency.py"


def load_module():
    spec = importlib.util.spec_from_file_location("collect_latency", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collector = load_module()
import bindings  # noqa: E402  (collect-latency.py put validation/ on sys.path)
import config  # noqa: E402

WINDOW = 300_000
ALLOWANCE = 70_000
B = 3_000_000  # a window boundary: a multiple of WINDOW
END = B + WINDOW


class TriggerTimesTest(unittest.TestCase):
    def scan(self, events):
        return collector.trigger_times(events, WINDOW, ALLOWANCE)

    def test_boundary_fires_exactly_at_end_plus_allowance(self):
        events = [(B + 10, 100), (B + 299_999, 200), (B + 369_999, 300), (B + 370_000, 400)]
        self.assertEqual(self.scan(events), {B: 200, B + WINDOW: 400})

    def test_one_event_crossing_several_boundaries(self):
        events = [(B + 10, 1), (B + 10 + 3 * WINDOW + ALLOWANCE, 2)]
        self.assertEqual(self.scan(events),
                         {B: 2, B + WINDOW: 2, B + 2 * WINDOW: 2, B + 3 * WINDOW: 2})

    def test_late_events_do_not_move_the_maximum(self):
        events = [(B + 10, 100), (B + 299_999, 200), (B + 100, 300), (B + 370_000, 400)]
        self.assertEqual(self.scan(events), {B: 200, B + WINDOW: 400})

    def test_first_event_triggers_nothing(self):
        self.assertEqual(self.scan([(B + 5, 100)]), {})

    def test_accepts_a_generator(self):
        self.assertEqual(self.scan(iter([(B + 10, 1), (B + WINDOW + ALLOWANCE, 2)])),
                         {B: 2, B + WINDOW: 2})


class StatisticsTest(unittest.TestCase):
    def test_percentile_interpolates_between_closest_ranks(self):
        self.assertEqual(collector.percentile([10, 20, 30, 40], 50), 25.0)
        self.assertAlmostEqual(collector.percentile([1, 2, 3, 4, 5], 95), 4.8)

    def test_percentile_of_one_value_and_the_extremes(self):
        self.assertEqual(collector.percentile([7], 99), 7)
        self.assertEqual(collector.percentile([3, 9, 12], 0), 3)
        self.assertEqual(collector.percentile([3, 9, 12], 100), 12)

    def test_summarize(self):
        summary = collector.summarize([5, 1, 4, 2, 3])
        self.assertEqual(summary["count"], 5)
        self.assertAlmostEqual(summary["mean_ms"], 3.0)
        self.assertAlmostEqual(summary["p50_ms"], 3.0)
        self.assertAlmostEqual(summary["p95_ms"], 4.8)
        self.assertAlmostEqual(summary["p99_ms"], 4.96)
        self.assertEqual(summary["max_ms"], 5)

    def test_summarize_of_nothing_has_a_zero_count_and_no_percentiles(self):
        summary = collector.summarize([])
        self.assertEqual(summary["count"], 0)
        for key in ("mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"):
            self.assertIsNone(summary[key])


def stamps_for(symbol="IUSAC.FR", end=END, t1=1500, t2=1502, t3=1503, advisory=False):
    return [collector.Stamp(symbol, end, "fired", t1, False),
            collector.Stamp(symbol, end, "ema", t2, False),
            collector.Stamp(symbol, end, "crossover", t3, advisory)]


class BuildRowsTest(unittest.TestCase):
    def test_stage_values_from_the_stamps(self):
        rows, coverage = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000})
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["t0_ms"], row["t1_ms"], row["t2_ms"], row["t3_ms"], row["t4e_ms"]),
                         (1000, 1500, 1502, 1503, 1510))
        self.assertEqual(row["ingest_to_fire"], 500)
        self.assertEqual(row["ema_compute"], 2)
        self.assertEqual(row["advisory_compute"], 1)
        self.assertEqual(row["ema_publish"], 8)
        self.assertEqual(row["end_to_end_ema"], 510)
        self.assertFalse(row["advisory_emitted"])
        self.assertIsNone(row["t4a_ms"])
        self.assertIsNone(row["advisory_publish"])
        self.assertIsNone(row["end_to_end_advisory"])
        self.assertEqual(coverage["measured"], 1)
        self.assertEqual(coverage["advisories"], 0)

    def test_an_advisory_adds_its_own_stages(self):
        rows, coverage = collector.build_rows(
            stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
            [("IUSAC.FR", END, 1512)], {END: 1000})
        row = rows[0]
        self.assertTrue(row["advisory_emitted"])
        self.assertEqual(row["t4a_ms"], 1512)
        self.assertEqual(row["advisory_publish"], 9)
        self.assertEqual(row["end_to_end_advisory"], 512)
        self.assertEqual(coverage["advisories"], 1)

    def test_rows_are_sorted_by_window_end_then_symbol(self):
        stamps = (stamps_for("B.FR", END + WINDOW) + stamps_for("B.FR", END)
                  + stamps_for("A.FR", END))
        landed = [("B.FR", END + WINDOW, 1510), ("B.FR", END, 1510), ("A.FR", END, 1510)]
        rows, _ = collector.build_rows(stamps, landed, [], {END: 1000, END + WINDOW: 1000})
        self.assertEqual([(r["window_end_ms"], r["symbol"]) for r in rows],
                         [(END, "A.FR"), (END, "B.FR"), (END + WINDOW, "B.FR")])

    def test_duplicates_keep_the_first_and_are_counted(self):
        stamps = stamps_for() + [collector.Stamp("IUSAC.FR", END, "ema", 9999, False)]
        landed = [("IUSAC.FR", END, 1510), ("IUSAC.FR", END, 8888)]
        rows, coverage = collector.build_rows(stamps, landed, [], {END: 1000})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["t2_ms"], 1502)
        self.assertEqual(rows[0]["t4e_ms"], 1510)
        self.assertEqual(coverage["duplicates"], 2)
        self.assertEqual(coverage["measured"], 1)

    def test_windows_without_a_trigger_are_flushed(self):
        rows, coverage = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {})
        self.assertEqual(rows, [])
        self.assertEqual(coverage["flushed"], 1)
        self.assertEqual(coverage["measured"], 0)

    def test_missing_stage_is_counted_not_dropped(self):
        stamps = [s for s in stamps_for() if s.stage != "ema"]
        rows, coverage = collector.build_rows(
            stamps, [("IUSAC.FR", END, 1510)], [], {END: 1000})
        self.assertEqual(rows, [])
        self.assertEqual(coverage["missing_stage"], 1)
        # A landed EMA with no stamps at all is missing its stages too.
        _, coverage = collector.build_rows([], [("IUSAC.FR", END, 1510)], [], {END: 1000})
        self.assertEqual(coverage["missing_stage"], 1)

    def test_advisory_counts_are_reported(self):
        stamps = stamps_for("A.FR", END, advisory=True) + stamps_for("B.FR", END, advisory=True)
        landed = [("A.FR", END, 1510), ("B.FR", END, 1510)]
        rows, coverage = collector.build_rows(stamps, landed, [("A.FR", END, 1512)], {END: 1000})
        self.assertEqual([r["symbol"] for r in rows], ["A.FR"])
        self.assertEqual(coverage["advisory_stamps"], 2)
        self.assertEqual(coverage["advisory_records"], 1)
        self.assertEqual(coverage["missing_stage"], 1)  # B's advisory never landed

    def test_create_time_topic_is_detected(self):
        # An event-time timestamp (here: 5) is earlier than the EMA stamp, which is impossible.
        with self.assertRaisesRegex(collector.LatencyError, "make reset-flow"):
            collector.build_rows(stamps_for(), [("IUSAC.FR", END, 5)], [], {END: 1000})
        with self.assertRaisesRegex(collector.LatencyError, "LogAppendTime"):
            collector.build_rows(stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
                                 [("IUSAC.FR", END, 5)], {END: 1000})


class SummarizeRowsTest(unittest.TestCase):
    def test_stage_without_samples_reports_zero_count(self):
        rows, _ = collector.build_rows(stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000})
        summary = {item["stage"]: item for item in collector.summarize_rows(rows)}
        self.assertEqual(list(summary), list(collector.STAGES))
        self.assertEqual(summary["ema_compute"]["count"], 1)
        self.assertEqual(summary["ema_compute"]["p50_ms"], 2)
        for stage in ("advisory_publish", "end_to_end_advisory"):
            self.assertEqual(summary[stage]["count"], 0)
            self.assertIsNone(summary[stage]["p50_ms"])


class StageNamesTest(unittest.TestCase):
    def test_stages_in_the_documented_order(self):
        self.assertEqual(collector.STAGES, (
            "ingest_to_fire", "ema_compute", "advisory_compute", "ema_publish",
            "advisory_publish", "end_to_end_ema", "end_to_end_advisory"))


E = 1_636_693_500_000  # a window end: a multiple of WINDOW
BASE = 1_700_000_000_000  # wall-clock base for the stamps


def ts(ms):
    return Timestamp(seconds=ms // 1000, nanos=ms % 1000 * 1_000_000)


def stamp_record(stage, at, advisory=False, symbol="IUSAC.FR"):
    return bindings.StageStamp(symbol=symbol, window_end=ts(E), stage=stage, at=ts(at),
                               advisory_emitted=advisory).SerializeToString()


def fake_stream(topics):
    """stream(brokers, topic) over {topic: [(value_bytes, timestamp_ms)]}."""
    def stream(brokers, topic):
        return iter([(offset, value, stamp) for offset, (value, stamp) in enumerate(topics[topic])])
    return stream


def full_run(advisory=True):
    ema = bindings.EmaWindow(symbol="IUSAC.FR", window_end=ts(E)).SerializeToString()
    advice = bindings.TradeAdvisory(
        ema_window=bindings.EmaWindow(symbol="IUSAC.FR", window_end=ts(E))).SerializeToString()

    def event(trade_time_ms):
        return bindings.PriceEvent(symbol="IUSAC.FR", trade_time=ts(trade_time_ms)).SerializeToString()

    return {
        config.TOPIC_LATENCY_STAMPS: [
            (stamp_record(bindings.STAGE_WINDOW_FIRED, BASE + 1500), 0),
            (stamp_record(bindings.STAGE_EMA_COMPUTED, BASE + 1502), 0),
            (stamp_record(bindings.STAGE_CROSSOVER_EVALUATED, BASE + 1503, advisory), 0)],
        config.TOPIC_EMA_WINDOWS: [(ema, BASE + 1510)],
        config.TOPIC_TRADE_ADVISORIES: [(advice, BASE + 1512)] if advisory else [],
        config.TOPIC_EVENTS: [(event(E - 60_000), BASE + 1000), (event(E + 70_000), BASE + 1400)],
    }


class ReadRunTest(unittest.TestCase):
    def test_stage_stamps_decode_to_millisecond_stamps(self):
        stamps, _, _, _ = collector.read_run("b", fake_stream(full_run()))
        self.assertEqual(stamps, [
            collector.Stamp("IUSAC.FR", E, "fired", BASE + 1500, False),
            collector.Stamp("IUSAC.FR", E, "ema", BASE + 1502, False),
            collector.Stamp("IUSAC.FR", E, "crossover", BASE + 1503, True)])

    def test_landed_times_use_the_record_timestamp_and_embedded_window_key(self):
        _, ema_landed, advisory_landed, _ = collector.read_run("b", fake_stream(full_run()))
        self.assertEqual(ema_landed, [("IUSAC.FR", E, BASE + 1510)])
        self.assertEqual(advisory_landed, [("IUSAC.FR", E, BASE + 1512)])

    def test_the_trigger_is_the_event_that_reaches_the_threshold(self):
        *_, triggers = collector.read_run("b", fake_stream(full_run()))
        self.assertEqual(triggers, {E: BASE + 1400})

    def test_trade_events_are_streamed_into_the_trigger_scan(self):
        seen = []

        def spy(events, window_ms, out_of_orderness_ms):
            seen.append((inspect.isgenerator(events), window_ms, out_of_orderness_ms))
            return {}

        with mock.patch.object(collector, "trigger_times", side_effect=spy):
            collector.read_run("b", fake_stream(full_run()))
        self.assertEqual(seen, [(True, config.WINDOW_MS, config.OUT_OF_ORDERNESS_MS)])

    def test_empty_stamps_name_the_flag(self):
        topics = full_run()
        topics[config.TOPIC_LATENCY_STAMPS] = []
        with self.assertRaisesRegex(collector.LatencyError, "--latency-topic latency-stamps"):
            collector.read_run("b", fake_stream(topics))


class CollectTest(unittest.TestCase):
    def test_outputs_have_the_documented_columns_and_values(self):
        with tempfile.TemporaryDirectory() as directory:
            summary, coverage, table = collector.collect("b", Path(directory), fake_stream(full_run()))
            with open(Path(directory) / "latency.csv", newline="") as handle:
                rows = list(csv.DictReader(handle))
            with open(Path(directory) / "latency-summary.csv", newline="") as handle:
                summary_rows = list(csv.DictReader(handle))
        self.assertEqual(list(rows[0]), [
            "symbol", "window_end_ms", "t0_ms", "t1_ms", "t2_ms", "t3_ms", "t4e_ms",
            "advisory_emitted", "t4a_ms", *collector.STAGES])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "IUSAC.FR")
        self.assertEqual(rows[0]["t0_ms"], str(BASE + 1400))
        self.assertEqual(rows[0]["advisory_emitted"], "true")
        self.assertEqual(rows[0]["ingest_to_fire"], "100")
        self.assertEqual(rows[0]["end_to_end_ema"], "110")
        self.assertEqual(rows[0]["end_to_end_advisory"], "112")
        self.assertEqual(list(summary_rows[0]),
                         ["stage", "count", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
        self.assertEqual([row["stage"] for row in summary_rows], list(collector.STAGES))
        by_stage = {row["stage"]: row for row in summary_rows}
        self.assertEqual(by_stage["ema_compute"]["count"], "1")
        self.assertEqual(float(by_stage["ema_compute"]["p50_ms"]), 2.0)
        self.assertEqual(coverage["measured"], 1)
        self.assertEqual([item["stage"] for item in summary], list(collector.STAGES))
        self.assertIn("ingest_to_fire", table)

    def test_advisory_columns_are_blank_without_an_advisory(self):
        with tempfile.TemporaryDirectory() as directory:
            collector.collect("b", Path(directory), fake_stream(full_run(advisory=False)))
            with open(Path(directory) / "latency.csv", newline="") as handle:
                row = next(csv.DictReader(handle))
            with open(Path(directory) / "latency-summary.csv", newline="") as handle:
                by_stage = {r["stage"]: r for r in csv.DictReader(handle)}
        self.assertEqual(row["advisory_emitted"], "false")
        self.assertEqual((row["t4a_ms"], row["advisory_publish"], row["end_to_end_advisory"]),
                         ("", "", ""))
        self.assertEqual(by_stage["end_to_end_advisory"]["count"], "0")
        self.assertEqual(by_stage["end_to_end_advisory"]["p50_ms"], "")


class FormatTableTest(unittest.TestCase):
    COVERAGE = {"measured": 3, "flushed": 1, "missing_stage": 0, "duplicates": 2,
                "advisories": 1, "advisory_stamps": 1, "advisory_records": 1}

    def test_footer_lists_the_coverage(self):
        table = collector.format_table(collector.summarize_rows([]), self.COVERAGE)
        for text in ("measured 3", "flushed 1", "duplicates 2"):
            self.assertIn(text, table)
        self.assertNotIn("mismatch", table)

    def test_footer_flags_an_advisory_count_mismatch(self):
        table = collector.format_table(
            collector.summarize_rows([]), {**self.COVERAGE, "advisory_records": 0})
        self.assertIn("mismatch", table)


class MainTest(unittest.TestCase):
    def test_main_reports_errors_with_status_1(self):
        errors = io.StringIO()
        with mock.patch.object(collector, "collect", side_effect=collector.LatencyError("boom")):
            with contextlib.redirect_stderr(errors):
                status = collector.main(["--out", "/tmp/never-written"])
        self.assertEqual(status, 1)
        self.assertIn("Error: boom", errors.getvalue())

    def test_main_prints_the_table_and_succeeds(self):
        out = io.StringIO()
        with mock.patch.object(collector, "collect", return_value=([], {}, "THE TABLE")):
            with contextlib.redirect_stdout(out):
                status = collector.main(["--out", "/tmp/never-written", "--brokers", "x:1"])
        self.assertEqual(status, 0)
        self.assertIn("THE TABLE", out.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
