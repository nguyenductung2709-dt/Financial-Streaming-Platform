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
ALLOWANCE = 70_000  # an explicit allowance for the scan unit tests, not the configured one
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


class CrossingTimesTest(unittest.TestCase):
    def test_zero_allowance_records_the_event_that_reaches_the_window_end(self):
        events = [(B + 10, 100), (B + 299_999, 200), (B + 300_000, 300), (B + 370_000, 400)]
        self.assertEqual(collector.crossing_times(events, WINDOW, (0,)), [{B + WINDOW: 300}])

    def test_one_pass_gives_every_allowance(self):
        events = iter([(B + 10, 100), (B + WINDOW + 5, 200), (B + WINDOW + ALLOWANCE, 300),
                       (B + 2 * WINDOW + ALLOWANCE, 400)])
        reached, triggers = collector.crossing_times(events, WINDOW, (0, ALLOWANCE))
        self.assertEqual(reached, {B + WINDOW: 200, B + 2 * WINDOW: 400})
        # B is the end of an empty window before the first event; build_rows never joins it.
        self.assertEqual(triggers, {B: 200, B + WINDOW: 300, B + 2 * WINDOW: 400})
        # The stream reaches a window's end no later than the event that fires it.
        for end, at in reached.items():
            self.assertLessEqual(at, triggers[end])

    def test_late_events_cross_nothing(self):
        events = [(B + 10, 100), (B + 299_999, 200), (B + 100, 300), (B + 300_001, 400)]
        self.assertEqual(collector.crossing_times(events, WINDOW, (0,)), [{B + WINDOW: 400}])


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

    def test_materialize_stamps_add_the_database_stages(self):
        rows, coverage = collector.build_rows(
            stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
            [("IUSAC.FR", END, 1512)], {END: 1000},
            materialized=([("IUSAC.FR", END, 1520)], [("IUSAC.FR", END, 1522)]))
        row = rows[0]
        self.assertEqual(row["t5e_ms"], 1520)
        self.assertEqual(row["t5a_ms"], 1522)
        self.assertEqual(row["ema_materialize"], 10)      # t5e - t4e
        self.assertEqual(row["advisory_materialize"], 10)  # t5a - t4a
        self.assertEqual(row["end_to_end_ema_materialized"], 520)    # t5e - t0
        self.assertEqual(row["end_to_end_advisory_materialized"], 522)
        self.assertTrue(coverage["materialized"])
        self.assertEqual(coverage["missing_materialize"], 0)

    def test_a_measured_window_without_a_materialize_stamp_keeps_its_stages(self):
        rows, coverage = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000},
            materialized=([], []))
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertIsNone(row["t5e_ms"])
        self.assertIsNone(row["ema_materialize"])
        self.assertIsNone(row["end_to_end_ema_materialized"])
        self.assertTrue(coverage["materialized"])
        self.assertEqual(coverage["missing_materialize"], 1)
        self.assertEqual(coverage["measured"], 1)

    def test_an_advisory_without_its_materialize_stamp_counts_once(self):
        rows, coverage = collector.build_rows(
            stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
            [("IUSAC.FR", END, 1512)], {END: 1000},
            materialized=([("IUSAC.FR", END, 1520)], []))
        self.assertEqual(coverage["missing_materialize"], 1)
        self.assertEqual(rows[0]["t5e_ms"], 1520)
        self.assertIsNone(rows[0]["t5a_ms"])

    def test_a_result_stamped_before_it_landed_is_refused(self):
        with self.assertRaisesRegex(collector.LatencyError, "materialized"):
            collector.build_rows(
                stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000},
                materialized=([("IUSAC.FR", END, 1500)], []))
        with self.assertRaisesRegex(collector.LatencyError, "materialized"):
            collector.build_rows(
                stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
                [("IUSAC.FR", END, 1512)], {END: 1000},
                materialized=([("IUSAC.FR", END, 1520)], [("IUSAC.FR", END, 1511)]))

    def test_window_end_stages_count_from_the_moment_the_stream_reached_the_end(self):
        rows, _ = collector.build_rows(
            stamps_for(advisory=True), [("IUSAC.FR", END, 1510)],
            [("IUSAC.FR", END, 1512)], {END: 1000},
            materialized=([("IUSAC.FR", END, 1520)], [("IUSAC.FR", END, 1522)]),
            window_ends={END: 800})
        row = rows[0]
        self.assertEqual(row["tw_ms"], 800)
        self.assertEqual(row["watermark_wait"], 200)                     # t0 - tw
        self.assertEqual(row["window_end_to_ema"], 702)                  # t2 - tw
        self.assertEqual(row["window_end_to_ema_published"], 710)        # t4e - tw
        self.assertEqual(row["window_end_to_advisory_published"], 712)   # t4a - tw
        self.assertEqual(row["window_end_to_ema_materialized"], 720)     # t5e - tw
        self.assertEqual(row["window_end_to_advisory_materialized"], 722)
        # Each window_end_to_* stage is the watermark wait plus its end_to_end_* stage.
        self.assertEqual(row["window_end_to_ema_published"],
                         row["watermark_wait"] + row["end_to_end_ema"])

    def test_window_end_stages_follow_the_missing_moments(self):
        rows, _ = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000}, window_ends={END: 800})
        row = rows[0]
        self.assertEqual(row["window_end_to_ema_published"], 710)
        self.assertIsNone(row["window_end_to_advisory_published"])
        self.assertIsNone(row["window_end_to_ema_materialized"])

    def test_without_window_ends_the_window_end_stages_stay_empty(self):
        rows, _ = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000})
        row = rows[0]
        self.assertIsNone(row["tw_ms"])
        for stage in ("watermark_wait", "window_end_to_ema", "window_end_to_ema_published"):
            self.assertIsNone(row[stage])
        self.assertEqual(row["end_to_end_ema"], 510)

    def test_without_materialize_stamps_the_database_stages_stay_empty(self):
        rows, coverage = collector.build_rows(
            stamps_for(), [("IUSAC.FR", END, 1510)], [], {END: 1000})
        row = rows[0]
        self.assertIsNone(row["t5e_ms"])
        self.assertIsNone(row["ema_materialize"])
        self.assertFalse(coverage["materialized"])
        self.assertEqual(coverage["missing_materialize"], 0)


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
            "advisory_publish", "end_to_end_ema", "end_to_end_advisory",
            "ema_materialize", "advisory_materialize",
            "end_to_end_ema_materialized", "end_to_end_advisory_materialized",
            "watermark_wait", "window_end_to_ema", "window_end_to_ema_published",
            "window_end_to_advisory_published", "window_end_to_ema_materialized",
            "window_end_to_advisory_materialized"))


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


def fake_partitions(topics):
    """topic_partitions(brokers, topic) that knows only the fixture's topics."""
    def partitions(brokers, topic):
        return {0} if topic in topics else set()
    return partitions


def full_run(advisory=True, materialized=False):
    ema = bindings.EmaWindow(symbol="IUSAC.FR", window_end=ts(E)).SerializeToString()
    advice = bindings.TradeAdvisory(
        ema_window=bindings.EmaWindow(symbol="IUSAC.FR", window_end=ts(E))).SerializeToString()

    def event(trade_time_ms):
        return bindings.PriceEvent(symbol="IUSAC.FR", trade_time=ts(trade_time_ms)).SerializeToString()

    topics = {
        config.TOPIC_LATENCY_STAMPS: [
            (stamp_record(bindings.STAGE_WINDOW_FIRED, BASE + 1500), 0),
            (stamp_record(bindings.STAGE_EMA_COMPUTED, BASE + 1502), 0),
            (stamp_record(bindings.STAGE_CROSSOVER_EVALUATED, BASE + 1503, advisory), 0)],
        config.TOPIC_EMA_WINDOWS: [(ema, BASE + 1510)],
        config.TOPIC_TRADE_ADVISORIES: [(advice, BASE + 1512)] if advisory else [],
        # reaches the window end (tw), then the end plus the configured allowance (t0)
        config.TOPIC_EVENTS: [(event(E - 60_000), BASE + 1000),
                              (event(E + config.OUT_OF_ORDERNESS_MS // 2), BASE + 1200),
                              (event(E + config.OUT_OF_ORDERNESS_MS), BASE + 1400)],
    }
    if materialized:
        stamps = [(stamp_record(bindings.STAGE_EMA_MATERIALIZED, BASE + 1520), 0)]
        if advisory:
            stamps.append((stamp_record(bindings.STAGE_ADVISORY_MATERIALIZED, BASE + 1522), 0))
        topics[config.TOPIC_MATERIALIZE_STAMPS] = stamps
    return topics


class ReadRunTest(unittest.TestCase):
    def read(self, topics, **kwargs):
        return collector.read_run("b", fake_stream(topics), fake_partitions(topics), **kwargs)

    def test_stage_stamps_decode_to_millisecond_stamps(self):
        stamps, *_ = self.read(full_run())
        self.assertEqual(stamps, [
            collector.Stamp("IUSAC.FR", E, "fired", BASE + 1500, False),
            collector.Stamp("IUSAC.FR", E, "ema", BASE + 1502, False),
            collector.Stamp("IUSAC.FR", E, "crossover", BASE + 1503, True)])

    def test_landed_times_use_the_record_timestamp_and_embedded_window_key(self):
        _, ema_landed, advisory_landed, *_ = self.read(full_run())
        self.assertEqual(ema_landed, [("IUSAC.FR", E, BASE + 1510)])
        self.assertEqual(advisory_landed, [("IUSAC.FR", E, BASE + 1512)])

    def test_the_trigger_is_the_event_that_reaches_the_threshold(self):
        *_, triggers, window_ends = self.read(full_run())
        self.assertEqual(triggers, {E: BASE + 1400})
        self.assertEqual(window_ends, {E: BASE + 1200})

    def test_trade_events_are_streamed_into_one_scan(self):
        seen = []

        def spy(events, window_ms, allowances, partitions):
            seen.append((inspect.isgenerator(events), window_ms, allowances, partitions))
            return [{}, {}]

        with mock.patch.object(collector, "crossing_times", side_effect=spy):
            self.read(full_run())
        self.assertEqual(seen, [(True, config.WINDOW_MS, (0, config.OUT_OF_ORDERNESS_MS), 1)])

    def test_empty_stamps_name_the_flag(self):
        topics = full_run()
        topics[config.TOPIC_LATENCY_STAMPS] = []
        with self.assertRaisesRegex(collector.LatencyError, "--latency-topic latency-stamps"):
            self.read(topics)

    def test_materialize_stamps_decode_to_triples(self):
        _, _, _, materialized, *_ = self.read(full_run(materialized=True))
        self.assertEqual(materialized, ([("IUSAC.FR", E, BASE + 1520)],
                                         [("IUSAC.FR", E, BASE + 1522)]))

    def test_without_the_materialize_stamps_topic_there_are_no_database_stages(self):
        _, _, _, materialized, *_ = self.read(full_run())
        self.assertIsNone(materialized)

    def test_an_existing_but_empty_stamps_topic_means_no_database_stages(self):
        topics = full_run()
        topics[config.TOPIC_MATERIALIZE_STAMPS] = []
        _, _, _, materialized, *_ = self.read(topics)
        self.assertIsNone(materialized)

    def test_an_unrecognized_materialize_stage_is_refused(self):
        topics = full_run()
        topics[config.TOPIC_MATERIALIZE_STAMPS] = [
            (stamp_record(bindings.STAGE_WINDOW_FIRED, BASE + 1520), 0)]
        with self.assertRaisesRegex(collector.LatencyError, "unrecognized stage"):
            self.read(topics)


class CollectTest(unittest.TestCase):
    def test_outputs_have_the_documented_columns_and_values(self):
        with tempfile.TemporaryDirectory() as directory:
            topics = full_run(materialized=True)
            summary, coverage, table = collector.collect(
                "b", Path(directory), fake_stream(topics), fake_partitions(topics))
            with open(Path(directory) / "latency.csv", newline="") as handle:
                rows = list(csv.DictReader(handle))
            with open(Path(directory) / "latency-summary.csv", newline="") as handle:
                summary_rows = list(csv.DictReader(handle))
        self.assertEqual(list(rows[0]), [
            "symbol", "window_end_ms", "tw_ms", "t0_ms", "t1_ms", "t2_ms", "t3_ms", "t4e_ms",
            "advisory_emitted", "t4a_ms", "t5e_ms", "t5a_ms", *collector.STAGES])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "IUSAC.FR")
        self.assertEqual(rows[0]["t0_ms"], str(BASE + 1400))
        self.assertEqual(rows[0]["advisory_emitted"], "true")
        self.assertEqual(rows[0]["ingest_to_fire"], "100")
        self.assertEqual(rows[0]["end_to_end_ema"], "110")
        self.assertEqual(rows[0]["end_to_end_advisory"], "112")
        self.assertEqual(rows[0]["t5e_ms"], str(BASE + 1520))
        self.assertEqual(rows[0]["ema_materialize"], "10")
        self.assertEqual(rows[0]["advisory_materialize"], "10")
        self.assertEqual(rows[0]["end_to_end_ema_materialized"], "120")
        self.assertEqual(rows[0]["end_to_end_advisory_materialized"], "122")
        self.assertEqual(rows[0]["tw_ms"], str(BASE + 1200))
        self.assertEqual(rows[0]["watermark_wait"], "200")
        self.assertEqual(rows[0]["window_end_to_ema"], "302")
        self.assertEqual(rows[0]["window_end_to_ema_published"], "310")
        self.assertEqual(rows[0]["window_end_to_advisory_published"], "312")
        self.assertEqual(rows[0]["window_end_to_ema_materialized"], "320")
        self.assertEqual(rows[0]["window_end_to_advisory_materialized"], "322")
        self.assertEqual(list(summary_rows[0]),
                         ["stage", "count", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
        self.assertEqual([row["stage"] for row in summary_rows], list(collector.STAGES))
        by_stage = {row["stage"]: row for row in summary_rows}
        self.assertEqual(by_stage["ema_compute"]["count"], "1")
        self.assertEqual(float(by_stage["ema_compute"]["p50_ms"]), 2.0)
        self.assertEqual(by_stage["end_to_end_advisory_materialized"]["count"], "1")
        self.assertTrue(coverage["materialized"])
        self.assertEqual([item["stage"] for item in summary], list(collector.STAGES))
        self.assertIn("ingest_to_fire", table)

    def test_advisory_columns_are_blank_without_an_advisory(self):
        with tempfile.TemporaryDirectory() as directory:
            topics = full_run(advisory=False)
            collector.collect("b", Path(directory), fake_stream(topics), fake_partitions(topics))
            with open(Path(directory) / "latency.csv", newline="") as handle:
                row = next(csv.DictReader(handle))
            with open(Path(directory) / "latency-summary.csv", newline="") as handle:
                by_stage = {r["stage"]: r for r in csv.DictReader(handle)}
        self.assertEqual(row["advisory_emitted"], "false")
        self.assertEqual((row["t4a_ms"], row["advisory_publish"], row["end_to_end_advisory"]),
                         ("", "", ""))
        self.assertEqual(by_stage["end_to_end_advisory"]["count"], "0")
        self.assertEqual(by_stage["end_to_end_advisory"]["p50_ms"], "")

    def test_database_columns_are_blank_without_materialize_stamps(self):
        with tempfile.TemporaryDirectory() as directory:
            topics = full_run()  # no materialize-stamps topic
            summary, coverage, table = collector.collect(
                "b", Path(directory), fake_stream(topics), fake_partitions(topics))
            with open(Path(directory) / "latency.csv", newline="") as handle:
                row = next(csv.DictReader(handle))
            with open(Path(directory) / "latency-summary.csv", newline="") as handle:
                by_stage = {r["stage"]: r for r in csv.DictReader(handle)}
        self.assertEqual((row["t5e_ms"], row["t5a_ms"], row["ema_materialize"],
                          row["end_to_end_ema_materialized"]), ("", "", "", ""))
        self.assertEqual(by_stage["ema_materialize"]["count"], "0")
        self.assertFalse(coverage["materialized"])
        self.assertIn("no materialize stamps", table)


class FormatTableTest(unittest.TestCase):
    COVERAGE = {"measured": 3, "flushed": 1, "missing_stage": 0, "duplicates": 2,
                "materialized": True, "missing_materialize": 0,
                "advisories": 1, "advisory_stamps": 1, "advisory_records": 1}

    def test_footer_lists_the_coverage(self):
        table = collector.format_table(collector.summarize_rows([]), self.COVERAGE)
        for text in ("measured 3", "flushed 1", "duplicates 2",
                     "missing a materialize stamp 0"):
            self.assertIn(text, table)
        self.assertNotIn("mismatch", table)
        self.assertNotIn("no materialize stamps", table)

    def test_footer_flags_an_advisory_count_mismatch(self):
        table = collector.format_table(
            collector.summarize_rows([]), {**self.COVERAGE, "advisory_records": 0})
        self.assertIn("mismatch", table)

    def test_without_materialize_stamps_the_footer_says_so(self):
        coverage = {**self.COVERAGE, "materialized": False}
        table = collector.format_table(collector.summarize_rows([]), coverage)
        self.assertIn("no materialize stamps", table)
        self.assertNotIn("missing a materialize stamp", table)


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


class PartitionedCrossingTest(unittest.TestCase):
    """Several partitions: a window fires once the minimum over the partitions
    of their highest trade time passes window end + allowance."""

    def test_the_slowest_partition_decides(self):
        events = [(B + 10, 1, 0), (B + 20, 2, 1),
                  (END + ALLOWANCE, 3, 0),        # p0 passes, p1 does not: no fire
                  (END + ALLOWANCE + 5, 4, 1)]    # p1 passes too: fires here
        triggers, = collector.crossing_times(iter(events), WINDOW, (ALLOWANCE,), partitions=2)
        # the window before B also first passes B + ALLOWANCE in the slowest partition here
        self.assertEqual(triggers, {B: 4, END: 4})

    def test_nothing_fires_before_every_partition_had_an_event(self):
        events = [(B + 10, 1, 0), (END + 10 * WINDOW, 2, 0)]
        triggers, = collector.crossing_times(iter(events), WINDOW, (ALLOWANCE,), partitions=2)
        self.assertEqual(triggers, {})

    def test_one_partition_is_the_highest_trade_time(self):
        pairs = [(B + 10, 1), (END + ALLOWANCE, 2)]
        triples = [(t, p, 0) for t, p in pairs]
        self.assertEqual(collector.crossing_times(iter(pairs), WINDOW, (ALLOWANCE,)),
                         collector.crossing_times(iter(triples), WINDOW, (ALLOWANCE,), partitions=1))


class PartitionedReadRunTest(unittest.TestCase):
    """read_run over a 2-partition trade-events topic: the partitions are merged
    back into publish (source row) order before the trigger scan."""

    def test_partitions_are_merged_by_source_row(self):
        topics = full_run()
        E = int(bindings.EmaWindow.FromString(topics[config.TOPIC_EMA_WINDOWS][0][0])
                .window_end.seconds) * 1000

        def event(trade_time_ms, row, symbol):
            return bindings.PriceEvent(symbol=symbol, trade_time=ts(trade_time_ms),
                                       source_row=row).SerializeToString()

        allowance = config.OUT_OF_ORDERNESS_MS
        by_partition = {
            0: [(event(E - 60_000, 1, "A.FR"), BASE + 1000),
                (event(E + allowance, 4, "A.FR"), BASE + 1300)],     # p0 passes first
            1: [(event(E - 50_000, 2, "B.FR"), BASE + 1100),
                (event(E + allowance // 2, 3, "B.FR"), BASE + 1200),  # p1 reaches the end
                (event(E + allowance + 1, 5, "B.FR"), BASE + 1400)],  # p1 passes: fires
        }
        seen = []

        def stream_partition(brokers, topic, partition):
            seen.append(partition)
            return iter([(o, v, t) for o, (v, t) in enumerate(by_partition[partition])])

        def partitions(brokers, topic):
            return {0, 1} if topic == config.TOPIC_EVENTS else ({0} if topic in topics else set())

        *_, triggers, window_ends = collector.read_run(
            "b", fake_stream(topics), partitions, stream_partition=stream_partition)
        self.assertEqual(sorted(seen), [0, 1])
        self.assertEqual(triggers[E], BASE + 1400)   # the slowest partition decides
        self.assertEqual(window_ends[E], BASE + 1300)  # min passes E once p0 and p1 have


if __name__ == "__main__":
    unittest.main(verbosity=2)
