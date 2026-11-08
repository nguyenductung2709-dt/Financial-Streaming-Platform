#!/usr/bin/env python3
"""Tests for measure-flow.py: when a run counts as finished and how results are formatted."""

import importlib.util
from pathlib import Path
import signal
import sys
import unittest

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))


def load_module():
    spec = importlib.util.spec_from_file_location("measure_flow", SCRIPTS / "measure-flow.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


measure = load_module()
EMA, ADV = "ema-windows", "trade-advisories"


class PipelineSampleTest(unittest.TestCase):
    def test_caught_up_when_committed_reaches_every_end_offset(self):
        _, caught_up = measure.pipeline_sample(10, {EMA: 5, ADV: 2}, {EMA: 5, ADV: 2})
        self.assertTrue(caught_up)

    def test_behind_on_one_topic_is_not_caught_up(self):
        _, caught_up = measure.pipeline_sample(10, {EMA: 5, ADV: 2}, {EMA: 5, ADV: 1})
        self.assertFalse(caught_up)

    def test_a_topic_with_data_and_no_commit_is_not_caught_up(self):
        _, caught_up = measure.pipeline_sample(10, {EMA: 5, ADV: 0}, {})
        self.assertFalse(caught_up)

    def test_empty_topics_need_no_commit(self):
        _, caught_up = measure.pipeline_sample(0, {}, {})
        self.assertTrue(caught_up)

    def test_signature_changes_with_any_progress(self):
        base, _ = measure.pipeline_sample(10, {EMA: 5, ADV: 2}, {EMA: 5, ADV: 2})
        for changed in (measure.pipeline_sample(11, {EMA: 5, ADV: 2}, {EMA: 5, ADV: 2}),
                        measure.pipeline_sample(10, {EMA: 6, ADV: 2}, {EMA: 5, ADV: 2}),
                        measure.pipeline_sample(10, {EMA: 5, ADV: 2}, {EMA: 4, ADV: 2})):
            self.assertNotEqual(base, changed[0])


class SettleTest(unittest.TestCase):
    def feed(self, settle, samples):
        for now, signature, caught_up in samples:
            settle.update(now, signature, caught_up)

    def test_finishes_after_the_idle_period_and_ends_at_the_last_change(self):
        settle = measure.Settle(started=0)
        self.feed(settle, [(1, "a", False), (2, "b", False), (3, "c", True), (30, "c", True)])
        self.assertEqual(settle.last_change, 3)
        self.assertFalse(settle.done(22, True, 20))
        self.assertTrue(settle.done(23, True, 20))

    def test_waits_for_the_producer(self):
        settle = measure.Settle(started=0)
        self.feed(settle, [(1, "a", True), (2, "b", True), (100, "b", True)])
        self.assertFalse(settle.done(100, False, 20))

    def test_waits_for_the_materializer_to_catch_up(self):
        settle = measure.Settle(started=0)
        self.feed(settle, [(1, "a", False), (2, "b", False), (100, "b", False)])
        self.assertFalse(settle.done(100, True, 20))

    def test_never_finishes_if_nothing_ever_moved(self):
        settle = measure.Settle(started=0)
        self.feed(settle, [(1, "a", True), (100, "a", True)])
        self.assertFalse(settle.done(100, True, 20))

    def test_renewed_activity_restarts_the_idle_period(self):
        settle = measure.Settle(started=0)
        self.feed(settle, [(1, "a", True), (2, "b", True), (15, "c", True)])
        self.assertFalse(settle.done(30, True, 20))
        self.assertTrue(settle.done(35, True, 20))


class ThroughputTest(unittest.TestCase):
    def test_rates_over_producer_and_total_time(self):
        self.assertEqual(measure.throughput(1000, 10, 20), {"producer_events_per_s": 100.0, "end_to_end_events_per_s": 50.0})

    def test_zero_durations_give_no_rate(self):
        self.assertEqual(measure.throughput(1000, 0, 0), {"producer_events_per_s": None, "end_to_end_events_per_s": None})


class PrometheusTest(unittest.TestCase):
    def test_sample_combines_flink_and_redpanda_state(self):
        class Stub(measure.Prometheus):
            def query(self, expression):
                if "numRecordsIn" in expression:
                    return [{"metric": {}, "value": [0, "42"]}]
                topic = lambda name, value: {"metric": {"redpanda_topic": name}, "value": [0, str(value)]}
                if "max_offset" in expression:
                    return [topic(EMA, 7), topic(ADV, 3)]
                return [topic(EMA, 7), topic(ADV, 2)]

        (signature, caught_up), ends = Stub("http://x").sample("materializer")
        self.assertEqual(signature, (42.0, (7.0, 3.0), (7.0, 2.0)))
        self.assertFalse(caught_up)
        self.assertEqual(ends, {EMA: 7.0, ADV: 3.0})


class FormatTableTest(unittest.TestCase):
    def test_one_line_per_component_with_mib_and_host_marker(self):
        mib = 1024 * 1024
        rows = [
            {"component": "taskmanager", "kind": "docker", "metric": "cpu_cores", "avg": "0.5", "max": "1.5", "total": "30"},
            {"component": "taskmanager", "kind": "docker", "metric": "memory_bytes", "avg": str(100 * mib), "max": str(200 * mib), "total": ""},
            {"component": "taskmanager", "kind": "docker", "metric": "network_receive_bytes_per_s", "avg": "1", "max": "1", "total": str(5 * mib)},
            {"component": "data-producer", "kind": "host", "metric": "cpu_cores", "avg": "0.25", "max": "0.3", "total": "10"},
        ]
        lines = measure.format_table(rows).splitlines()
        task = next(line for line in lines if line.startswith("taskmanager"))
        self.assertEqual(task.split(), ["taskmanager", "0.500", "1.500", "100", "200", "5.0", "-", "-", "-"])
        host = next(line for line in lines if line.startswith("data-producer"))
        self.assertEqual(host.split()[:3], ["data-producer", "*", "0.250"])


class RaiseOnSigtermTest(unittest.TestCase):
    def test_sigterm_interrupts_the_run_so_its_children_are_stopped(self):
        self.addCleanup(signal.signal, signal.SIGTERM, signal.signal(signal.SIGTERM, signal.SIG_DFL))
        measure.raise_on_sigterm()
        with self.assertRaises(KeyboardInterrupt):
            signal.raise_signal(signal.SIGTERM)


if __name__ == "__main__":
    unittest.main()
