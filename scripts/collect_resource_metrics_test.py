#!/usr/bin/env python3
"""Tests for collect-resource-metrics.py with Prometheus replaced by a stub."""

import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parent / "collect-resource-metrics.py"


def load_module():
    spec = importlib.util.spec_from_file_location("collect_resource_metrics", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collector = load_module()


class FakePrometheus:
    """Answers each query by looking for a marker in the PromQL text."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def __call__(self, endpoint, params):
        self.calls.append((endpoint, params))
        for marker, (label, name, payload) in self.answers.items():
            if marker in params["query"]:
                if endpoint == "query_range":
                    return [{"metric": {label: name}, "values": payload}]
                return [{"metric": {label: name}, "value": [params["time"], str(payload)]}]
        return []


class ParseTimeTest(unittest.TestCase):
    def test_accepts_epoch_seconds(self):
        self.assertEqual(collector.parse_time("1700000000.5"), 1700000000.5)

    def test_treats_naive_iso_as_utc(self):
        self.assertEqual(collector.parse_time("2023-11-14T22:13:20"), 1700000000.0)

    def test_honours_an_offset(self):
        self.assertEqual(collector.parse_time("2023-11-15T00:13:20+02:00"), 1700000000.0)


class StepTest(unittest.TestCase):
    def test_keeps_the_requested_step_for_short_windows(self):
        self.assertEqual(collector.effective_step(0, 600, 2), 2)

    def test_widens_the_step_so_a_long_window_stays_within_the_point_limit(self):
        step = collector.effective_step(0, 86400, 2)
        self.assertLessEqual(86400 / step, collector.MAX_POINTS)


class CollectTest(unittest.TestCase):
    def setUp(self):
        self.fetch = FakePrometheus({
            "docker_container_cpu_usage_seconds_total[10s]": ("service", "taskmanager", [[100, "1.5"], [102, "2.5"], [104, "NaN"]]),
            "docker_container_cpu_usage_seconds_total[60s]": ("service", "taskmanager", 87.5),
            "docker_container_memory_working_set_bytes": ("service", "taskmanager", [[100, "1000"], [102, "3000"]]),
            "namedprocess_namegroup_cpu_seconds_total[10s]": ("groupname", "data-producer", [[100, "0.5"]]),
        })

    def test_groups_series_by_component_kind_and_metric(self):
        series, _ = collector.collect(self.fetch, 100, 160, 2, "10s")
        self.assertEqual(series[("taskmanager", "docker", "cpu_cores")], [(100.0, 1.5), (102.0, 2.5)])
        self.assertEqual(series[("taskmanager", "docker", "memory_bytes")], [(100.0, 1000.0), (102.0, 3000.0)])
        self.assertEqual(series[("data-producer", "host", "cpu_cores")], [(100.0, 0.5)])

    def test_drops_non_finite_samples(self):
        series, _ = collector.collect(self.fetch, 100, 160, 2, "10s")
        self.assertNotIn(float("nan"), [v for _, v in series[("taskmanager", "docker", "cpu_cores")]])

    def test_leaves_out_excluded_components(self):
        fetch = FakePrometheus({"docker_container_memory_working_set_bytes": ("service", "grafana", [[100, "5"]]),
                                "docker_container_cpu_usage_seconds_total[60s]": ("service", "grafana", 1.0)})
        series, totals = collector.collect(fetch, 100, 160, 2, "10s", exclude=("grafana",))
        self.assertEqual((series, totals), ({}, {}))

    def test_totals_use_the_window_length_evaluated_at_the_end(self):
        _, totals = collector.collect(self.fetch, 100, 160, 2, "10s")
        self.assertEqual(totals[("taskmanager", "docker", "cpu_cores")], 87.5)
        instant = [params for endpoint, params in self.fetch.calls if endpoint == "query"]
        self.assertTrue(all(params["time"] == 160 for params in instant))


class SummarizeTest(unittest.TestCase):
    def test_reports_average_max_and_total(self):
        rows = collector.summarize(
            {("redpanda-0", "docker", "cpu_cores"): [(0, 1.0), (2, 3.0)]},
            {("redpanda-0", "docker", "cpu_cores"): 8.0},
        )
        self.assertEqual(rows, [{"component": "redpanda-0", "kind": "docker", "metric": "cpu_cores", "avg": 2.0, "max": 3.0, "total": 8.0}])

    def test_skips_series_without_samples_and_leaves_missing_totals_blank(self):
        rows = collector.summarize(
            {("a", "docker", "memory_bytes"): [], ("b", "docker", "memory_bytes"): [(0, 5.0)]}, {})
        self.assertEqual([r["component"] for r in rows], ["b"])
        self.assertEqual(rows[0]["total"], "")


class WriteOutputsTest(unittest.TestCase):
    def test_writes_timeseries_summary_and_run_metadata(self):
        series = {("taskmanager", "docker", "cpu_cores"): [(100.0, 1.5), (102.0, 2.5)]}
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "run"
            collector.write_outputs(out, series, {("taskmanager", "docker", "cpu_cores"): 4.0}, 100.0, 160.0,
                                    {"step_s": 2, "labels": {"rate": "1000"}})
            with (out / "timeseries.csv").open() as handle:
                points = list(csv.DictReader(handle))
            with (out / "summary.csv").open() as handle:
                summary = list(csv.DictReader(handle))
            run = json.loads((out / "run.json").read_text())
        self.assertEqual([p["offset_s"] for p in points], ["0.0", "2.0"])
        self.assertEqual(points[1]["value"], "2.5")
        self.assertEqual((summary[0]["avg"], summary[0]["max"], summary[0]["total"]), ("2", "2.5", "4"))
        self.assertEqual(run["duration_s"], 60.0)
        self.assertEqual(run["labels"], {"rate": "1000"})


class MainTest(unittest.TestCase):
    def test_rejects_an_end_before_the_start(self):
        with self.assertRaises(SystemExit):
            collector.main(["--start", "200", "--end", "100", "--out", "unused"])


if __name__ == "__main__":
    unittest.main()
