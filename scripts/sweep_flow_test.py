#!/usr/bin/env python3
"""Tests for sweep-flow.py: one measure-flow run per rate, then one comparison table."""

import contextlib
import csv
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parent / "sweep-flow.py"


def load_module():
    spec = importlib.util.spec_from_file_location("sweep_flow", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sweep = load_module()

STAGES = ("ingest_to_fire", "ema_compute")


def summary_for(scale):
    return [{"stage": stage, "count": "10", "mean_ms": str(scale), "p50_ms": str(scale),
             "p95_ms": str(scale * 2), "p99_ms": str(scale * 3), "max_ms": str(scale * 4)}
            for stage in STAGES]


class FakeMeasure:
    """subprocess.run stand-in: writes what measure-flow would, or fails for chosen rates."""

    def __init__(self, failing=()):
        self.commands = []
        self.failing = set(failing)

    def __call__(self, command, **kwargs):
        self.commands.append(command)
        rate = int(command[-1].removeprefix("-rate="))
        if rate in self.failing:
            return type("Result", (), {"returncode": 1})()
        out = Path(command[command.index("--out") + 1])
        out.mkdir(parents=True)
        with open(out / "latency-summary.csv", "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summary_for(1)[0]))
            writer.writeheader()
            writer.writerows(summary_for(rate))
        (out / "pipeline.json").write_text(json.dumps(
            {"producer_events_per_s": float(rate), "end_to_end_events_per_s": rate / 2}))
        return type("Result", (), {"returncode": 0})()


class MeasureCommandTest(unittest.TestCase):
    def test_measure_command_targets_a_rate_directory_and_labels_the_rate(self):
        command = sweep.measure_command(500, Path("/out"), ["-csvPath=a.csv", "-dlqEnabled"])
        self.assertEqual(command[0], sys.executable)
        self.assertEqual(command[1:], [
            "scripts/measure-flow.py", "--out", "/out/rate-500", "--label", "rate=500", "--",
            "-csvPath=a.csv", "-dlqEnabled", "-rate=500"])


class SweepRowsTest(unittest.TestCase):
    def test_sweep_rows_carry_latency_and_throughput_per_rate_and_stage(self):
        rows = sweep.build_sweep_rows([
            (500, summary_for(1), {"producer_events_per_s": 480.0, "end_to_end_events_per_s": 400.0}),
            (1000, summary_for(2), {"producer_events_per_s": 950.0, "end_to_end_events_per_s": 700.0})])
        self.assertEqual(len(rows), 4)
        self.assertEqual(list(rows[0]), list(sweep.SUMMARY_COLUMNS))
        self.assertEqual(rows[0], {
            "rate": 500, "stage": "ingest_to_fire", "count": "10", "p50_ms": "1", "p95_ms": "2",
            "p99_ms": "3", "max_ms": "4", "producer_events_per_s": 480.0,
            "end_to_end_events_per_s": 400.0})
        self.assertEqual([(r["rate"], r["stage"]) for r in rows],
                         [(500, "ingest_to_fire"), (500, "ema_compute"),
                          (1000, "ingest_to_fire"), (1000, "ema_compute")])


class MainTest(unittest.TestCase):
    def run_sweep(self, directory, rates, failing=()):
        runner = FakeMeasure(failing)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = sweep.main(["--out", directory, "--rates", *map(str, rates), "--",
                                 "-csvPath=a.csv"], runner=runner)
        return status, runner

    def read_summary(self, directory):
        with open(Path(directory) / "sweep-summary.csv", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_every_rate_gets_a_run_and_a_row_per_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            status, runner = self.run_sweep(directory, [500, 1000])
            rows = self.read_summary(directory)
        self.assertEqual(status, 0)
        self.assertEqual([c[-1] for c in runner.commands], ["-rate=500", "-rate=1000"])
        self.assertEqual(list(rows[0]), list(sweep.SUMMARY_COLUMNS))
        self.assertEqual([(r["rate"], r["stage"]) for r in rows],
                         [("500", "ingest_to_fire"), ("500", "ema_compute"),
                          ("1000", "ingest_to_fire"), ("1000", "ema_compute")])
        self.assertEqual(rows[2]["p95_ms"], "2000")  # the fake writes p95 = 2 * rate
        self.assertEqual(rows[2]["producer_events_per_s"], "1000.0")

    def test_a_failing_run_stops_the_sweep_and_keeps_earlier_results(self):
        with tempfile.TemporaryDirectory() as directory:
            status, runner = self.run_sweep(directory, [500, 1000, 2000], failing=[1000])
            rows = self.read_summary(directory)
        self.assertEqual(status, 1)
        self.assertEqual(len(runner.commands), 2)  # 2000 never ran
        self.assertEqual({r["rate"] for r in rows}, {"500"})


if __name__ == "__main__":
    unittest.main()
