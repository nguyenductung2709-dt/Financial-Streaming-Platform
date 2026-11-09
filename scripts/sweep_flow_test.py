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
    """subprocess.run stand-in: writes what measure-flow would, or fails for chosen loads."""

    def __init__(self, failing=()):
        self.commands = []
        self.failing = {str(value) for value in failing}

    def __call__(self, command, **kwargs):
        self.commands.append(command)
        flag, value = command[-1].lstrip("-").split("=")
        rate = int(value) if value.isdigit() else float(value)
        if value in self.failing:
            return type("Result", (), {"returncode": 1})()
        out = Path(command[command.index("--out") + 1])
        out.mkdir(parents=True)
        with open(out / "latency-summary.csv", "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summary_for(1)[0]))
            writer.writeheader()
            writer.writerows(summary_for(rate))
        pipeline = {"producer_events_per_s": float(rate), "end_to_end_events_per_s": rate / 2}
        if flag == "speed":
            pipeline["producer"] = {"published": 10, "replay_lag_max_ms": 7}
        (out / "pipeline.json").write_text(json.dumps(pipeline))
        return type("Result", (), {"returncode": 0})()


class MeasureCommandTest(unittest.TestCase):
    def test_measure_command_targets_a_rate_directory_and_labels_the_rate(self):
        command = sweep.measure_command("rate", 500, Path("/out"), ["-csvPath=a.csv", "-dlqEnabled"])
        self.assertEqual(command[0], sys.executable)
        self.assertEqual(command[1:], [
            "scripts/measure-flow.py", "--out", "/out/rate-500", "--label", "rate=500", "--",
            "-csvPath=a.csv", "-dlqEnabled", "-rate=500"])

    def test_measure_command_for_a_speed(self):
        command = sweep.measure_command("speed", "0.5", Path("/out"), ["-csvPath=a.csv"])
        self.assertEqual(command[1:], [
            "scripts/measure-flow.py", "--out", "/out/speed-0.5", "--label", "speed=0.5", "--",
            "-csvPath=a.csv", "-speed=0.5"])


class SweepRowsTest(unittest.TestCase):
    def test_sweep_rows_carry_latency_and_throughput_per_rate_and_stage(self):
        rows = sweep.build_sweep_rows("rate", [
            (500, summary_for(1), {"producer_events_per_s": 480.0, "end_to_end_events_per_s": 400.0}),
            (1000, summary_for(2), {"producer_events_per_s": 950.0, "end_to_end_events_per_s": 700.0})])
        self.assertEqual(len(rows), 4)
        self.assertEqual(list(rows[0]), list(sweep.summary_columns("rate")))
        self.assertEqual(rows[0], {
            "rate": 500, "stage": "ingest_to_fire", "count": "10", "p50_ms": "1", "p95_ms": "2",
            "p99_ms": "3", "max_ms": "4", "producer_events_per_s": 480.0,
            "end_to_end_events_per_s": 400.0})
        self.assertEqual([(r["rate"], r["stage"]) for r in rows],
                         [(500, "ingest_to_fire"), (500, "ema_compute"),
                          (1000, "ingest_to_fire"), (1000, "ema_compute")])

    def test_speed_rows_carry_the_replay_lag(self):
        rows = sweep.build_sweep_rows("speed", [
            ("100", summary_for(1), {"producer_events_per_s": 90.0,
                                     "producer": {"published": 5, "replay_lag_max_ms": 12}}),
            ("200", summary_for(1), {"producer": None})])
        self.assertEqual(list(rows[0]), list(sweep.summary_columns("speed")))
        self.assertEqual((rows[0]["speed"], rows[0]["replay_lag_max_ms"]), ("100", 12))
        self.assertIsNone(rows[2]["replay_lag_max_ms"])


class MainTest(unittest.TestCase):
    def run_sweep(self, directory, rates, failing=(), mode="--rates", producer_args=("-csvPath=a.csv",)):
        runner = FakeMeasure(failing)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = sweep.main(["--out", directory, mode, *map(str, rates), "--",
                                 *producer_args], runner=runner)
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
        self.assertEqual(list(rows[0]), list(sweep.summary_columns("rate")))
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

    def test_a_speed_sweep_runs_speed_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            status, runner = self.run_sweep(directory, ["100", "0.5"], mode="--speeds")
            rows = self.read_summary(directory)
            made = sorted(path.name for path in Path(directory).iterdir() if path.is_dir())
        self.assertEqual(status, 0)
        self.assertEqual([c[-1] for c in runner.commands], ["-speed=100", "-speed=0.5"])
        self.assertEqual(made, ["speed-0.5", "speed-100"])
        self.assertEqual(list(rows[0]), list(sweep.summary_columns("speed")))
        self.assertEqual({r["replay_lag_max_ms"] for r in rows}, {"7"})

    def test_rates_and_speeds_are_exclusive_and_invalid_speeds_are_refused(self):
        for argv in (["--rates", "500", "--speeds", "100"], ["--speeds", "0"],
                     ["--speeds", "nan"], ["--speeds", "-5"], ["--speeds", "fast"], []):
            with self.subTest(argv=argv), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    sweep.main(["--out", directory, *argv], runner=FakeMeasure())

    def test_the_sweep_owns_the_load_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.run_sweep(directory, ["100"], mode="--speeds",
                               producer_args=("-csvPath=a.csv", "-rate=1000"))


if __name__ == "__main__":
    unittest.main()
