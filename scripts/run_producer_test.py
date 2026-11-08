#!/usr/bin/env python3
"""Tests for run-producer.py: the producer container runs in the foreground and is always stopped."""

import contextlib
import importlib.util
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))


def load_module():
    spec = importlib.util.spec_from_file_location("run_producer", SCRIPTS / "run-producer.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_producer = load_module()


class RunProducerTest(unittest.TestCase):
    def run_main(self, argv, result=None, error=None, run=None):
        runner = mock.Mock(side_effect=run or error, return_value=result)
        stopper = mock.Mock()
        with mock.patch.object(run_producer.subprocess, "run", runner), \
                mock.patch.object(run_producer.containers, "stop_container", stopper):
            status = run_producer.main(argv)
        return status, runner, stopper

    def test_help_runs_without_a_dataset(self):
        status, runner, stopper = self.run_main(["-help"], result=subprocess.CompletedProcess([], 0))
        self.assertEqual(status, 0)
        command = runner.call_args.args[0]
        self.assertEqual(command[-2:], ["data-producer", "-help"])
        self.assertNotIn("-v", command)

    def test_returns_the_producers_exit_status_and_always_stops_the_container(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.csv"
            path.write_text("x")
            status, runner, stopper = self.run_main([f"-csvPath={path}", "-dlqEnabled"], result=subprocess.CompletedProcess([], 3))
        self.assertEqual(status, 3)
        name = runner.call_args.args[0][runner.call_args.args[0].index("--name") + 1]
        self.assertEqual(name, f"data-producer-run-{os.getpid()}")
        stopper.assert_called_once_with(name)

    def test_interrupt_exits_130_and_stops_the_container(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.csv"
            path.write_text("x")
            status, _, stopper = self.run_main([f"-csvPath={path}"], error=KeyboardInterrupt)
        self.assertEqual(status, 130)
        stopper.assert_called_once()

    def test_sigterm_and_sighup_stop_the_container_like_ctrl_c(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "a.csv"
            path.write_text("x")
            for sig in (signal.SIGTERM, signal.SIGHUP):
                # A no-op stands in for the default action, so a missing handler fails this test instead of killing the runner.
                original = signal.signal(sig, lambda *_: None)
                try:
                    before = signal.getsignal(sig)

                    def deliver(command, **kwargs):
                        os.kill(os.getpid(), sig)
                        time.sleep(2)  # the handler under test interrupts this

                    status, _, stopper = self.run_main([f"-csvPath={path}"], run=deliver)
                    self.assertEqual(status, 130, sig.name)
                    stopper.assert_called_once()
                    self.assertIs(signal.getsignal(sig), before, "the previous handler is restored")
                finally:
                    signal.signal(sig, original)

    def test_a_bad_input_exits_1_without_running_docker(self):
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            status, runner, stopper = self.run_main(["-csvPath=/nonexistent/day.csv"])
        self.assertEqual(status, 1)
        self.assertIn("Cannot use the CSV input", errors.getvalue())
        runner.assert_not_called()
        stopper.assert_not_called()


if __name__ == "__main__":
    unittest.main()
