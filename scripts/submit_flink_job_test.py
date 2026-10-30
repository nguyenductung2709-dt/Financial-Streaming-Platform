#!/usr/bin/env python3
"""Tests for submit-flink-job.py.

wait_for_job is exercised over canned /jobs/{id} state sequences with a
stubbed get_json and a fake clock (sleep advances monotonic time, so the
1 s poll loop runs deterministically): live mode returns as soon as every
vertex is RUNNING, while bounded mode keeps polling past RUNNING until
FINISHED, another terminal state, or --bounded-timeout expiry. The
argparse layer is covered via parse_args, including the
`-- ...job args` REMAINDER shape the full-flow validation target uses.
"""

import contextlib
import importlib.util
import io
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "submit-flink-job.py"


def load_module():
    spec = importlib.util.spec_from_file_location("submit_flink_job", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeClock:
    """time.monotonic/time.sleep stand-in: sleeping advances the clock."""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def job_payload(state, running_tasks=True):
    """A /jobs/{id} body with one fully- or not-yet-running vertex."""
    return {
        "state": state,
        "vertices": [{"parallelism": 1,
                      "tasks": {"RUNNING": 1 if running_tasks else 0}}],
    }


class WaitForJobTest(unittest.TestCase):
    """wait_for_job over simulated state sequences."""

    def setUp(self):
        self.mod = load_module()
        self.mod.time = FakeClock()
        self.paths = []
        self.responses = []

        def fake_get_json(path):
            self.paths.append(path)
            return self.responses.pop(0)

        self.mod.get_json = fake_get_json

    def wait(self, **kwargs):
        """Call wait_for_job with stdout captured; return captured text."""
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.mod.wait_for_job(**kwargs)
        return buffer.getvalue()

    def failing_wait(self, **kwargs):
        """Call wait_for_job expecting RuntimeError; return the message."""
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as ctx:
                self.mod.wait_for_job(**kwargs)
        return str(ctx.exception)

    def test_live_returns_when_all_vertices_running(self):
        self.responses = [job_payload("CREATED", running_tasks=False),
                          job_payload("RUNNING")]
        output = self.wait(job_id="livejob", timeout=120, bounded=False)
        self.assertEqual(self.paths, ["/jobs/livejob"] * 2)
        self.assertIn("to start", output)

    def test_bounded_polls_past_running_until_finished(self):
        self.responses = [job_payload("RUNNING"), job_payload("FINISHED")]
        output = self.wait(job_id="boundedjob", timeout=120,
                           bounded_timeout=10, bounded=True)
        # RUNNING with all vertices running must NOT end the wait
        self.assertEqual(self.paths, ["/jobs/boundedjob"] * 2)
        self.assertIn("to finish", output)

    def test_bounded_returns_on_first_poll_finished(self):
        self.responses = [job_payload("FINISHED")]
        self.wait(job_id="fastjob", timeout=120, bounded_timeout=10,
                  bounded=True)
        self.assertEqual(self.paths, ["/jobs/fastjob"])

    def test_bounded_failed_state_raises(self):
        self.responses = [job_payload("RUNNING"), job_payload("FAILED")]
        message = self.failing_wait(job_id="failedjob", timeout=120,
                                    bounded_timeout=10, bounded=True)
        self.assertIn("ended with state FAILED", message)
        self.assertIn("/#/job/failedjob/overview", message)

    def test_bounded_timeout_expiry_raises(self):
        # one RUNNING poll, then the 1 s sleep exhausts the 1 s budget
        self.responses = [job_payload("RUNNING")]
        message = self.failing_wait(job_id="stuckjob", timeout=120,
                                    bounded_timeout=1, bounded=True)
        self.assertIn("did not finish within 1s", message)
        self.assertIn("/#/job/stuckjob/overview", message)
        self.assertEqual(self.paths, ["/jobs/stuckjob"])


class ParseArgsTest(unittest.TestCase):
    """parse_args: --bounded-timeout validation and the target call shape."""

    def setUp(self):
        self.mod = load_module()

    def parse(self, argv):
        """Run parse_args capturing stderr; return (options, stderr_text)."""
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            options = self.mod.parse_args(argv)
        return options, stderr.getvalue()

    def reject(self, argv, expected_stderr):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as ctx:
                self.mod.parse_args(argv)
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn(expected_stderr, stderr.getvalue())

    def test_bounded_timeout_zero_rejected(self):
        self.reject(["--bounded-timeout", "0"],
                    "bounded-timeout must be positive")

    def test_bounded_timeout_negative_rejected(self):
        self.reject(["--bounded-timeout", "-5"],
                    "bounded-timeout must be positive")

    def test_bounded_timeout_non_integer_rejected(self):
        self.reject(["--bounded-timeout", "soon"], "invalid int value")

    def test_bounded_timeout_defaults_to_one_hour(self):
        options, _ = self.parse([])
        self.assertEqual(options.timeout, 120)
        self.assertEqual(options.bounded_timeout, 3600)
        self.assertEqual(options.job_args, [])

    def test_validation_target_shape(self):
        # the Task 8 invocation: --timeout N --bounded-timeout M -- --bounded ...
        options, _ = self.parse([
            "--timeout", "300", "--bounded-timeout", "1800",
            "--", "--bounded", "--starting-offsets", "earliest"])
        self.assertEqual(options.timeout, 300)
        self.assertEqual(options.bounded_timeout, 1800)
        self.assertEqual(options.job_args,
                         ["--bounded", "--starting-offsets", "earliest"])


if __name__ == "__main__":
    unittest.main(verbosity=2)