#!/usr/bin/env python3
"""Tests for reset-flow.py's topic recreation.

recreate_topics is exercised with a fake runner standing in for
subprocess.run: every project topic must be deleted and recreated with
one partition and one replica (a trim-prefix reset would leave the low
watermark at the previous high watermark, and the flow validation
refuses to read topics whose head is truncated). The post-recreation
describe must list exactly the project topics with a pristine log —
log_start_offset and high_watermark both zero — or the reset fails
loudly rather than leaving a half-reset flow behind.
"""

import importlib.util
import json
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parent / "reset-flow.py"


def load_module():
    spec = importlib.util.spec_from_file_location("reset_flow", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def describe_payload(topics):
    """An rpk topic describe --format json body for the given topics.

    `topics` maps topic name to a list of partition entries: either a
    (log_start_offset, high_watermark) tuple or an already-shaped
    partition dict (for error cases).
    """
    return [
        {"summary": {"name": name}, "partitions": [
            partition if isinstance(partition, dict) else
            {"partition": index, "log_start_offset": partition[0],
             "high_watermark": partition[1]}
            for index, partition in enumerate(partitions)]}
        for name, partitions in topics.items()
    ]


class FakeRunner:
    """subprocess.run stand-in recording commands and faking output."""

    def __init__(self, describe_topics):
        self.commands = []
        self.describe_topics = describe_topics

    def __call__(self, command, **kwargs):
        self.commands.append(list(command))
        if "describe" in command:
            stdout = json.dumps(describe_payload(self.describe_topics))
            return type("Result", (), {"stdout": stdout})()
        return type("Result", (), {"stdout": ""})()


class RecreateTopicsTest(unittest.TestCase):
    """recreate_topics over a recorded command stream."""

    def setUp(self):
        self.mod = load_module()

    def recreate(self, topics):
        runner = FakeRunner(topics)
        self.mod.recreate_topics(run=runner)
        return runner

    def failing_recreate(self, topics):
        runner = FakeRunner(topics)
        with self.assertRaises(RuntimeError) as ctx:
            self.mod.recreate_topics(run=runner)
        return str(ctx.exception)

    def test_deletes_then_recreates_every_topic(self):
        runner = self.recreate({name: [(0, 0)] for name in self.mod.TOPICS})
        rpk = ["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "topic"]
        broker = ["-X", "brokers=redpanda-0:9092"]
        expected = []
        for topic in self.mod.TOPICS:
            expected.append([*rpk, "delete", topic, *broker])
            expected.append([*rpk, "create", topic, "--partitions", "1",
                             "--replicas", "1", *broker])
        expected.append([*rpk, "describe", *self.mod.TOPICS,
                         "--format", "json", *broker])
        self.assertEqual(runner.commands, expected)

    def test_passes_on_pristine_topics(self):
        runner = self.recreate({name: [(0, 0)] for name in self.mod.TOPICS})
        # every topic was verified exactly once via the describe output
        self.assertEqual(len(runner.commands),
                         2 * len(self.mod.TOPICS) + 1)

    def test_fails_when_describe_reports_records(self):
        topics = {name: [(0, 0)] for name in self.mod.TOPICS}
        topics["trade-events"] = [(0, 5)]
        message = self.failing_recreate(topics)
        self.assertIn("not empty", message)

    def test_fails_when_describe_reports_truncated_head(self):
        topics = {name: [(0, 0)] for name in self.mod.TOPICS}
        topics["ema-windows"] = [(7, 7)]
        message = self.failing_recreate(topics)
        self.assertIn("not empty", message)

    def test_fails_when_a_topic_is_missing(self):
        topics = {name: [(0, 0)] for name in self.mod.TOPICS}
        del topics["trade-advisories"]
        message = self.failing_recreate(topics)
        self.assertIn("Could not verify all project topics", message)

    def test_fails_when_a_partition_reports_an_error(self):
        topics = {name: [(0, 0)] for name in self.mod.TOPICS}
        topics["trade-events-dlq"] = [{"error": "leader not available"}]
        message = self.failing_recreate(topics)
        self.assertIn("Could not inspect trade-events-dlq", message)


if __name__ == "__main__":
    unittest.main(verbosity=2)