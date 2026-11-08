#!/usr/bin/env python3
"""Tests for the internal validation reset's topic recreation.

recreate_topics is exercised with a fake runner standing in for
subprocess.run: every project topic must be deleted and recreated with
one partition and one replica (a trim-prefix reset would leave the low
watermark at the previous high watermark, and the flow validation
refuses to read topics whose head is truncated). The post-recreation
describe must list exactly the project topics with a pristine log —
log_start_offset and high_watermark both zero — or the reset fails
loudly rather than leaving a half-reset flow behind.

`rpk topic create` returns once the controller has recorded the topic,
before the partition has a leader, and a recreated partition cannot start
until the old log of the same name is physically gone (about 30 s for the
12M-event topic of a full day). A topic that is not ready yet — summary or
partition error, no partitions, no leader — is therefore waited for, up to
a timeout, and reported with its last observed reason if it never is; a
topic that is ready but holds records fails immediately, since waiting
cannot empty it.
"""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parent / "reset-flow-data.py"


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


class SequencedRunner(FakeRunner):
    """FakeRunner whose successive describe calls return successive snapshots
    (the last one repeats), like a topic that becomes ready over time."""

    def __init__(self, snapshots):
        super().__init__(snapshots[-1])
        self.snapshots = list(snapshots)
        self.describes = 0

    def __call__(self, command, **kwargs):
        if "describe" in command:
            index = min(self.describes, len(self.snapshots) - 1)
            self.describes += 1
            self.describe_topics = self.snapshots[index]
        return super().__call__(command, **kwargs)


class FakeTime:
    """Fake sleep/monotonic: every sleep advances the clock by that much."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def monotonic(self):
        return self.now


def pristine(mod):
    return {name: [(0, 0)] for name in mod.TOPICS}


class RecreateTopicsTest(unittest.TestCase):
    """recreate_topics over a recorded command stream."""

    def setUp(self):
        self.mod = load_module()
        self.time = FakeTime()

    def recreate(self, topics):
        runner = FakeRunner(topics)
        self.mod.recreate_topics(run=runner, sleep=self.time.sleep,
                                 clock=self.time.monotonic)
        return runner

    def failing_recreate(self, topics):
        runner = FakeRunner(topics)
        with self.assertRaises(RuntimeError) as ctx:
            self.mod.recreate_topics(run=runner, sleep=self.time.sleep,
                                     clock=self.time.monotonic)
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


class TopicReadinessTest(unittest.TestCase):
    """A recreated topic may take a while to get a leader; waiting is bounded."""

    def setUp(self):
        self.mod = load_module()
        self.time = FakeTime()

    def recreate(self, snapshots, **kwargs):
        runner = SequencedRunner(snapshots)
        self.mod.recreate_topics(run=runner, sleep=self.time.sleep,
                                 clock=self.time.monotonic, **kwargs)
        return runner

    def with_events(self, partition):
        topics = pristine(self.mod)
        topics["trade-events"] = [partition]
        return topics

    def test_waits_for_a_partition_that_reports_an_error_then_succeeds(self):
        pending = self.with_events({"error": "LEADER_NOT_AVAILABLE"})
        runner = self.recreate([pending, pending, pending, pristine(self.mod)])
        self.assertEqual(runner.describes, 4)
        self.assertEqual(len(self.time.sleeps), 3)

    def test_waits_while_the_topic_lists_no_partitions(self):
        topics = pristine(self.mod)
        topics["trade-events"] = []
        runner = self.recreate([topics, pristine(self.mod)])
        self.assertEqual(runner.describes, 2)

    def test_waits_while_a_partition_has_no_leader(self):
        no_leader = self.with_events({"partition": 0, "leader": -1,
                                      "log_start_offset": 0, "high_watermark": 0})
        runner = self.recreate([no_leader, pristine(self.mod)])
        self.assertEqual(runner.describes, 2)

    def test_waits_while_a_topic_is_not_listed_yet(self):
        topics = pristine(self.mod)
        del topics["trade-events"]
        runner = self.recreate([topics, pristine(self.mod)])
        self.assertEqual(runner.describes, 2)

    def test_never_ready_fails_with_the_last_reason_after_the_timeout(self):
        stuck = self.with_events({"error": "LEADER_NOT_AVAILABLE"})
        with self.assertRaises(RuntimeError) as ctx:
            self.recreate([stuck], timeout_s=10, poll_s=2)
        message = str(ctx.exception)
        self.assertIn("Could not inspect trade-events", message)
        self.assertIn("not ready after 10s", message)
        self.assertEqual(sum(self.time.sleeps), 10)       # polled up to the deadline, no further

    def test_records_in_a_ready_topic_fail_at_once_without_waiting(self):
        dirty = self.with_events((0, 5))
        with self.assertRaisesRegex(RuntimeError, "not empty"):
            runner = SequencedRunner([dirty])
            self.mod.recreate_topics(run=runner, sleep=self.time.sleep,
                                     clock=self.time.monotonic)
        self.assertEqual(self.time.sleeps, [])
        self.assertEqual(runner.describes, 1)

    def test_records_win_over_a_pending_topic(self):
        topics = self.with_events((0, 5))
        topics["ema-windows"] = [{"error": "LEADER_NOT_AVAILABLE"}]
        with self.assertRaisesRegex(RuntimeError, "not empty"):
            self.recreate([topics])
        self.assertEqual(self.time.sleeps, [])

    def test_says_once_that_it_is_waiting_and_why(self):
        pending = self.with_events({"error": "LEADER_NOT_AVAILABLE"})
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.recreate([pending, pending, pending, pristine(self.mod)])
        self.assertEqual(out.getvalue().count("Waiting for the recreated topics"), 1)
        self.assertIn("Could not inspect trade-events", out.getvalue())

    def test_a_topic_that_is_ready_at_once_prints_nothing(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.recreate([pristine(self.mod)])
        self.assertEqual(out.getvalue(), "")

    def test_the_wait_defaults_come_from_config(self):
        self.assertEqual(self.mod.TOPIC_READY_TIMEOUT_S, 120.0)
        self.assertEqual(self.mod.TOPIC_READY_POLL_S, 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
