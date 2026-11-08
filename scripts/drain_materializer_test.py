#!/usr/bin/env python3
"""Tests for drain-materializer.py.

The materializer runs forever and commits a topic's offset only after the
database transaction for those records, so "committed offset == end offset
on every result topic" means the database holds everything the topics hold.
drain() starts from that predicate: it must leave the process running until
every topic is caught up, then stop it with SIGTERM and require a clean exit;
a materializer that dies early, never catches up, or exits non-zero on
shutdown is an error, and the process is stopped on every path.  The real
Kafka offsets reader is exercised with a stubbed confluent_kafka module.
"""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parent / "drain-materializer.py"
EMA, ADV = "ema-windows", "trade-advisories"
TOPICS = (EMA, ADV)


def load_module():
    spec = importlib.util.spec_from_file_location("drain_materializer", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeOffsets:
    """end_offsets/committed over a script: committed[i] is the state at poll i."""

    def __init__(self, ends, committed_script):
        self.ends = ends
        self.script = committed_script
        self.poll = 0

    def end_offsets(self, topic):
        return self.ends[topic]

    def committed(self, topic):
        state = self.script[min(self.poll, len(self.script) - 1)]
        return state[topic]


class FakeProcess:
    def __init__(self, exits_at_poll=None, returncode_on_exit=1, stop_returncode=0):
        self.returncode = None
        self.terminated = False
        self.killed = False
        self.polls = 0
        self._exits_at_poll = exits_at_poll
        self._returncode_on_exit = returncode_on_exit
        self._stop_returncode = stop_returncode

    def poll(self):
        self.polls += 1
        if self._exits_at_poll is not None and self.polls >= self._exits_at_poll:
            self.returncode = self._returncode_on_exit
        return self.returncode

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = self._stop_returncode
        return self.returncode

    def kill(self):
        self.killed = True


class Clock:
    """Fake time: each sleep advances it and the offsets script by one poll."""

    def __init__(self, offsets=None):
        self.now = 0.0
        self.sleeps = []
        self.offsets = offsets

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds
        if self.offsets is not None:
            self.offsets.poll += 1


class CaughtUpTest(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()

    def test_empty_topics_are_caught_up_without_any_commit(self):
        offsets = FakeOffsets({EMA: (0, 0), ADV: (0, 0)}, [{EMA: None, ADV: None}])
        self.assertTrue(self.mod.caught_up(offsets, TOPICS))

    def test_a_topic_is_caught_up_only_when_the_commit_reaches_its_end(self):
        ends = {EMA: (0, 10), ADV: (0, 4)}
        behind = FakeOffsets(ends, [{EMA: 9, ADV: 4}])
        done = FakeOffsets(ends, [{EMA: 10, ADV: 4}])
        self.assertFalse(self.mod.caught_up(behind, TOPICS))
        self.assertTrue(self.mod.caught_up(done, TOPICS))

    def test_no_commit_yet_on_a_topic_with_records_is_not_caught_up(self):
        offsets = FakeOffsets({EMA: (0, 10), ADV: (0, 0)}, [{EMA: None, ADV: None}])
        self.assertFalse(self.mod.caught_up(offsets, TOPICS))


class DrainTest(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.ends = {EMA: (0, 10), ADV: (0, 4)}

    def drain(self, process, script, timeout_s=100.0):
        offsets = FakeOffsets(self.ends, script)
        clock = Clock(offsets)
        self.mod.drain(process, offsets, TOPICS, timeout_s=timeout_s, poll_s=1.0,
                       stop_timeout_s=5.0, sleep=clock.sleep, clock=clock.monotonic,
                       log=lambda message: None)
        return clock

    def test_stops_the_materializer_once_every_topic_is_caught_up(self):
        process = FakeProcess()
        clock = self.drain(process, [{EMA: None, ADV: None}, {EMA: 5, ADV: 4},
                                     {EMA: 10, ADV: 4}])
        self.assertTrue(process.terminated)
        self.assertEqual(len(clock.sleeps), 2)           # waited through two polls
        self.assertEqual(process.returncode, 0)

    def test_does_not_stop_while_one_topic_is_still_behind(self):
        process = FakeProcess()
        clock = self.drain(process, [{EMA: 10, ADV: 1}] * 4 + [{EMA: 10, ADV: 4}])
        self.assertEqual(len(clock.sleeps), 4)
        self.assertTrue(process.terminated)

    def test_times_out_and_still_stops_the_process(self):
        process = FakeProcess()
        with self.assertRaisesRegex(RuntimeError, "did not catch up within 5"):
            self.drain(process, [{EMA: 1, ADV: 1}], timeout_s=5.0)
        self.assertTrue(process.terminated)

    def test_a_materializer_that_dies_early_is_an_error(self):
        process = FakeProcess(exits_at_poll=2, returncode_on_exit=3)
        with self.assertRaisesRegex(RuntimeError, "exited with status 3 before"):
            self.drain(process, [{EMA: 1, ADV: 1}])
        self.assertFalse(process.terminated)               # already gone

    def test_a_nonzero_exit_on_shutdown_is_an_error(self):
        process = FakeProcess(stop_returncode=2)
        with self.assertRaisesRegex(RuntimeError, "exited with status 2 on shutdown"):
            self.drain(process, [{EMA: 10, ADV: 4}])

    def test_a_process_that_ignores_sigterm_is_killed(self):
        process = FakeProcess()

        def stuck_wait(timeout=None):
            if not process.killed:
                raise subprocess.TimeoutExpired("materializer", timeout)
            process.returncode = -9
            return -9

        process.wait = stuck_wait
        with self.assertRaisesRegex(RuntimeError, "did not stop"):
            self.drain(process, [{EMA: 10, ADV: 4}])
        self.assertTrue(process.killed)


class KafkaOffsetsTest(unittest.TestCase):
    """The reader over a stubbed confluent_kafka (the real library never loads here)."""

    def stub(self, watermarks, committed):
        seen = {}

        class TopicPartition:
            def __init__(self, topic, partition, offset=-1001):
                self.topic, self.partition, self.offset = topic, partition, offset

        class Consumer:
            def __init__(self, config):
                seen["config"] = config

            def get_watermark_offsets(self, tp, timeout=None):
                seen["watermark_timeout"] = timeout
                return watermarks[tp.topic]

            def committed(self, partitions, timeout=None):
                return [TopicPartition(tp.topic, tp.partition, committed[tp.topic])
                        for tp in partitions]

            def close(self):
                seen["closed"] = True

        module = types.ModuleType("confluent_kafka")
        module.Consumer, module.TopicPartition = Consumer, TopicPartition
        return module, seen

    def test_reads_end_offsets_and_commits_of_the_materializer_group(self):
        mod = load_module()
        stub, seen = self.stub({EMA: (0, 12)}, {EMA: 12})
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            offsets = mod.KafkaOffsets("localhost:19092", "materializer")
            self.assertEqual(offsets.end_offsets(EMA), (0, 12))
            self.assertEqual(offsets.committed(EMA), 12)
            offsets.close()
        self.assertEqual(seen["config"]["group.id"], "materializer")
        self.assertEqual(seen["config"]["bootstrap.servers"], "localhost:19092")
        self.assertEqual(seen["config"]["broker.address.family"], "v4")
        self.assertFalse(seen["config"]["enable.auto.commit"])
        self.assertTrue(seen["closed"])

    def test_a_group_with_no_commit_reads_as_none(self):
        mod = load_module()
        stub, _ = self.stub({EMA: (0, 12)}, {EMA: -1001})
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            self.assertIsNone(mod.KafkaOffsets("b:1", "g").committed(EMA))


class MainTest(unittest.TestCase):
    def test_starts_the_container_and_drains_it_without_a_database_url(self):
        mod = load_module()
        container = mock.MagicMock()
        with mock.patch.dict(os.environ, {"MATERIALIZER_GROUP_ID": "g"}, clear=True), \
                mock.patch.object(mod.containers, "MaterializerContainer", return_value=container), \
                mock.patch.object(mod, "KafkaOffsets") as offsets, \
                mock.patch.object(mod, "drain") as drain, \
                mock.patch.object(mod, "flow_lock"):
            mod.main()
        container.start.assert_called_once_with()
        drain.assert_called_once()
        self.assertIs(drain.call_args.args[0], container)
        offsets.return_value.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
