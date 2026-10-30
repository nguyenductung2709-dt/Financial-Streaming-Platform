#!/usr/bin/env python3
"""Tests for kafka_topics.py.

consume_all is the validation transport: it drains a single-partition,
quiescent topic into (offset, value) pairs.  Every test drives a
FakeConsumer injected via consumer_factory, so confluent_kafka never
loads and no broker is needed — the real consumer surface (config keys,
TopicPartition, KafkaError) lives only in the default factory.
"""

import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import kafka_topics  # noqa: E402


class FakeConsumer:
    """In-memory stand-in for the four methods consume_all uses.

    Messages are (offset, value) pairs queued in fetch order; the high
    watermark is last offset + 1, like the real consumer's.
    """

    def __init__(self, messages=(), partitions=(0,)):
        self.messages = list(messages)
        self.partitions = set(partitions)
        self.high = max((offset for offset, _ in self.messages), default=-1) + 1
        self.requested_topic = None
        self.assigned = None

    def list_topics(self, topic):
        self.requested_topic = topic
        return self.partitions

    def get_watermark_offsets(self, topic, partition):
        return (0, self.high)

    def assign(self, topic, partition):
        self.assigned = (topic, partition)

    def poll(self, timeout):
        if not self.messages:
            return None
        return self.messages.pop(0)


class GrowingConsumer(FakeConsumer):
    """Records keep landing mid-read: the high watermark rises."""

    def poll(self, timeout):
        record = super().poll(timeout)
        self.messages.append((self.high, b"mid-read"))
        self.high += 1
        return record


class HappyPathTest(unittest.TestCase):
    def test_three_messages_drain_in_order_with_manual_assignment(self):
        fake = FakeConsumer([(0, b"m0"), (1, b"m1"), (2, b"m2")])
        factory_brokers = []

        def factory(brokers):
            factory_brokers.append(brokers)
            return fake

        records = kafka_topics.consume_all(
            "localhost:19092", "trade-events", consumer_factory=factory)
        self.assertEqual(records, [(0, b"m0"), (1, b"m1"), (2, b"m2")])
        self.assertEqual(factory_brokers, ["localhost:19092"])
        self.assertEqual(fake.requested_topic, "trade-events")
        self.assertEqual(fake.assigned, ("trade-events", 0))


class EmptyTopicTest(unittest.TestCase):
    def test_empty_topic_yields_no_records(self):
        fake = FakeConsumer([])
        records = kafka_topics.consume_all(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        self.assertEqual(records, [])


class WatermarkGrowthTest(unittest.TestCase):
    def test_high_watermark_growth_mid_read_raises(self):
        fake = GrowingConsumer([(0, b"m0"), (1, b"m1")])
        with self.assertRaises(RuntimeError):
            kafka_topics.consume_all(
                "localhost:19092", "trade-events", consumer_factory=lambda _: fake)


class MultiPartitionTest(unittest.TestCase):
    def test_two_partitions_raise(self):
        fake = FakeConsumer([(0, b"m0")], partitions=(0, 1))
        with self.assertRaises(RuntimeError):
            kafka_topics.consume_all(
                "localhost:19092", "trade-events", consumer_factory=lambda _: fake)


class OffsetOrderTest(unittest.TestCase):
    def test_offsets_come_back_strictly_ascending(self):
        messages = [(offset, bytes([offset])) for offset in range(5)]
        fake = FakeConsumer(messages)
        records = kafka_topics.consume_all(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        self.assertEqual([offset for offset, _ in records], [0, 1, 2, 3, 4])
        self.assertEqual(records, sorted(records))


class RealConsumerAdapterTest(unittest.TestCase):
    """_RealConsumer adapts the four-method surface to confluent-kafka.

    Consumer.get_watermark_offsets (2.x) requires a TopicPartition, so the
    adapter must forward one rather than the (topic, partition) pair.
    confluent_kafka stays stubbed via sys.modules: the real library never
    loads here either.
    """

    def test_watermark_query_passes_topic_partition(self):
        seen = []

        class StubTopicPartition:
            def __init__(self, topic, partition, offset=None):
                self.topic, self.partition = topic, partition

        class StubConsumer:
            def __init__(self, config):
                self.config = config

            def get_watermark_offsets(self, topic_partition):
                seen.append(topic_partition)
                return (0, 5)

        stub = types.ModuleType("confluent_kafka")
        stub.Consumer = StubConsumer
        stub.TopicPartition = StubTopicPartition
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            consumer = kafka_topics._default_consumer_factory(
                "localhost:19092")
            self.assertEqual(
                consumer.get_watermark_offsets("trade-events", 0), (0, 5))
            self.assertEqual(seen[0].topic, "trade-events")
            self.assertEqual(seen[0].partition, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
