#!/usr/bin/env python3
"""Tests for kafka_topics.py.

iter_records is the validation transport: it streams a single-partition,
quiescent topic as (offset, value) pairs, and consume_all collects them.  Every test drives a
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
    """In-memory stand-in for the five methods consume_all uses.

    Messages are (offset, value) pairs queued in fetch order; the low
    watermark defaults to 0 and the high watermark is last offset + 1,
    like the real consumer's.
    """

    def __init__(self, messages=(), partitions=(0,), low=0):
        self.messages = list(messages)
        self.partitions = set(partitions)
        self.low = low
        self.high = max((offset for offset, _ in self.messages), default=-1) + 1
        self.requested_topic = None
        self.assigned = None
        self.closed = False

    def list_topics(self, topic):
        self.requested_topic = topic
        return self.partitions

    def get_watermark_offsets(self, topic, partition):
        return (self.low, self.high)

    def assign(self, topic, partition):
        self.assigned = (topic, partition)

    def poll(self, timeout):
        if not self.messages:
            return None
        return self.messages.pop(0)

    def close(self):
        self.closed = True


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
        self.assertTrue(fake.closed)


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
        self.assertTrue(fake.closed)


class RetentionGapTest(unittest.TestCase):
    """A purged segment mid-read: polled offsets skip forward, the
    skipped records would be a silently partial PASS, so it raises."""

    def test_offset_gap_mid_read_raises(self):
        fake = FakeConsumer([(0, b"m0"), (2, b"m2")])  # offset 1 is gone
        with self.assertRaises(RuntimeError) as raised:
            kafka_topics.consume_all(
                "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        message = str(raised.exception)
        self.assertIn("gap", message)
        self.assertIn("2", message)
        self.assertIn("1", message)
        self.assertIn("retention", message)
        self.assertTrue(fake.closed)


class TruncatedHeadTest(unittest.TestCase):
    """Retention already purged the topic head before the run: the
    low watermark is past 0, so reading now would silently skip
    everything before it."""

    def test_nonzero_low_watermark_raises_before_any_poll(self):
        fake = FakeConsumer([(5, b"m5")], low=5)
        with self.assertRaises(RuntimeError) as raised:
            kafka_topics.consume_all(
                "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        message = str(raised.exception)
        self.assertIn("trade-events", message)
        self.assertIn("low", message)
        self.assertIn("5", message)
        self.assertIn("retention", message)
        self.assertIsNone(fake.assigned)  # raised before assign/poll
        self.assertTrue(fake.closed)


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
    """_RealConsumer adapts the five-method surface to confluent-kafka.

    Consumer.get_watermark_offsets (2.x) requires a TopicPartition, so the
    adapter must forward one rather than the (topic, partition) pair; a
    transient failure of the otherwise one-shot query (local transport
    error while the broker connection races bring-up) is retried, while a
    permanent error surfaces immediately.  confluent_kafka stays stubbed
    via sys.modules: the real library never loads here either.
    """

    def _stub(self, responses):
        seen = []

        class StubKafkaError:
            _ALL_BROKERS_DOWN = -151
            _DESTROY_BROKER = -137
            _TRANSPORT = -195

            def __init__(self, code):
                self._code = code

            def code(self):
                return self._code

        class StubKafkaException(Exception):
            pass

        class StubTopicPartition:
            def __init__(self, topic, partition, offset=None):
                self.topic, self.partition = topic, partition

        class StubConsumer:
            def __init__(self, config):
                self.config = config

            def get_watermark_offsets(self, topic_partition):
                seen.append(topic_partition)
                response = responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

            def close(self):
                pass

        stub = types.ModuleType("confluent_kafka")
        stub.Consumer = StubConsumer
        stub.TopicPartition = StubTopicPartition
        stub.KafkaException = StubKafkaException
        stub.KafkaError = StubKafkaError
        return stub, seen

    def test_watermark_query_passes_topic_partition(self):
        stub, seen = self._stub([(0, 5)])
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            consumer = kafka_topics._default_consumer_factory(
                "localhost:19092")
            self.assertEqual(
                consumer.get_watermark_offsets("trade-events", 0), (0, 5))
            self.assertEqual(seen[0].topic, "trade-events")
            self.assertEqual(seen[0].partition, 0)

    def test_consumer_dials_ipv4_only(self):
        # Docker publishes the Redpanda port on IPv4 only, but "localhost"
        # resolves to ::1 first; without this every run logs refused ::1
        # connections that look like failures.
        stub, _ = self._stub([])
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            consumer = kafka_topics._default_consumer_factory("localhost:19092")
        self.assertEqual(consumer._consumer.config["broker.address.family"], "v4")

    def test_watermark_query_retries_transient_failure(self):
        responses = [None, None, (0, 5)]
        stub, seen = self._stub(responses)
        for attempt in (0, 1):
            responses[attempt] = stub.KafkaException(
                type("error", (), {"code": lambda self: -195})())
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            with mock.patch.object(kafka_topics.time, "sleep"):
                consumer = kafka_topics._default_consumer_factory(
                    "localhost:19092")
                self.assertEqual(
                    consumer.get_watermark_offsets("trade-events", 0), (0, 5))
                self.assertEqual(len(seen), 3)

    def test_watermark_query_raises_permanent_error_immediately(self):
        responses = [None, (0, 5)]
        stub, seen = self._stub(responses)
        responses[0] = stub.KafkaException(
            type("error", (), {"code": lambda self: -3})())
        with mock.patch.dict(sys.modules, {"confluent_kafka": stub}):
            consumer = kafka_topics._default_consumer_factory(
                "localhost:19092")
            with self.assertRaises(stub.KafkaException):
                consumer.get_watermark_offsets("trade-events", 0)
            self.assertEqual(len(seen), 1)


class IterRecordsTest(unittest.TestCase):
    def test_records_are_read_lazily_and_the_consumer_closes_at_the_end(self):
        fake = FakeConsumer([(0, b"m0"), (1, b"m1"), (2, b"m2")])
        records = kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        self.assertEqual(next(records), (0, b"m0"))
        self.assertEqual(len(fake.messages), 2)  # nothing read ahead
        self.assertFalse(fake.closed)
        self.assertEqual(list(records), [(1, b"m1"), (2, b"m2")])
        self.assertTrue(fake.closed)

    def test_closing_early_releases_the_consumer(self):
        fake = FakeConsumer([(0, b"m0"), (1, b"m1")])
        records = kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        next(records)
        records.close()
        self.assertTrue(fake.closed)

    def test_nothing_is_opened_until_the_first_record_is_requested(self):
        built = []
        kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=built.append)
        self.assertEqual(built, [])

    def test_a_gap_raises_after_the_records_before_it_were_yielded(self):
        fake = FakeConsumer([(0, b"m0"), (2, b"m2")])  # offset 1 is gone
        records = kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        self.assertEqual(next(records), (0, b"m0"))
        with self.assertRaisesRegex(RuntimeError, "gap"):
            next(records)
        self.assertTrue(fake.closed)

    def test_growth_is_reported_only_once_the_stream_is_exhausted(self):
        fake = GrowingConsumer([(0, b"m0"), (1, b"m1")])
        records = kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=lambda _: fake)
        self.assertEqual(next(records), (0, b"m0"))
        self.assertEqual(next(records), (1, b"m1"))
        with self.assertRaisesRegex(RuntimeError, "not quiescent"):
            next(records)
        self.assertTrue(fake.closed)

    def test_consume_all_is_the_collected_stream(self):
        messages = [(offset, bytes([offset])) for offset in range(4)]
        collected = kafka_topics.consume_all(
            "localhost:19092", "trade-events", consumer_factory=lambda _: FakeConsumer(messages))
        streamed = list(kafka_topics.iter_records(
            "localhost:19092", "trade-events", consumer_factory=lambda _: FakeConsumer(messages)))
        self.assertEqual(collected, streamed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
