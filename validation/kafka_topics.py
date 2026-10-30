#!/usr/bin/env python3
"""Quiescent single-partition topic consumption for the full-flow validation.

consume_all(brokers, topic) reads every record currently in a topic and
returns (offset, value) pairs in offset order.  The topics under test are
single-partition and quiescent once the flow has finished, so the read is
a simple drain: capture the high watermark up front, consume from the
beginning until reaching it, then re-check — a watermark that grew
mid-read means records appeared while reading and the result would be
silently partial, so that (and more than one partition, or a poll
error) raises RuntimeError.

The default consumer factory is the only place confluent_kafka appears;
everything else, including these tests, drives the four plain methods
(list_topics, get_watermark_offsets, assign, poll) on an injected fake.
"""

POLL_TIMEOUT_S = 1.0


class _RealConsumer:
    """confluent_kafka.Consumer narrowed to the surface consume_all uses.

    Assignment is manual (assign() bypasses consumer-group management;
    the placeholder group.id exists only because librdkafka requires
    one) and auto-commit is off, so nothing is ever committed.
    """

    def __init__(self, brokers):
        from confluent_kafka import Consumer
        self._consumer = Consumer({
            "bootstrap.servers": brokers,
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "group.id": "full-flow-validation",
        })

    def list_topics(self, topic):
        metadata = self._consumer.list_topics(topic)
        topic_metadata = metadata.topics.get(topic)
        return set() if topic_metadata is None else set(topic_metadata.partitions)

    def get_watermark_offsets(self, topic, partition):
        return self._consumer.get_watermark_offsets(topic, partition)

    def assign(self, topic, partition):
        from confluent_kafka import OFFSET_BEGINNING, TopicPartition
        self._consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])

    def poll(self, timeout):
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            raise RuntimeError(
                f"kafka poll error on {message.topic()}[{message.partition()}]:"
                f" {error}")
        return (message.offset(), message.value())


def _default_consumer_factory(brokers):
    """Build the real consumer; the only path that needs confluent_kafka."""
    return _RealConsumer(brokers)


def _watermarks(consumer, topic, partition):
    """(low, high); the real query returns None on a broker timeout."""
    watermarks = consumer.get_watermark_offsets(topic, partition)
    if watermarks is None:
        raise RuntimeError(
            f"watermark query for topic {topic!r} partition {partition}"
            " timed out")
    return watermarks


def consume_all(brokers: str, topic: str,
                consumer_factory=None) -> list[tuple[int, bytes]]:
    """Read every record currently in `topic` as (offset, value) pairs.

    Requires exactly one partition and a quiescent topic: raises
    RuntimeError when the topic has more than one partition, when its
    high watermark grew between the pre-read capture and the post-read
    re-check (records appeared mid-read), or when a poll fails.
    """
    consumer = (consumer_factory or _default_consumer_factory)(brokers)
    partitions = consumer.list_topics(topic)
    if len(partitions) != 1:
        raise RuntimeError(
            f"topic {topic!r} has {len(partitions)} partitions,"
            " expected exactly one")
    partition = next(iter(partitions))
    low, high = _watermarks(consumer, topic, partition)
    consumer.assign(topic, partition)
    records = []
    next_offset = low
    while next_offset < high:
        record = consumer.poll(POLL_TIMEOUT_S)
        if record is None:
            continue
        offset, value = record
        records.append((offset, value))
        next_offset = offset + 1
    if _watermarks(consumer, topic, partition)[1] > high:
        raise RuntimeError(
            f"topic {topic!r} is not quiescent: high watermark grew past"
            f" {high} during the read")
    return records
