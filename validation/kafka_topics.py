#!/usr/bin/env python3
"""Quiescent single-partition topic consumption for the full-flow validation.

iter_records(brokers, topic) yields every record currently in a topic as
(offset, value) pairs in offset order; stream_records yields the same
records as (offset, value, timestamp_ms) triples; consume_all collects
iter_records into a list.  The topics under test are single-partition and
quiescent once the flow has finished, so the read is a simple drain:
capture the high watermark up front, consume from the beginning until
reaching it, then re-check — a watermark that grew mid-read means records
appeared while reading and the result would be silently partial, so that
(and more than one partition, or a poll error) raises RuntimeError.
Retention truncation is the same sin in two shapes — a purged segment
mid-read (polled offsets skip forward) and a topic head already truncated
(low watermark past 0) — and both raise RuntimeError rather than silently
under-validate.

The default consumer factory is the only place confluent_kafka appears;
everything else, including these tests, drives the five plain methods
(list_topics, get_watermark_offsets, assign, poll, close) on an injected
fake.
"""

import time
from typing import Iterator

import config

POLL_TIMEOUT_S = config.KAFKA_POLL_TIMEOUT_S
WATERMARK_ATTEMPTS = config.WATERMARK_ATTEMPTS
WATERMARK_BACKOFF_S = config.WATERMARK_BACKOFF_S
CONSUMER_GROUP_ID = config.CONSUMER_GROUP_ID


class _RealConsumer:
    """confluent_kafka.Consumer narrowed to the surface iter_records uses.

    Assignment is manual (assign() bypasses consumer-group management;
    the placeholder group.id exists only because librdkafka requires
    one) and auto-commit is off, so nothing is ever committed.
    """

    def __init__(self, brokers):
        from confluent_kafka import Consumer
        self._consumer = Consumer({
            "bootstrap.servers": brokers,
            "broker.address.family": "v4",  # the published Redpanda port is IPv4-only
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "group.id": CONSUMER_GROUP_ID,
        })

    def list_topics(self, topic):
        metadata = self._consumer.list_topics(topic)
        topic_metadata = metadata.topics.get(topic)
        return set() if topic_metadata is None else set(topic_metadata.partitions)

    def get_watermark_offsets(self, topic, partition):
        """Retry transient failures; the query is otherwise one-shot.

        A fresh consumer's one-shot query can race its own broker
        connection bring-up (e.g. an advertised name that resolves to a
        refused address first) and fail with a local transport error
        even though the broker is fine; metadata and fetches retry
        internally, this query does not.
        """
        from confluent_kafka import KafkaError, KafkaException, TopicPartition
        transient = {KafkaError._ALL_BROKERS_DOWN,
                     KafkaError._DESTROY_BROKER,
                     KafkaError._TRANSPORT}
        for attempt in range(WATERMARK_ATTEMPTS):
            try:
                return self._consumer.get_watermark_offsets(
                    TopicPartition(topic, partition))
            except KafkaException as error:
                if (attempt == WATERMARK_ATTEMPTS - 1
                        or error.args[0].code() not in transient):
                    raise
                time.sleep(WATERMARK_BACKOFF_S)

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
        return (message.offset(), message.value(), message.timestamp()[1])

    def close(self):
        self._consumer.close()


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


def topic_partitions(brokers: str, topic: str,
                     consumer_factory=None) -> set[int]:
    """The partitions of `topic`; empty when the topic does not exist.

    The same consumer surface stream_records uses, closed after one
    metadata query; a topic that exists only in a stale deployment reports
    no partitions instead of a failed read.
    """
    consumer = (consumer_factory or _default_consumer_factory)(brokers)
    try:
        return consumer.list_topics(topic)
    finally:
        consumer.close()


def stream_records(brokers: str, topic: str,
                   consumer_factory=None) -> Iterator[tuple[int, bytes, int]]:
    """Yield every record currently in `topic` as (offset, value, timestamp_ms).

    One record at a time, so a caller that folds them into a smaller
    summary never holds the whole topic.  The checks that can only be
    made at the end (a high watermark that grew during the read) run when
    the iterator is exhausted, so the caller must consume it fully; the
    consumer is closed when it ends, fails, or is closed early.

    Requires exactly one partition and a quiescent topic: raises
    RuntimeError when the topic has more than one partition, when its
    high watermark grew between the pre-read capture and the post-read
    re-check (records appeared mid-read; raised once the last record has
    been yielded), or when a poll fails.  Retention truncation also
    raises: a low watermark already past 0 (the head was purged before
    the run) or a polled offset skipping past the expected next offset
    (a segment was purged mid-read) - either way the read would be
    silently partial.  A consumer that carries no timestamp reports -1.
    """
    consumer = (consumer_factory or _default_consumer_factory)(brokers)
    try:
        partitions = consumer.list_topics(topic)
        if len(partitions) != 1:
            raise RuntimeError(
                f"topic {topic!r} is missing or has {len(partitions)}"
                " partitions, expected exactly one")
        partition = next(iter(partitions))
        low, high = _watermarks(consumer, topic, partition)
        if low > 0:
            raise RuntimeError(
                f"topic {topic!r} head already truncated by retention:"
                f" low watermark is {low}, not 0 (records [0, {low}) are"
                " gone; reading now would be silently partial)")
        consumer.assign(topic, partition)
        next_offset = low
        while next_offset < high:
            record = consumer.poll(POLL_TIMEOUT_S)
            if record is None:
                continue
            offset, value, *rest = record
            if offset != next_offset:
                raise RuntimeError(
                    f"topic {topic!r} read gap: polled offset {offset},"
                    f" expected {next_offset} (retention likely purged a"
                    " segment mid-read; the skipped records would make"
                    " the read silently partial)")
            next_offset = offset + 1
            yield offset, value, rest[0] if rest else -1
        if _watermarks(consumer, topic, partition)[1] > high:
            raise RuntimeError(
                f"topic {topic!r} is not quiescent: high watermark grew past"
                f" {high} during the read")
    finally:
        consumer.close()


def iter_records(brokers: str, topic: str,
                 consumer_factory=None) -> Iterator[tuple[int, bytes]]:
    """Yield every record currently in `topic` as (offset, value) pairs.

    stream_records without the timestamps: same checks, same contract.
    """
    records = stream_records(brokers, topic, consumer_factory)
    try:
        for offset, value, _ in records:
            yield offset, value
    finally:
        records.close()


def consume_all(brokers: str, topic: str,
                consumer_factory=None) -> list[tuple[int, bytes]]:
    """Read every record currently in `topic` as (offset, value) pairs.

    iter_records collected into a list, for topics small enough to hold.
    """
    return list(iter_records(brokers, topic, consumer_factory))
