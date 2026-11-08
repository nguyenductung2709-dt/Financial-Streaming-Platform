#!/usr/bin/env python3
"""Run the materializer until it has consumed the result topics, then stop it.

The materializer never exits by itself and commits a topic's offset only
after the database transaction for those records, so its group's committed
offset reaching a topic's end offset means PostgreSQL holds everything the
topic holds.  This starts the materializer container (docker compose up -d
materializer, which gets its settings from Compose), waits for that on both
result topics (a timeout or an early exit is an error), then stops it with
SIGTERM and requires a clean exit.  Run it after the bounded processor
finished and before the database is compared with the topics.
"""

import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from flow import flow_lock
import containers

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
import config

TOPICS = (config.TOPIC_EMA_WINDOWS, config.TOPIC_TRADE_ADVISORIES)
DRAIN_TIMEOUT_S = config.DRAIN_TIMEOUT_S
DRAIN_POLL_S = config.DRAIN_POLL_S
MATERIALIZER_STOP_TIMEOUT_S = config.MATERIALIZER_STOP_TIMEOUT_S
OFFSET_QUERY_TIMEOUT_S = config.OFFSET_QUERY_TIMEOUT_S


class KafkaOffsets:
    """End offsets of a topic and the materializer group's committed offset.

    The consumer never subscribes or polls, so it does not join the group;
    all topics are single-partition (partition 0).
    """

    def __init__(self, brokers, group_id):
        from confluent_kafka import Consumer
        self._consumer = Consumer({
            "bootstrap.servers": brokers,
            "broker.address.family": "v4",
            "group.id": group_id,
            "enable.auto.commit": False,
        })

    def end_offsets(self, topic):
        from confluent_kafka import TopicPartition
        return self._consumer.get_watermark_offsets(
            TopicPartition(topic, 0), timeout=OFFSET_QUERY_TIMEOUT_S)

    def committed(self, topic):
        from confluent_kafka import TopicPartition
        offset = self._consumer.committed(
            [TopicPartition(topic, 0)], timeout=OFFSET_QUERY_TIMEOUT_S)[0].offset
        return offset if offset >= 0 else None

    def close(self):
        self._consumer.close()


def caught_up(offsets, topics):
    """Whether every topic is empty or fully committed by the group."""
    for topic in topics:
        low, high = offsets.end_offsets(topic)
        if high != low and offsets.committed(topic) != high:
            return False
    return True


def _progress(offsets, topics):
    parts = []
    for topic in topics:
        _, high = offsets.end_offsets(topic)
        parts.append(f"{topic} {offsets.committed(topic)}/{high}")
    return ", ".join(parts)


def _stop(process, stop_timeout_s):
    """SIGTERM, then SIGKILL after the timeout; returns the exit status."""
    process.terminate()
    try:
        return process.wait(timeout=stop_timeout_s)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise RuntimeError(f"The materializer did not stop within {stop_timeout_s:g}s of SIGTERM")


def drain(process, offsets, topics, timeout_s=DRAIN_TIMEOUT_S, poll_s=DRAIN_POLL_S,
          stop_timeout_s=MATERIALIZER_STOP_TIMEOUT_S, sleep=time.sleep,
          clock=time.monotonic, log=print):
    """Wait until `process` (the running materializer) has caught up, then stop it.

    Raises RuntimeError when it exits early, does not catch up within
    timeout_s, or does not exit 0 after SIGTERM; it is stopped on every path.
    """
    deadline = clock() + timeout_s
    next_report = clock()
    try:
        while True:
            returncode = process.poll()
            if returncode is not None:
                raise RuntimeError(f"The materializer exited with status {returncode} before it caught up")
            if caught_up(offsets, topics):
                break
            if clock() >= deadline:
                raise RuntimeError(f"The materializer did not catch up within {timeout_s:g}s"
                                   f" ({_progress(offsets, topics)})")
            if clock() >= next_report:
                log(f"Waiting for the materializer: {_progress(offsets, topics)}")
                next_report = clock() + 10
            sleep(poll_s)
        log(f"Materializer caught up ({_progress(offsets, topics)}); stopping it.")
        returncode = _stop(process, stop_timeout_s)
        if returncode != 0:
            raise RuntimeError(f"The materializer exited with status {returncode} on shutdown")
    finally:
        if process.poll() is None:
            _stop(process, stop_timeout_s)


def main():
    group_id = os.environ.get("MATERIALIZER_GROUP_ID", "")
    if not group_id or group_id != group_id.strip():
        raise RuntimeError("Set a nonempty, trimmed MATERIALIZER_GROUP_ID in .env")
    brokers = os.environ.get("MATERIALIZER_BROKERS", config.BROKERS)
    offsets = KafkaOffsets(brokers.split(",")[0].strip(), group_id)
    try:
        with flow_lock():
            process = containers.MaterializerContainer()
            process.start()
            drain(process, offsets, TOPICS)
    finally:
        offsets.close()
    print("Materializer drained.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (RuntimeError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
