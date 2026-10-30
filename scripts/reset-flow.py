#!/usr/bin/env python3
"""Stop the local flow, recreate its topics empty, and clear consumer offsets and database results."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

from flow import flow_lock

ROOT = Path(__file__).resolve().parent.parent
REST_URL = "http://localhost:8081"
TOPICS = ("trade-events", "trade-events-dlq", "ema-windows", "trade-advisories")
TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}


def get_json(path):
    with urllib.request.urlopen(REST_URL + path, timeout=5) as response:
        return json.load(response)


def start_services():
    """Bring up the infrastructure, create missing topics, and create missing tables."""
    subprocess.run([
        "docker", "compose", "up", "-d", "--wait", "--wait-timeout", "120",
        "postgres", "redpanda-0", "console", "jobmanager", "taskmanager",
    ], cwd=ROOT, check=True)
    subprocess.run(["docker", "compose", "run", "--rm", "create-topic"], cwd=ROOT, check=True)
    subprocess.run([str(ROOT / "bin/materializer"), "-init-db"], cwd=ROOT, check=True)


def stop_jobs():
    deadline = time.monotonic() + 120
    while True:
        try:
            jobs = get_json("/jobs/overview")["jobs"]
            break
        except (urllib.error.URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise RuntimeError("Flink did not become available; no data was cleared")
            time.sleep(1)
    for job in jobs:
        if job["name"] != "market-price-event-consumer" or job["state"] in TERMINAL_STATES:
            continue
        job_id = job["jid"]
        if job["state"] != "CANCELLING":
            try:
                subprocess.run([
                    "docker", "compose", "exec", "-T", "jobmanager",
                    "flink", "cancel", job_id,
                ], cwd=ROOT, check=True)
            except subprocess.CalledProcessError:
                if get_json(f"/jobs/{job_id}")["state"] not in TERMINAL_STATES:
                    raise
        deadline = time.monotonic() + 120
        while get_json(f"/jobs/{job_id}")["state"] not in TERMINAL_STATES:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Job {job_id} has not stopped; topics were not cleared")
            time.sleep(1)


def remove_materializer_group(group_id):
    """Delete the materializer's consumer group so a fresh run starts at offset 0."""
    broker = ["-X", "brokers=redpanda-0:9092"]
    groups = subprocess.run([
        "docker", "compose", "exec", "-T", "redpanda-0", "rpk", "group", "list",
        "--format", "json", *broker,
    ], cwd=ROOT, text=True, capture_output=True, check=True)
    for group in json.loads(groups.stdout):
        if group["group"] != group_id:
            continue
        if group.get("state", "").lower() not in {"empty", "dead"}:
            raise RuntimeError("The materializer group is still active; stop separately started materializers before resetting")
        deleted = subprocess.run([
            "docker", "compose", "exec", "-T", "redpanda-0", "rpk", "group", "delete", group_id,
            "--format", "json", *broker,
        ], cwd=ROOT, text=True, capture_output=True, check=True)
        if any(result["status"] != "OK" for result in json.loads(deleted.stdout)):
            raise RuntimeError("Could not remove the materializer group; topics and read models were not cleared")


def recreate_topics(run=None):
    """Delete and recreate every project topic with a pristine, empty log.

    Clearing records in place (trim-prefix) leaves the low watermark at
    the previous high watermark; the flow validation refuses to read a
    topic whose head is truncated that way, so the reset must recreate
    the topics instead (one partition, one replica, like create-topic).
    `run` defaults to subprocess.run looked up at call time, so callers
    that patch subprocess.run never reach the real broker.
    """
    run = run or subprocess.run
    rpk =["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "topic"]
    broker = ["-X", "brokers=redpanda-0:9092"]
    for topic in TOPICS:
        run([*rpk, "delete", topic, *broker], cwd=ROOT, check=True)
        run([*rpk, "create", topic, "--partitions", "1", "--replicas", "1", *broker],
            cwd=ROOT, check=True)

    described = run([*rpk, "describe", *TOPICS, "--format", "json", *broker],
                    cwd=ROOT, text=True, capture_output=True, check=True)
    topics = json.loads(described.stdout)
    if {topic["summary"]["name"] for topic in topics} != set(TOPICS):
        raise RuntimeError("Could not verify all project topics")
    for topic in topics:
        if topic["summary"].get("error") or not topic["partitions"]:
            raise RuntimeError(f"Could not inspect {topic['summary']['name']}")
        for partition in topic["partitions"]:
            if partition.get("error"):
                raise RuntimeError(f"Could not inspect {topic['summary']['name']}")
            if partition["log_start_offset"] != 0 \
                    or partition["high_watermark"] != 0:
                raise RuntimeError("Recreated topics are not empty. Stop the producer and reset again.")


def reset():
    print("Stop any separately started producer or materializer before resetting.", flush=True)
    group_id = os.environ.get("MATERIALIZER_GROUP_ID", "")
    if not group_id or group_id != group_id.strip():
        raise RuntimeError("Set a nonempty, trimmed MATERIALIZER_GROUP_ID in .env")
    start_services()
    stop_jobs()
    remove_materializer_group(group_id)
    recreate_topics()

    subprocess.run([str(ROOT / "bin/materializer"), "-reset-db"], cwd=ROOT, check=True)
    (ROOT / "bin/streaming-processor.job-id").unlink(missing_ok=True)
    print(f"Reset complete: flow stopped; {', '.join(TOPICS)} recreated empty and PostgreSQL read models cleared.")
    print("Infrastructure and checkpoint files are preserved.")


if __name__ == "__main__":
    try:
        with flow_lock(stop_running=True):
            reset()
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        sys.exit(error.returncode)
    except (RuntimeError, urllib.error.URLError, TimeoutError, OSError, ValueError, KeyError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
