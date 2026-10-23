#!/usr/bin/env python3
"""Cancel local processor jobs and clear the three project topics."""

import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
REST_URL = "http://localhost:8081"
TOPICS = ("trade-events", "ema-windows", "trade-events-dlq")
TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}


def get_json(path):
    with urllib.request.urlopen(REST_URL + path, timeout=5) as response:
        return json.load(response)


def main():
    print("Stop any running data producer with Ctrl+C before resetting.", flush=True)
    jobs = get_json("/jobs/overview")["jobs"]
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

    rpk = ["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "topic"]
    broker = ["-X", "brokers=redpanda-0:9092"]
    for topic in TOPICS:
        subprocess.run([
            *rpk, "trim-prefix", topic, "--offset", "end", "--no-confirm", *broker,
        ], cwd=ROOT, check=True)

    described = subprocess.run([
        *rpk, "describe", *TOPICS, "--format", "json", *broker,
    ], cwd=ROOT, text=True, capture_output=True, check=True)
    topics = json.loads(described.stdout)
    if {topic["summary"]["name"] for topic in topics} != set(TOPICS):
        raise RuntimeError("Could not verify all project topics")
    for topic in topics:
        if topic["summary"].get("error") or not topic["partitions"]:
            raise RuntimeError(f"Could not inspect {topic['summary']['name']}")
        for partition in topic["partitions"]:
            if partition.get("error") or partition["log_start_offset"] != partition["high_watermark"]:
                raise RuntimeError("Topics still contain records. Stop the producer and reset again.")

    (ROOT / "bin/streaming-processor.job-id").unlink(missing_ok=True)
    print("Reset complete: processor stopped; trade-events, ema-windows and trade-events-dlq are empty.")
    print("Infrastructure, topic configurations and checkpoint files are preserved.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        sys.exit(error.returncode)
    except (RuntimeError, urllib.error.URLError, TimeoutError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
