#!/usr/bin/env python3
"""Submit the local processor after Flink is ready, then wait for its tasks.

With a --bounded job arg, instead wait for the run to reach FINISHED
within --bounded-timeout seconds."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
REST_URL = "http://localhost:8081"
JOB_NAME = "market-price-event-consumer"
TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}


def get_json(path):
    with urllib.request.urlopen(REST_URL + path, timeout=5) as response:
        return json.load(response)


def wait_for_cluster(timeout):
    print("Waiting for Flink and a registered TaskManager...", flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            overview = get_json("/overview")
            if overview.get("taskmanagers", 0) > 0 and overview.get("slots-total", 0) > 0:
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(1)
    raise RuntimeError(f"Flink has no registered TaskManager slots after {timeout}s: {REST_URL}")


def wait_for_job(job_id, timeout, bounded_timeout=None, bounded=False):
    goal = "finish" if bounded else "start"
    print(f"Waiting for job {job_id} to {goal}...", flush=True)
    wait_timeout = bounded_timeout if bounded else timeout
    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        try:
            job = get_json(f"/jobs/{job_id}")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(1)
            continue
        state = job["state"]
        vertices = job.get("vertices", [])
        if state == "FINISHED" and bounded:
            return
        if state in TERMINAL_STATES:
            raise RuntimeError(f"Job {job_id} ended with state {state}; inspect {REST_URL}/#/job/{job_id}/overview")
        if not bounded and state == "RUNNING" and vertices and all(
            vertex.get("tasks", {}).get("RUNNING", 0) == vertex["parallelism"]
            for vertex in vertices
        ):
            return
        time.sleep(1)
    raise RuntimeError(f"Job {job_id} did not {goal} within {wait_timeout}s; inspect {REST_URL}/#/job/{job_id}/overview")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--bounded-timeout", type=int, default=3600)
    parser.add_argument("job_args", nargs=argparse.REMAINDER)
    options = parser.parse_args(argv)
    if options.timeout <= 0:
        parser.error("timeout must be positive")
    if options.bounded_timeout <= 0:
        parser.error("bounded-timeout must be positive")
    if options.job_args[:1] == ["--"]:
        options.job_args = options.job_args[1:]
    return options


def main():
    options = parse_args()
    job_args = options.job_args

    wait_for_cluster(options.timeout)
    active = [job for job in get_json("/jobs/overview")["jobs"]
              if job["name"] == JOB_NAME and job["state"] not in TERMINAL_STATES]
    if active:
        jobs = ", ".join(f"{job['jid']} ({job['state']})" for job in active)
        raise RuntimeError(f"A processor job is already active: {jobs}. Use make run-data-producer to feed it, or cancel it in the Flink UI before submitting another job.")

    subprocess.run([
        "docker", "compose", "cp", "bin/streaming-processor.jar",
        "jobmanager:/tmp/streaming-processor.jar",
    ], cwd=ROOT, check=True)
    submitted = subprocess.run([
        "docker", "compose", "exec", "-T", "jobmanager", "flink", "run", "-d",
        "/tmp/streaming-processor.jar", *job_args,
    ], cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
    print(submitted.stdout, end="", flush=True)
    match = re.search(r"JobID ([0-9a-f]{32})", submitted.stdout)
    if not match:
        raise RuntimeError("Flink did not return a job ID; check the submission output above")
    job_id = match.group(1)
    (ROOT / "bin/streaming-processor.job-id").write_text(job_id + "\n")
    bounded = "--bounded" in job_args
    wait_for_job(job_id, options.timeout,
                 bounded_timeout=options.bounded_timeout if bounded else None,
                 bounded=bounded)
    status = "Bounded run finished" if bounded else "Processor ready"
    print(f"{status}: {REST_URL}/#/job/{job_id}/overview", flush=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        if error.output:
            print(error.output, file=sys.stderr, end="")
        sys.exit(error.returncode)
    except (RuntimeError, urllib.error.URLError, TimeoutError,
            ConnectionError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
