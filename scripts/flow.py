#!/usr/bin/env python3
"""Run the complete local flow and supervise its producer and materializer."""

import contextlib
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent


@contextlib.contextmanager
def flow_lock(stop_running=False):
    """Serialize start/reset and let reset stop the process that owns this flow."""
    (ROOT / "bin").mkdir(exist_ok=True)
    with (ROOT / "bin/flow.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.seek(0)
            owner = lock.read().split()
            if not stop_running or len(owner) != 2 or owner[0] != "run":
                raise RuntimeError("A flow start or reset is already running; use make reset-flow to stop the flow")
            print("Stopping the managed producer and materializer...", flush=True)
            try:
                os.kill(int(owner[1]), signal.SIGTERM)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 30
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("The running flow did not stop; no data was cleared")
                    time.sleep(0.1)
        lock.seek(0)
        lock.truncate()
        lock.write(f"{'reset' if stop_running else 'run'} {os.getpid()}\n")
        lock.flush()
        try:
            yield
        finally:
            lock.seek(0)
            lock.truncate()


def main():
    producer_args = sys.argv[1:]
    if producer_args[:1] == ["--"]:
        producer_args = producer_args[1:]
    children = []

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)

    def start(command):
        child = subprocess.Popen(command, cwd=ROOT, start_new_session=True)
        children.append(child)
        return child

    def run(command):
        child = start(command)
        result = child.wait()
        children.remove(child)
        if result:
            raise subprocess.CalledProcessError(result, command)

    with flow_lock():
        try:
            run(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "120",
                 "postgres", "redpanda-0", "console", "jobmanager", "taskmanager"])
            run(["docker", "compose", "run", "--rm", "create-topic"])
            run([str(ROOT / "bin/materializer"), "-init-db"])
            run(["make", "run-streaming-processor"])
            materializer = start([str(ROOT / "bin/materializer")])
            producer = start([str(ROOT / "bin/data-producer"), *producer_args])
            print("Full flow running. Ctrl+C stops the producer and materializer; make reset-flow also cancels Flink and clears data.", flush=True)
            producer_finished = False
            while True:
                if materializer.poll() is not None:
                    raise RuntimeError(f"Materializer exited with status {materializer.returncode}")
                if producer.poll() is not None and not producer_finished:
                    if producer.returncode:
                        raise RuntimeError(f"Producer exited with status {producer.returncode}")
                    producer_finished = True
                    print("Producer finished; the materializer continues processing. Ctrl+C to stop.", flush=True)
                time.sleep(0.2)
        finally:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            active = [child for child in children if child.poll() is None]
            for child in active:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 10
            for child in active:
                try:
                    child.wait(timeout=max(0.1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Flow stopped. Infrastructure and the Flink job remain available; use make reset-flow before a fresh run.")
    except (RuntimeError, subprocess.CalledProcessError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
