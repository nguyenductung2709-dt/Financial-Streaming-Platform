#!/usr/bin/env python3
"""Run the complete local flow and supervise producer, materializer, and dashboard."""

import contextlib
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

import containers

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
                raise RuntimeError("A flow operation is already running; use make stop-flow to stop the managed flow")
            print("Stopping the managed producer, materializer, and backend...", flush=True)
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


def wait_for_backend(child, url, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        if child.poll() is not None:
            raise RuntimeError(f"Backend exited with status {child.returncode}")
        try:
            with urllib.request.urlopen(url + "/api/health", timeout=1) as response:
                health = json.load(response)
                if response.status == 200 and health.get("status") == "ok" and health.get("pid") == child.pid:
                    return
        except (urllib.error.URLError, TimeoutError, ValueError, AttributeError):
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("Backend did not become ready; check its migration and connection errors")
        time.sleep(0.1)


def stop_flow(reset=False):
    """Stop managed processes before removing containers and optional volumes."""
    with flow_lock(stop_running=True):
        command = ["docker", "compose", "down", "--remove-orphans"]
        if reset:
            command.append("--volumes")
        subprocess.run(command, cwd=ROOT, check=True)
        (ROOT / "bin/streaming-processor.job-id").unlink(missing_ok=True)
    print("Flow reset: local database, broker data, and Flink checkpoints deleted."
          if reset else "Flow stopped; local data and checkpoints preserved.")


def main():
    producer_args = sys.argv[1:]
    if producer_args[:1] == ["--"]:
        producer_args = producer_args[1:]
    children = []
    backend_python = os.environ.get("BACKEND_PYTHON", str(ROOT / ".venv/bin/python"))
    if not Path(backend_python).is_file():
        raise RuntimeError("Backend runtime missing; run make setup-backend first")
    host = os.environ.get("BACKEND_HOST", "127.0.0.1")
    port = int(os.environ.get("BACKEND_PORT", "8000"))
    if not 1 <= port <= 65535:
        raise ValueError("BACKEND_PORT must be between 1 and 65535")
    probe_host = "127.0.0.1" if host == "0.0.0.0" else "::1" if host == "::" else host
    if ":" in probe_host:
        probe_host = f"[{probe_host}]"
    dashboard_url = f"http://{probe_host}:{port}"

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)

    def start(command, env=None):
        child = subprocess.Popen(command, cwd=ROOT, env=env, start_new_session=True)
        children.append(child)
        return child

    def run(command):
        child = start(command)
        result = child.wait()
        children.remove(child)
        if result:
            raise subprocess.CalledProcessError(result, command)

    materializer = containers.MaterializerContainer()
    producer_name = f"data-producer-run-{os.getpid()}"

    with flow_lock():
        containers.resolve_inputs(producer_args)  # fail before anything starts or needs cleaning up
        try:
            run(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "120",
                 "postgres", "redpanda-0", "console", "jobmanager", "taskmanager"])
            run(["docker", "compose", "run", "--rm", "create-topic"])
            run(containers.materializer_command("-init-db"))
            run([backend_python, str(ROOT / "services/backend/migrate.py")])
            run(["make", "run-streaming-processor"])
            materializer.start()
            backend = start([backend_python, str(ROOT / "services/backend/main.py")],
                            env=dict(os.environ, HOST=host, PORT=str(port)))
            wait_for_backend(backend, dashboard_url)
            producer = start(containers.producer_command(producer_args, producer_name))
            print(f"Full flow running. Dashboard: {dashboard_url}. Ctrl+C stops the producer, materializer and dashboard; make stop-flow also stops containers.", flush=True)
            producer_finished = False
            while True:
                if materializer.poll() is not None:
                    raise RuntimeError(f"Materializer exited with status {materializer.returncode}")
                if backend.poll() is not None:
                    raise RuntimeError(f"Backend exited with status {backend.returncode}")
                if producer.poll() is not None and not producer_finished:
                    if producer.returncode:
                        raise RuntimeError(f"Producer exited with status {producer.returncode}")
                    producer_finished = True
                    print("Producer finished; the materializer and dashboard remain running. Ctrl+C to stop.", flush=True)
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
            # A killed `compose run` client leaves its container running, so stop it by name.
            containers.stop_run(producer_name, materializer)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Flow stopped. Infrastructure and the Flink job remain available; use make reset-flow before a fresh run.")
    except (RuntimeError, subprocess.CalledProcessError, OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
