"""Lifecycle checks use real child processes with disposable local service fixtures."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import flow

spec = importlib.util.spec_from_file_location("reset_flow", Path(__file__).with_name("reset-flow-data.py"))
reset_flow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reset_flow)

FIXTURE = """#!/usr/bin/env python3
# Fake docker and make: log every call; docker also fakes the producer client,
# the materializer container (up/ps/kill) and everything else as a no-op.
import json, os, sys, time
from pathlib import Path

name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['FLOW_TEST_EVENTS'], 'a') as output:
    output.write(json.dumps({'name': name, 'args': args, 'pid': os.getpid()}) + '\\n')
if name == 'backend-python':
    if args[0].endswith('migrate.py'):
        sys.exit(6 if os.environ.get('FLOW_TEST_FAIL_MIGRATION') else 0)
    if os.environ.get('FLOW_TEST_FAIL_BACKEND'):
        sys.exit(9)
    from http.server import BaseHTTPRequestHandler, HTTPServer
    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'ok', 'pid': os.getpid()}).encode())
        def log_message(self, *args):
            pass
    HTTPServer(('127.0.0.1', int(os.environ['PORT'])), Health).serve_forever()
if name != 'docker':
    sys.exit(0)

state_file = Path(os.environ['FLOW_TEST_STATE'])


def set_state(state, code=0):
    state_file.write_text(json.dumps({'state': state, 'code': code, 'since': time.time()}))


def container():
    if not state_file.exists():
        return None
    state = json.loads(state_file.read_text())
    if state['state'] == 'running' and os.environ.get('FLOW_TEST_FAIL_MATERIALIZER') and time.time() - state['since'] >= 0.2:
        return {'State': 'exited', 'ExitCode': 7}
    return {'State': state['state'], 'ExitCode': state['code']}


rest = args[1:] if args[:1] == ['compose'] else args
if rest[:1] == ['run'] and 'data-producer' in rest:
    if os.environ.get('FLOW_TEST_FAIL_PRODUCER'):
        sys.exit(3)
    if os.environ.get('FLOW_TEST_FINITE_PRODUCER'):
        sys.exit(0)
    while True:
        time.sleep(0.1)
elif rest[:3] == ['up', '-d', 'materializer']:
    set_state('running')
elif rest[:1] == ['ps']:
    info = container()
    if info:
        print(json.dumps({'Service': 'materializer', **info}))
elif rest[:1] == ['kill']:
    set_state('exited', 137 if 'SIGKILL' in rest else 0)
sys.exit(0)
"""


class FlowLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="flow-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        repo = Path(flow.__file__).resolve().parent.parent
        for name in ("scripts", "validation", "bin"):
            (self.root / name).mkdir()
        shutil.copyfile(repo / "scripts/flow.py", self.root / "scripts/flow.py")
        shutil.copyfile(repo / "scripts/containers.py", self.root / "scripts/containers.py")
        shutil.copyfile(repo / "scripts/stop-flow.py", self.root / "scripts/stop-flow.py")
        shutil.copyfile(repo / "scripts/reset-flow.py", self.root / "scripts/reset-flow.py")
        shutil.copyfile(repo / "validation/csv_inputs.py", self.root / "validation/csv_inputs.py")
        for name in ("docker", "make", "backend-python"):
            executable = self.root / "bin" / name
            executable.write_text(FIXTURE)
            executable.chmod(0o755)
        self.csv = self.root / "trades.csv"
        self.csv.write_text("x")
        self.events_file = self.root / "events.jsonl"
        self.state_file = self.root / "materializer.state"
        self.env = dict(os.environ, FLOW_TEST_EVENTS=str(self.events_file), FLOW_TEST_STATE=str(self.state_file))
        self.env["PATH"] = str(self.root / "bin") + os.pathsep + self.env["PATH"]
        self.env["BACKEND_PYTHON"] = str(self.root / "bin/backend-python")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.env["BACKEND_PORT"] = str(sock.getsockname()[1])

    def events(self):
        if not self.events_file.exists():
            return []
        return [json.loads(line) for line in self.events_file.read_text().splitlines()]

    def docker_calls(self):
        return [event["args"] for event in self.events() if event["name"] == "docker"]

    def producer_started(self):
        return any(args[:1] == ["compose"] and "run" in args and "data-producer" in args for args in self.docker_calls())

    def run_flow(self, *producer_args, **env):
        return subprocess.Popen([sys.executable, str(self.root / "scripts/flow.py"), "--", *producer_args],
                                env=self.env | env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def start(self, **env):
        process = self.run_flow(f"-csvPath={self.csv}", "-maxEvents=10", **env)
        self.addCleanup(self.stop, process)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.producer_started():
                return process
            if process.poll() is not None:
                if self.producer_started():
                    return process
                self.fail(process.communicate()[0])
            time.sleep(0.02)
        self.fail("flow did not start its producer")

    def stop(self, process):
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        process.communicate(timeout=5)

    def assert_children_stopped(self):
        for event in self.events():
            with self.assertRaises(ProcessLookupError):
                os.kill(event["pid"], 0)

    def container_state(self):
        return json.loads(self.state_file.read_text())["state"]

    def test_complete_start_and_signal_shutdown(self):
        process = self.start(FLOW_TEST_FINITE_PRODUCER="1")
        time.sleep(0.3)
        self.assertIsNone(process.poll(), "materializer must keep running after producer completion")
        events = self.events()
        calls = self.docker_calls()
        self.assertIn("postgres", calls[0])
        self.assertIn(["compose", "run", "--rm", "-T", "materializer", "-init-db"], calls)
        self.assertIn(["compose", "up", "-d", "materializer"], calls)
        self.assertTrue(any(event["name"] == "make" and event["args"] == ["run-streaming-processor"] for event in events))
        migrated = next(i for i, event in enumerate(events) if any(arg.endswith("migrate.py") for arg in event["args"]))
        backend = next(i for i, event in enumerate(events) if any(arg.endswith("main.py") for arg in event["args"]))
        producer = next(i for i, event in enumerate(events)
                        if event["name"] == "docker" and "data-producer" in event["args"] and "run" in event["args"])
        self.assertLess(migrated, backend)
        self.assertLess(backend, producer)
        self.assertIn(f"{self.csv.resolve()}:/input/00-trades.csv:ro", events[producer]["args"])
        self.assertEqual(events[producer]["args"][-2:], ["-maxEvents=10", "-csvPath=/input/00-trades.csv"])
        self.stop(process)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(self.container_state(), "exited")
        self.assert_children_stopped()

    def test_materializer_failure_stops_producer(self):
        failure = self.root / "fail-materializer"
        process = self.start(FLOW_TEST_FAIL_MATERIALIZER=str(failure))
        failure.touch()
        output, _ = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("Materializer exited with status 7", output)
        self.assert_children_stopped()

    def test_producer_failure_is_reported(self):
        process = self.start(FLOW_TEST_FAIL_PRODUCER="1")
        output, _ = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("Producer exited with status 3", output)

    def test_abnormal_stop_stops_the_producer_container_by_name(self):
        process = self.start(FLOW_TEST_FAIL_MATERIALIZER="1")
        process.communicate(timeout=5)
        calls = self.docker_calls()
        stop = ["stop", "-t", "10", f"data-producer-run-{process.pid}"]
        self.assertIn(stop, calls)
        producer = next(i for i, args in enumerate(calls) if "data-producer" in args and "run" in args)
        self.assertGreater(calls.index(stop), producer)
        self.assertIn(f"data-producer-run-{process.pid}", calls[producer])

    def test_a_loopback_broker_fails_before_anything_starts(self):
        process = self.run_flow(f"-csvPath={self.csv}", "-brokers=localhost:19092")
        output, _ = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("container itself", output)
        self.assertEqual(self.docker_calls(), [])

    def test_missing_csv_fails_before_anything_starts(self):
        process = self.run_flow(f"-csvPath={self.root / 'nope.csv'}")
        output, _ = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("Cannot use the CSV input", output)
        self.assertEqual(self.docker_calls(), [])

    def test_reset_stops_managed_processes_before_taking_lock(self):
        process = self.start()
        with patch.object(flow, "ROOT", self.root):
            with flow.flow_lock(stop_running=True):
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0)
                self.assert_children_stopped()
                another = subprocess.run([sys.executable, str(self.root / "scripts/flow.py")],
                                         env=self.env, text=True, capture_output=True, timeout=5)
                self.assertEqual(another.returncode, 1)
                self.assertIn("already running", another.stderr)

    def test_failed_migration_prevents_publication(self):
        process = subprocess.run([sys.executable, str(self.root / "scripts/flow.py"), "--", f"-csvPath={self.csv}"],
                                 env=self.env | {"FLOW_TEST_FAIL_MIGRATION": "1"},
                                 text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 1)
        self.assertTrue(any(event["name"] == "backend-python" for event in self.events()), process.stderr)
        self.assertFalse(self.producer_started())
        self.assert_children_stopped()

    def test_failed_backend_stops_materializer_before_publishing(self):
        process = subprocess.run([sys.executable, str(self.root / "scripts/flow.py"), "--", f"-csvPath={self.csv}"],
                                 env=self.env | {"FLOW_TEST_FAIL_BACKEND": "1"},
                                 text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 1)
        self.assertIn("Backend exited with status 9", process.stderr)
        self.assertFalse(self.producer_started())
        self.assert_children_stopped()

    def teardown_flow(self, reset):
        process = self.start()
        script = "reset-flow.py" if reset else "stop-flow.py"
        result = subprocess.run([sys.executable, str(self.root / "scripts" / script)], env=self.env,
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        process.communicate(timeout=5)
        self.assert_children_stopped()
        commands = [event["args"] for event in self.events() if event["name"] == "docker" and "down" in event["args"]]
        self.assertEqual(len(commands), 1)
        self.assertEqual("--volumes" in commands[0], reset)

    def test_stop_preserves_volumes_and_stops_backend(self):
        self.teardown_flow(reset=False)

    def test_reset_removes_volumes_after_stopping_processes(self):
        self.teardown_flow(reset=True)


class ResetSafetyTests(unittest.TestCase):
    def reset_and_record(self):
        """Run reset() against stand-ins and return every command it executed, in order."""
        calls = []

        def execute(command, **kwargs):
            calls.append(command)
            if "list" in command:
                stdout = json.dumps([{"group": "test-materializer", "state": "Empty"}])
            elif "delete" in command:
                stdout = json.dumps([{"group": "test-materializer", "status": "OK"}])
            elif "describe" in command:
                stdout = json.dumps([{"summary": {"name": topic}, "partitions": [
                    {"log_start_offset": 0, "high_watermark": 0}]} for topic in reset_flow.TOPICS])
            else:
                stdout = ""
            return subprocess.CompletedProcess(command, 0, stdout=stdout)

        def job(path):
            if path == "/jobs/overview":
                return {"jobs": [{"name": "market-price-event-consumer", "state": "RUNNING", "jid": "test-job"}]}
            return {"state": "CANCELED"}

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(reset_flow, "ROOT", Path(directory)):
                with patch.dict(os.environ, MATERIALIZER_GROUP_ID="test-materializer"):
                    with patch.object(reset_flow, "get_json", side_effect=job):
                        with patch.object(reset_flow.subprocess, "run", side_effect=execute):
                            reset_flow.reset()
        return calls

    def test_reset_cancels_flink_before_clearing_topics_and_database(self):
        calls = self.reset_and_record()
        cancel = next(i for i, command in enumerate(calls) if "cancel" in command)
        # Topics are recreated (rpk topic delete, then create), not trimmed in place.
        deletes = [i for i, command in enumerate(calls) if "topic" in command and "delete" in command]
        reset = next(i for i, command in enumerate(calls) if "-reset-db" in command)
        self.assertEqual(len(deletes), len(reset_flow.TOPICS))
        self.assertLess(cancel, min(deletes))
        self.assertLess(max(deletes), reset)
        self.assertIn(["docker", "compose", "run", "--rm", "-T", "materializer", "-reset-db"], calls)

    def test_reset_stops_the_materializer_container_before_the_group_check(self):
        calls = self.reset_and_record()
        stop = calls.index(["docker", "compose", "stop", "materializer"])
        group_list = next(i for i, command in enumerate(calls) if "rpk" in command and "group" in command and "list" in command)
        self.assertLess(stop, group_list)

    def test_active_external_materializer_prevents_data_clear(self):
        calls = []

        def execute(command, **kwargs):
            calls.append(command)
            stdout = json.dumps([{"group": "test-materializer", "state": "Stable"}]) if "list" in command else ""
            return subprocess.CompletedProcess(command, 0, stdout=stdout)

        with patch.dict(os.environ, MATERIALIZER_GROUP_ID="test-materializer"):
            with patch.object(reset_flow, "get_json", return_value={"jobs": []}):
                with patch.object(reset_flow.subprocess, "run", side_effect=execute):
                    with self.assertRaisesRegex(RuntimeError, "group is still active"):
                        reset_flow.reset()
        self.assertFalse(any(("topic" in command and "delete" in command) or "-reset-db" in command
                             for command in calls))


if __name__ == "__main__":
    unittest.main()
