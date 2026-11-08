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
import json, os, sys, time
from pathlib import Path
name = Path(sys.argv[0]).name
with open(os.environ['FLOW_TEST_EVENTS'], 'a') as output:
    output.write(json.dumps({'name': name, 'args': sys.argv[1:], 'pid': os.getpid()}) + '\\n')
if name == 'backend-python':
    if sys.argv[1].endswith('migrate.py'):
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
elif name == 'materializer' and not sys.argv[1:]:
    if os.environ.get('FLOW_TEST_FAIL_MATERIALIZER'):
        while not Path(os.environ['FLOW_TEST_FAIL_MATERIALIZER']).exists():
            time.sleep(0.01)
        sys.exit(7)
elif name == 'data-producer' and not os.environ.get('FLOW_TEST_FINITE_PRODUCER'):
    pass
else:
    sys.exit(0)
while True:
    time.sleep(0.1)
"""


class FlowLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="flow-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "scripts").mkdir()
        (self.root / "bin").mkdir()
        shutil.copyfile(Path(flow.__file__), self.root / "scripts/flow.py")
        shutil.copyfile(Path(flow.__file__).with_name("stop-flow.py"), self.root / "scripts/stop-flow.py")
        shutil.copyfile(Path(flow.__file__).with_name("reset-flow.py"), self.root / "scripts/reset-flow.py")
        for name in ("docker", "make", "materializer", "data-producer", "backend-python"):
            executable = self.root / "bin" / name
            executable.write_text(FIXTURE)
            executable.chmod(0o755)
        self.events_file = self.root / "events.jsonl"
        self.env = dict(os.environ, FLOW_TEST_EVENTS=str(self.events_file))
        self.env["PATH"] = str(self.root / "bin") + os.pathsep + self.env["PATH"]
        self.env["BACKEND_PYTHON"] = str(self.root / "bin/backend-python")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.env["BACKEND_PORT"] = str(sock.getsockname()[1])

    def events(self):
        if not self.events_file.exists():
            return []
        return [json.loads(line) for line in self.events_file.read_text().splitlines()]

    def start(self, **env):
        process = subprocess.Popen([sys.executable, str(self.root / "scripts/flow.py"), "--", "-maxEvents=10"],
                                   env=self.env | env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(self.stop, process)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if any(event["name"] == "data-producer" for event in self.events()):
                return process
            if process.poll() is not None:
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

    def test_complete_start_and_signal_shutdown(self):
        process = self.start(FLOW_TEST_FINITE_PRODUCER="1")
        time.sleep(0.3)
        self.assertIsNone(process.poll(), "materializer must keep running after producer completion")
        events = self.events()
        self.assertIn("postgres", events[0]["args"])
        self.assertTrue(any(event["name"] == "materializer" and event["args"] == ["-init-db"] for event in events))
        self.assertTrue(any(event["name"] == "make" and event["args"] == ["run-streaming-processor"] for event in events))
        migrated = next(i for i, event in enumerate(events) if any(arg.endswith("migrate.py") for arg in event["args"]))
        backend = next(i for i, event in enumerate(events) if any(arg.endswith("main.py") for arg in event["args"]))
        producer = next(i for i, event in enumerate(events) if event["name"] == "data-producer")
        self.assertLess(migrated, backend)
        self.assertLess(backend, producer)
        self.stop(process)
        self.assertEqual(process.returncode, 0)
        self.assert_children_stopped()

    def test_materializer_failure_stops_producer(self):
        failure = self.root / "fail-materializer"
        process = self.start(FLOW_TEST_FAIL_MATERIALIZER=str(failure))
        failure.touch()
        output, _ = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("Materializer exited with status 7", output)
        self.assert_children_stopped()

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
        process = subprocess.run([sys.executable, str(self.root / "scripts/flow.py")],
                                 env=self.env | {"FLOW_TEST_FAIL_MIGRATION": "1"},
                                 text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 1)
        self.assertFalse(any(event["name"] == "data-producer" for event in self.events()))
        self.assert_children_stopped()

    def test_failed_backend_stops_materializer_before_publishing(self):
        process = subprocess.run([sys.executable, str(self.root / "scripts/flow.py")],
                                 env=self.env | {"FLOW_TEST_FAIL_BACKEND": "1"},
                                 text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 1)
        self.assertIn("Backend exited with status 9", process.stderr)
        self.assertFalse(any(event["name"] == "data-producer" for event in self.events()))
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
    def test_reset_cancels_flink_before_clearing_topics_and_database(self):
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
        cancel = next(i for i, command in enumerate(calls) if "cancel" in command)
        # Topics are recreated (rpk topic delete, then create), not trimmed in place.
        deletes = [i for i, command in enumerate(calls) if "topic" in command and "delete" in command]
        reset = next(i for i, command in enumerate(calls) if "-reset-db" in command)
        self.assertEqual(len(deletes), 4)
        self.assertLess(cancel, min(deletes))
        self.assertLess(max(deletes), reset)

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
