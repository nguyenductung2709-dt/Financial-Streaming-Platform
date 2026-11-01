"""Lifecycle checks use real child processes with disposable local service fixtures."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import flow

spec = importlib.util.spec_from_file_location("reset_flow", Path(__file__).with_name("reset-flow.py"))
reset_flow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reset_flow)

FIXTURE = """#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
name = Path(sys.argv[0]).name
with open(os.environ['FLOW_TEST_EVENTS'], 'a') as output:
    output.write(json.dumps({'name': name, 'args': sys.argv[1:], 'pid': os.getpid()}) + '\\n')
if name == 'materializer' and not sys.argv[1:]:
    if os.environ.get('FLOW_TEST_FAIL_MATERIALIZER'):
        time.sleep(0.2)
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
        for name in ("docker", "make", "materializer", "data-producer"):
            executable = self.root / "bin" / name
            executable.write_text(FIXTURE)
            executable.chmod(0o755)
        self.events_file = self.root / "events.jsonl"
        self.env = dict(os.environ, FLOW_TEST_EVENTS=str(self.events_file))
        self.env["PATH"] = str(self.root / "bin") + os.pathsep + self.env["PATH"]

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
        self.stop(process)
        self.assertEqual(process.returncode, 0)
        self.assert_children_stopped()

    def test_materializer_failure_stops_producer(self):
        process = self.start(FLOW_TEST_FAIL_MATERIALIZER="1")
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
                    {"log_start_offset": 7, "high_watermark": 7}]} for topic in reset_flow.TOPICS])
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
        trims = [i for i, command in enumerate(calls) if "trim-prefix" in command]
        reset = next(i for i, command in enumerate(calls) if "-reset-db" in command)
        self.assertEqual(len(trims), 4)
        self.assertLess(cancel, min(trims))
        self.assertLess(max(trims), reset)

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
        self.assertFalse(any("trim-prefix" in command or "-reset-db" in command for command in calls))


if __name__ == "__main__":
    unittest.main()
