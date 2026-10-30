import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "submit-flink-job.py"
spec = importlib.util.spec_from_file_location("submit_flink_job", SCRIPT)
submit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(submit)
JOB_ID = "a" * 32


class SubmissionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "bin").mkdir()

    def invoke(self, args, jobs=()):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, f"Job has been submitted with JobID {JOB_ID}\n")

        with patch.object(submit, "ROOT", self.root), \
             patch.object(submit, "wait_for_cluster") as cluster, \
             patch.object(submit, "get_json", return_value={"jobs": list(jobs)}), \
             patch.object(submit.subprocess, "run", side_effect=run), \
             patch.object(submit, "wait_for_job") as ready, \
             contextlib.redirect_stdout(io.StringIO()):
            submit.main(args)
            cluster.assert_called_once_with(120)
            ready.assert_called_once_with(JOB_ID, 120, bounded="--bounded" in args)
        self.assertEqual((self.root / "bin/streaming-processor.job-id").read_text(), JOB_ID + "\n")
        return calls[-1]

    def test_fresh_submission_keeps_existing_behavior(self):
        command = self.invoke(["--", "--starting-offsets", "earliest"])
        self.assertNotIn("--fromSavepoint", command)
        self.assertEqual(command[-3:], ["/tmp/streaming-processor.jar", "--starting-offsets", "earliest"])

    def test_restore_option_precedes_jar_and_is_one_argument(self):
        path = "file:///opt/flink/state/savepoints/a path/savepoint-123"
        command = self.invoke(["--restore-from", path, "--", "--bounded", "--sample-limit", "0"])
        position = command.index("--fromSavepoint")
        self.assertEqual(command[position:position + 3], ["--fromSavepoint", path, "/tmp/streaming-processor.jar"])
        self.assertEqual(command[-3:], ["--bounded", "--sample-limit", "0"])
        self.assertNotIn("--allowNonRestoredState", command)

    def test_blank_restore_path_fails_before_touching_cluster(self):
        with patch.object(submit, "wait_for_cluster") as cluster, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                submit.main(["--restore-from", " "])
            cluster.assert_not_called()

    def test_active_job_still_blocks_restore(self):
        jobs = [{"name": submit.JOB_NAME, "state": "RUNNING", "jid": JOB_ID}]
        with self.assertRaisesRegex(RuntimeError, "already active"):
            self.invoke(["--restore-from", "file:///checkpoint"], jobs)

    def test_submission_failure_is_not_retried_as_a_fresh_job(self):
        with patch.object(submit, "ROOT", self.root), \
             patch.object(submit, "wait_for_cluster"), \
             patch.object(submit, "get_json", return_value={"jobs": []}), \
             patch.object(submit.subprocess, "run", side_effect=[
                 subprocess.CompletedProcess([], 0),
                 subprocess.CalledProcessError(1, ["flink", "run"], output="Cannot restore state"),
             ]) as run, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError):
                submit.main(["--restore-from", "file:///missing/checkpoint"])
            self.assertEqual(run.call_count, 2)
            self.assertFalse((self.root / "bin/streaming-processor.job-id").exists())


if __name__ == "__main__":
    unittest.main()
