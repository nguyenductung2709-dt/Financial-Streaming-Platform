#!/usr/bin/env python3
"""Tests for containers.py: the producer container command and the CSV inputs behind it."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

import containers


class DirectoryTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def touch(self, name):
        path = self.dir / name
        path.write_text("x")
        return path


class CsvInputsTest(unittest.TestCase):
    def test_reads_every_csv_path_form_in_order(self):
        args = ["-csvPath=a.csv", "-rate=0", "--csvPath", "b.csv", "-csvPath=c"]
        self.assertEqual(containers.csv_inputs(args), ["a.csv", "b.csv", "c"])


class ResolveInputsTest(DirectoryTestCase):
    def test_lists_the_files_of_a_directory(self):
        self.touch("b.csv"), self.touch("a.csv")
        files = containers.resolve_inputs([f"-csvPath={self.dir}"])
        self.assertEqual([p.name for p in files], ["a.csv", "b.csv"])

    def test_rejects_a_dangling_symlink(self):
        self.touch("day08.csv")
        os.symlink("missing.csv", self.dir / "day11.csv")
        with self.assertRaisesRegex(RuntimeError, r"Cannot use the CSV input.*day11\.csv"):
            containers.resolve_inputs([f"-csvPath={self.dir}"])

    def test_rejects_a_missing_file(self):
        with self.assertRaisesRegex(RuntimeError, "Cannot use the CSV input"):
            containers.resolve_inputs([f"-csvPath={self.dir / 'nope.csv'}"])

    def test_rejects_an_alias_next_to_its_target(self):
        real = self.touch("debs-08.csv")
        os.symlink(real.name, self.dir / "day08.csv")
        with self.assertRaisesRegex(RuntimeError, "same file.*replayed twice"):
            containers.resolve_inputs([f"-csvPath={self.dir}"])

    def test_rejects_the_same_file_listed_twice(self):
        real = self.touch("a.csv")
        with self.assertRaisesRegex(RuntimeError, "same file"):
            containers.resolve_inputs([f"-csvPath={real}", f"-csvPath={real}"])

    def test_directory_ignores_other_entries(self):
        self.touch("b.csv"), self.touch("a.csv"), self.touch("notes.txt")
        (self.dir / "nested.csv").mkdir()
        (self.dir / "nested.csv" / "c.csv").write_text("x")
        files = containers.resolve_inputs([f"-csvPath={self.dir}"])
        self.assertEqual([p.name for p in files], ["a.csv", "b.csv"])

    def test_relative_paths_are_relative_to_the_base(self):
        self.touch("a.csv")
        files = containers.resolve_inputs(["-csvPath=a.csv"], base=self.dir)
        self.assertEqual([p.resolve() for p in files], [(self.dir / "a.csv").resolve()])

    def test_no_csv_path_resolves_the_producers_default(self):
        default = self.dir / containers.DEFAULT_CSV
        default.parent.mkdir(parents=True)
        default.write_text("x")
        files = containers.resolve_inputs(["-rate=0"], base=self.dir)
        self.assertEqual([p.resolve() for p in files], [default.resolve()])

    def test_missing_default_csv_is_an_error(self):
        with self.assertRaisesRegex(RuntimeError, "Cannot use the CSV input"):
            containers.resolve_inputs(["-rate=0"], base=self.dir)

    def test_help_needs_no_dataset(self):
        for flag in ("-h", "-help", "--help"):
            self.assertEqual(containers.resolve_inputs([flag], base=self.dir), [])


class ProducerCommandTest(DirectoryTestCase):
    def command(self, args, name="p1"):
        return containers.producer_command(args, name, base=self.dir)

    @staticmethod
    def after_service(command):
        return command[command.index("data-producer") + 1:]

    @staticmethod
    def mounts(command):
        return [command[i + 1] for i, part in enumerate(command[:command.index("data-producer")]) if part == "-v"]

    def test_command_begins_with_run_rm_name(self):
        self.touch("a.csv")
        command = self.command(["-csvPath=a.csv"])
        self.assertEqual(command[:7], ["docker", "compose", "run", "--rm", "-T", "--name", "p1"])

    def test_compose_prefix_is_configurable(self):
        self.touch("a.csv")
        prefix = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.monitoring.yaml"]
        command = containers.producer_command(["-csvPath=a.csv"], "p1", compose=prefix, base=self.dir)
        self.assertEqual(command[:len(prefix) + 2], [*prefix, "run", "--rm"])

    def test_command_mounts_files_read_only_in_order(self):
        a, b = self.touch("a.csv"), self.touch("b.csv")
        command = self.command(["-rate=0", f"-csvPath={b}", f"-csvPath={a}"])
        self.assertEqual(self.mounts(command), [f"{b.resolve()}:/input/00-b.csv:ro", f"{a.resolve()}:/input/01-a.csv:ro"])
        self.assertEqual(self.after_service(command), ["-rate=0", "-csvPath=/input/00-b.csv", "-csvPath=/input/01-a.csv"])

    def test_directory_expands_alphabetically(self):
        self.touch("b.csv"), self.touch("a.csv")
        command = self.command([f"-csvPath={self.dir}"])
        self.assertEqual(self.after_service(command), ["-csvPath=/input/00-a.csv", "-csvPath=/input/01-b.csv"])

    def test_symlink_is_mounted_by_its_target(self):
        real = self.touch("debs-08.csv")
        os.symlink(real.name, self.dir / "day08.csv")
        command = self.command([f"-csvPath={self.dir / 'day08.csv'}"])
        self.assertEqual(self.mounts(command), [f"{real.resolve()}:/input/00-day08.csv:ro"])

    def test_default_csv_is_supplied_when_none_given(self):
        default = self.dir / containers.DEFAULT_CSV
        default.parent.mkdir(parents=True)
        default.write_text("x")
        command = self.command(["-dlqEnabled"])
        self.assertEqual(self.after_service(command),
                         ["-dlqEnabled", "-csvPath=/input/00-debs2022-gc-trading-day-12-11-21.csv"])
        with self.assertRaisesRegex(RuntimeError, "Cannot use the CSV input"):
            containers.producer_command(["-dlqEnabled"], "p1", base=self.dir / "elsewhere")

    def test_space_form_and_double_dash_forms_are_rewritten(self):
        a, b = self.touch("a.csv"), self.touch("b.csv")
        command = self.command(["--csvPath", str(a), "-csvPath", str(b)])
        self.assertEqual(self.after_service(command), ["-csvPath=/input/00-a.csv", "-csvPath=/input/01-b.csv"])

    def test_other_flags_pass_through_unchanged(self):
        self.touch("a.csv")
        args = ["-dlqEnabled", "-maxEvents=3", "-brokers=x:1", "-csvPath=a.csv", "-rate=5"]
        self.assertEqual(self.after_service(self.command(args)),
                         ["-dlqEnabled", "-maxEvents=3", "-brokers=x:1", "-rate=5", "-csvPath=/input/00-a.csv"])

    def test_help_needs_no_dataset(self):
        command = self.command(["-help"])
        self.assertNotIn("-v", command)
        self.assertEqual(command[-2:], ["data-producer", "-help"])

    def test_a_loopback_broker_is_rejected_when_building_the_command(self):
        self.touch("a.csv")
        with self.assertRaisesRegex(RuntimeError, "container itself"):
            self.command(["-csvPath=a.csv", "-brokers=localhost:19092"])

    def test_colon_in_a_path_is_rejected(self):
        path = self.touch("a:b.csv")
        with self.assertRaisesRegex(RuntimeError, ":"):
            self.command([f"-csvPath={path}"])

    def test_spaces_in_a_path_are_kept_in_the_source_and_sanitized_in_the_target(self):
        path = self.touch("my file.csv")
        command = self.command([f"-csvPath={path}"])
        self.assertEqual(self.mounts(command), [f"{path.resolve()}:/input/00-my_file.csv:ro"])


class CheckBrokersTest(unittest.TestCase):
    def test_loopback_addresses_are_rejected(self):
        for value in ("localhost:19092", "LOCALHOST:1", "127.0.0.1:19092", "127.0.1.1:9092", "[::1]:9092",
                      "0.0.0.0:9092", "redpanda-0:9092,localhost:9092"):
            with self.assertRaisesRegex(RuntimeError, "container itself"):
                containers.check_brokers([f"-brokers={value}"])

    def test_every_flag_form_is_checked(self):
        for args in (["-brokers", "localhost:1"], ["--brokers=localhost:1"], ["--brokers", "localhost:1"]):
            with self.assertRaisesRegex(RuntimeError, "container itself"):
                containers.check_brokers(args)

    def test_other_addresses_are_accepted(self):
        for value in ("redpanda-0:9092", "broker.example.com:9092", "10.0.0.5:9092", "redpanda-0:9092,redpanda-1:9092"):
            containers.check_brokers([f"-brokers={value}"])
        containers.check_brokers(["-rate=5"])

    def test_help_skips_the_check(self):
        containers.check_brokers(["-help", "-brokers=localhost:1"])


class PreflightTest(DirectoryTestCase):
    def test_returns_the_files_when_everything_is_usable(self):
        path = self.touch("a.csv")
        self.assertEqual(containers.preflight([f"-csvPath={path}", "-brokers=redpanda-0:9092"], base=self.dir), [path])

    def test_rejects_a_loopback_broker_before_looking_at_files(self):
        path = self.touch("a.csv")
        with self.assertRaisesRegex(RuntimeError, "container itself"):
            containers.preflight([f"-csvPath={path}", "-brokers=localhost:19092"], base=self.dir)

    def test_rejects_a_missing_csv(self):
        with self.assertRaisesRegex(RuntimeError, "Cannot use the CSV input"):
            containers.preflight(["-csvPath=nope.csv"], base=self.dir)


class StopContainerTest(unittest.TestCase):
    def test_runs_docker_stop_and_ignores_failure(self):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 1)

        containers.stop_container("p1", run=run)
        self.assertEqual(calls, [["docker", "stop", "-t", "10", "p1"]])


class StopRunTest(unittest.TestCase):
    def test_a_failing_cleanup_is_reported_and_does_not_skip_the_rest(self):
        stopped = []

        class Broken:
            def stop(self):
                raise subprocess.CalledProcessError(1, "docker")

        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            containers.stop_run("p1", Broken(), stop_producer=stopped.append)
        self.assertEqual(stopped, ["p1"])
        self.assertIn("Cleanup failed", errors.getvalue())

    def test_both_containers_are_stopped_in_the_normal_case(self):
        stopped = []

        class Fine:
            def stop(self):
                stopped.append("materializer")

        containers.stop_run("p1", Fine(), stop_producer=stopped.append)
        self.assertEqual(stopped, ["p1", "materializer"])


def ps_json(state, exit_code=0):
    """One line of `docker compose ps -a --format json materializer`, as Compose 5.6 prints it."""
    return json.dumps({"Service": "materializer", "State": state, "ExitCode": exit_code,
                       "Name": "financial-streaming-platform-materializer-1"}) + "\n"


class FakeDocker:
    """`run` stand-in: records commands, answers `ps` from `ps` (a string, or a list consumed in order)."""

    def __init__(self, ps="", ignore_sigterm=False):
        self.calls = []
        self.ps = ps
        self.ignore_sigterm = ignore_sigterm

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        stdout = ""
        if "ps" in command:
            stdout = self.ps.pop(0) if isinstance(self.ps, list) and len(self.ps) > 1 else \
                (self.ps[0] if isinstance(self.ps, list) else self.ps)
        elif "SIGKILL" in command:
            self.ps = ps_json("exited", 137)
        elif "SIGTERM" in command and not self.ignore_sigterm:
            self.ps = ps_json("exited", 0)
        return subprocess.CompletedProcess(command, 0, stdout=stdout)

    def commands(self, verb):
        return [c for c in self.calls if verb in c]


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class MaterializerCommandTest(unittest.TestCase):
    def test_passes_arguments_to_the_service(self):
        self.assertEqual(containers.materializer_command("-init-db"),
                         ["docker", "compose", "run", "--rm", "-T", "materializer", "-init-db"])


class MaterializerContainerTest(unittest.TestCase):
    def handle(self, **fake):
        self.docker = FakeDocker(**fake)
        self.clock = FakeClock()
        return containers.MaterializerContainer(run=self.docker, clock=self.clock, sleep=self.clock.sleep)

    def test_poll_is_none_while_running(self):
        handle = self.handle(ps=ps_json("running"))
        self.assertIsNone(handle.poll())
        self.assertIsNone(handle.returncode)

    def test_poll_returns_the_exit_code_once_exited(self):
        handle = self.handle(ps=ps_json("exited", 7))
        self.assertEqual(handle.poll(), 7)
        self.assertEqual(handle.returncode, 7)

    def test_poll_parses_object_lines_and_array_output(self):
        for output in (ps_json("exited", 3), "[" + ps_json("exited", 3).strip() + "]\n",
                       ps_json("exited", 3) + ps_json("exited", 3)):
            self.assertEqual(self.handle(ps=output).poll(), 3, output)

    def test_no_container_polls_as_none(self):
        self.assertIsNone(self.handle(ps="").poll())

    def test_running_state_is_queried_at_most_once_per_second(self):
        handle = self.handle(ps=ps_json("running"))
        for now in (0.0, 0.2, 0.4):
            self.clock.now = now
            handle.poll()
        self.assertEqual(len(self.docker.commands("ps")), 1)
        self.clock.now = 1.1
        handle.poll()
        self.assertEqual(len(self.docker.commands("ps")), 2)

    def test_start_discards_a_stale_exit_status(self):
        handle = self.handle(ps=ps_json("exited", 7))
        self.assertEqual(handle.poll(), 7)
        self.docker.ps = ps_json("running")
        handle.start()
        self.assertEqual(self.docker.commands("up"), [["docker", "compose", "up", "-d", "materializer"]])
        self.assertIsNone(handle.returncode)
        self.assertIsNone(handle.poll())

    def test_terminate_and_kill_send_the_signals_without_waiting(self):
        handle = self.handle(ps=ps_json("running"), ignore_sigterm=True)
        handle.terminate()
        handle.kill()
        self.assertEqual(self.docker.commands("kill"), [
            ["docker", "compose", "kill", "-s", "SIGTERM", "materializer"],
            ["docker", "compose", "kill", "-s", "SIGKILL", "materializer"]])
        self.assertEqual(self.clock.now, 0.0)

    def test_wait_times_out_with_timeout_expired(self):
        handle = self.handle(ps=ps_json("running"))
        with self.assertRaises(subprocess.TimeoutExpired):
            handle.wait(timeout=2)
        self.assertGreaterEqual(self.clock.now, 2)

    def test_wait_returns_the_exit_code(self):
        handle = self.handle(ps=[ps_json("running"), ps_json("running"), ps_json("exited", 0)])
        self.assertEqual(handle.wait(timeout=5), 0)

    def test_stop_escalates_to_sigkill_when_sigterm_is_ignored(self):
        handle = self.handle(ps=ps_json("running"), ignore_sigterm=True)
        handle.stop(timeout=3)
        signals = [c[c.index("-s") + 1] for c in self.docker.commands("kill")]
        self.assertEqual(signals, ["SIGTERM", "SIGKILL"])
        self.assertEqual(handle.returncode, 137)

    def test_stop_is_a_no_op_after_exit(self):
        handle = self.handle(ps=ps_json("exited", 0))
        handle.stop()
        self.assertEqual(self.docker.commands("kill"), [])
        self.assertEqual(handle.returncode, 0)

    def test_a_container_that_vanishes_after_start_counts_as_exited(self):
        handle = self.handle(ps=ps_json("running"))
        handle.start()
        self.assertIsNone(handle.poll())
        self.docker.ps = ""
        self.clock.now = 5.0
        self.assertEqual(handle.poll(), containers.VANISHED)
        self.assertEqual(handle.wait(timeout=1), containers.VANISHED)

    def test_stop_is_a_no_op_without_a_container(self):
        handle = self.handle(ps="")
        handle.stop()
        self.assertEqual(self.docker.commands("kill"), [])
        self.assertEqual(self.clock.now, 0.0)

    def test_env_merges_over_the_process_environment_on_every_call(self):
        clock = FakeClock()
        seen = []

        def run(command, **kwargs):
            seen.append(kwargs.get("env"))
            return subprocess.CompletedProcess(command, 0, stdout=ps_json("running"))

        stamped = containers.MaterializerContainer(run=run, clock=clock, sleep=clock.sleep,
                                                   env={"MATERIALIZER_STAMP_TOPIC": "materialize-stamps"})
        stamped.start()
        stamped.poll()
        self.assertEqual(seen, [{**os.environ, "MATERIALIZER_STAMP_TOPIC": "materialize-stamps"}] * 2)

        containers.MaterializerContainer(run=run, clock=clock, sleep=clock.sleep).start()
        self.assertIsNone(seen[-1], "without env the process environment is inherited untouched")


if __name__ == "__main__":
    unittest.main()
