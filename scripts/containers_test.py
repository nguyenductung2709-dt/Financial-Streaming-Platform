#!/usr/bin/env python3
"""Tests for containers.py: the producer container command and the CSV inputs behind it."""

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

    def test_colon_in_a_path_is_rejected(self):
        path = self.touch("a:b.csv")
        with self.assertRaisesRegex(RuntimeError, ":"):
            self.command([f"-csvPath={path}"])

    def test_spaces_in_a_path_are_kept_in_the_source_and_sanitized_in_the_target(self):
        path = self.touch("my file.csv")
        command = self.command([f"-csvPath={path}"])
        self.assertEqual(self.mounts(command), [f"{path.resolve()}:/input/00-my_file.csv:ro"])


class StopContainerTest(unittest.TestCase):
    def test_runs_docker_stop_and_ignores_failure(self):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 1)

        containers.stop_container("p1", run=run)
        self.assertEqual(calls, [["docker", "stop", "-t", "10", "p1"]])


if __name__ == "__main__":
    unittest.main()
