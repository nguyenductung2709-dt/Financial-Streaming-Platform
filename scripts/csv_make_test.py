#!/usr/bin/env python3
"""Exercise real Make recipes without starting services or publishing records."""

from pathlib import Path
import shlex
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(shutil.which("make"), "GNU Make is required for recipe checks")
class CsvMakeTest(unittest.TestCase):
    def commands(self, csv, *options):
        result = subprocess.run(
            ["make", "--no-print-directory", "-n", "validate-flow", f"CSV={csv}",
             "DB_CHECK=0", *options], cwd=ROOT, text=True, capture_output=True,
            check=True)
        return [shlex.split(line) for line in result.stdout.splitlines()
                if line.startswith(("python3 validation/generate_validation_data.py",
                                    "./bin/data-producer "))]

    def test_both_tools_receive_the_same_explicit_file_order(self):
        reference, producer = self.commands("z.csv a.csv")
        self.assertEqual(reference[2:6], ["--csv", "z.csv", "--csv", "a.csv"])
        self.assertEqual(producer[1:3], ["-csvPath=z.csv", "-csvPath=a.csv"])

    def test_directory_is_forwarded_to_both_native_readers(self):
        reference, producer = self.commands("dataset")
        self.assertEqual(reference[2:4], ["--csv", "dataset"])
        self.assertEqual(producer[1], "-csvPath=dataset")

    def test_max_events_skips_reference_and_keeps_all_producer_inputs(self):
        commands = self.commands("z.csv a.csv", "MAX_EVENTS=3")
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][1:3], ["-csvPath=z.csv", "-csvPath=a.csv"])
        self.assertIn("-maxEvents=3", commands[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
