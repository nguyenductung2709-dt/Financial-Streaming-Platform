#!/usr/bin/env python3
"""Native CSV discovery and per-file reader contract shared with the Go producer."""

import tempfile
import unittest
from pathlib import Path

from csv_inputs import iter_csv_rows, resolve_csv_paths

HEADER = "ID,SecType,Last,Trading time,Trading date"


class CsvInputsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def file(self, name, data="A,E,1,09:00:00.000,12-11-2021\n", header=HEADER):
        path = self.root / name
        path.write_text("# metadata\n" * 11 + header + "\n# descriptions\n" + data)
        return path

    def test_mixed_files_and_directories_expand_at_argument_position(self):
        first = self.file("z.csv")
        directory = self.root / "days"
        directory.mkdir()
        (directory / "b.csv").write_text("")
        (directory / "a.csv").write_text("")
        (directory / "ignore.txt").write_text("")
        (directory / "nested.csv").mkdir()
        (directory / "nested.csv" / "other.csv").write_text("")
        last = self.file("with spaces.csv")
        self.assertEqual(resolve_csv_paths([first, directory, last]),
                         [first, directory / "a.csv", directory / "b.csv", last])

    def test_empty_missing_and_unsupported_inputs_fail(self):
        empty = self.root / "empty"
        empty.mkdir()
        for inputs in ([], "", [" "], [empty], [self.root / "missing.csv"]):
            with self.subTest(inputs=inputs), self.assertRaises((ValueError, OSError)):
                resolve_csv_paths(inputs)

    def test_dot_csv_filename_is_included_like_the_go_reader(self):
        path = self.file(".csv")
        self.assertEqual(resolve_csv_paths(self.root), [path])

    def test_headers_and_cumulative_physical_source_rows(self):
        first = self.file("first.csv", '"ABC\nINC",E,1,09:00:00.000,12-11-2021\n\n')
        second = self.file("second.csv", "2,A,12-11-2021,09:00:00.000,E\n",
                           "Last,ID,Trading date,Trading time,SecType")
        rows = list(iter_csv_rows([first, second]))
        self.assertEqual([r.source_row for r in rows], [14, 30])
        self.assertEqual([r.line for r in rows], [14, 14])
        self.assertEqual([r.values[r.columns["Last"]] for r in rows], ["1", "2"])
        self.assertEqual([r.path for r in rows], [first, second])

    def test_final_line_without_newline_counts_in_next_file_offset(self):
        first = self.file("first.csv", "A,E,1,09:00:00.000,12-11-2021")
        second = self.file("second.csv")
        self.assertEqual([r.source_row for r in iter_csv_rows([first, second])], [14, 28])

    def test_bad_later_header_and_missing_header_name_file(self):
        first = self.file("first.csv")
        bad = self.file("bad.csv", header="ID,SecType")
        empty = self.root / "empty.csv"
        empty.write_text("")
        for path in (bad, empty):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, path.name):
                list(iter_csv_rows([first, path]))

    def test_short_row_names_file_and_local_line(self):
        bad = self.file("bad.csv", "A,E\n")
        with self.assertRaisesRegex(ValueError, r"bad.csv.*14"):
            list(iter_csv_rows([bad]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
