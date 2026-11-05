#!/usr/bin/env python3
"""Ordered CSV discovery and streaming, mirroring the native Go producer."""

import csv
import os
from pathlib import Path
from typing import NamedTuple

REQUIRED_COLUMNS = ("ID", "SecType", "Last", "Trading time", "Trading date")


def resolve_csv_paths(inputs) -> list[Path]:
    """Expand directories in place into immediate .csv files sorted by name."""
    if isinstance(inputs, (str, os.PathLike)):
        inputs = [inputs]
    paths = []
    if not inputs:
        raise ValueError("at least one CSV path is required")
    for value in inputs:
        if not str(value).strip():
            raise ValueError("CSV path must not be empty")
        path = Path(value)
        # stat() exposes missing/inaccessible paths instead of treating them as
        # empty directories, and follows symlinks like os.Stat in the producer.
        path.stat()
        if path.is_file():
            paths.append(path)
        elif path.is_dir():
            files = []
            for candidate in sorted(path.iterdir(), key=lambda p: p.name):
                if candidate.name.endswith(".csv"):
                    candidate.stat()
                    if candidate.is_file():
                        files.append(candidate)
            if not files:
                raise ValueError(f"CSV directory {str(path)!r} contains no .csv files")
            paths.extend(files)
        else:
            raise ValueError(f"CSV path {str(path)!r} is not a regular file or directory")
    return paths


class CsvRow(NamedTuple):
    path: Path
    line: int             # local physical record-start line, for diagnostics
    source_row: int       # prior files' physical line spans + local line
    columns: dict[str, int]
    values: list[str]


class _PhysicalLines:
    """Track record starts (not ends) even for quoted multiline CSV fields."""

    def __init__(self, stream):
        self.stream = stream
        self.count = 0
        self.record_start = None

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.stream)
        self.count += 1
        if self.record_start is None and line not in ("\n", "\r\n"):
            self.record_start = self.count
        return line


def iter_csv_rows(paths, limit_rows=None, *, on_truncate=None):
    """Read each file with its own header and cumulative physical source rows.

    `paths` is the resolved file list. Closing this generator also closes the
    active file. A global limit stops before parsing another record/file.
    `on_truncate` is called if nonblank input or unexamined files remain.
    """
    base = 0
    emitted = 0
    for index, path in enumerate(paths):
        path = Path(path)
        with path.open(newline="", encoding="utf-8", errors="replace") as stream:
            lines = _PhysicalLines(stream)
            reader = csv.reader(lines, strict=True)
            columns = None
            max_required = -1
            try:
                while True:
                    if (limit_rows is not None and emitted >= limit_rows
                            and columns is not None and lines.count >= 13):
                        # Inspect only raw trailing lines of the active file;
                        # parsing one extra record could fail beyond the limit.
                        # Do not open later files just to decide truncation.
                        remaining = index + 1 < len(paths) or any(line.strip() for line in lines)
                        if remaining and on_truncate is not None:
                            on_truncate()
                        return
                    lines.record_start = None
                    try:
                        values = next(reader)
                    except StopIteration:
                        break
                    if not values:  # Match encoding/csv: skip empty physical lines.
                        continue
                    line = lines.record_start
                    if line < 12 or line == 13:
                        continue
                    if line == 12:
                        columns = {name.strip(): i for i, name in enumerate(values)}
                        missing = [name for name in REQUIRED_COLUMNS if name not in columns]
                        if missing:
                            raise ValueError(f"CSV header is missing columns {missing}")
                        max_required = max(columns[name] for name in REQUIRED_COLUMNS)
                        continue
                    if columns is None:
                        raise ValueError("CSV header is missing at line 12")
                    if len(values) <= max_required:
                        raise ValueError(f"CSV line {line} is missing required fields")
                    emitted += 1
                    yield CsvRow(path, line, base + line, columns, values)
                if columns is None:
                    raise ValueError("CSV header is missing at line 12")
            except (csv.Error, ValueError) as error:
                raise ValueError(f"CSV {str(path)!r}: {error}") from error
            base += lines.count
