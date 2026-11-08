"""Commands for running the producer and materializer containers, shared by the flow scripts."""

import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
from csv_inputs import resolve_csv_paths

COMPOSE = ["docker", "compose"]
# The producer's built-in default; it is a path relative to the host, so the container needs it spelled out.
DEFAULT_CSV = "dataset/debs2022-gc-trading-day-12-11-21.csv"
HELP_FLAGS = ("-h", "-help", "--help")
CSV_FLAGS = ("-csvPath", "--csvPath")
INPUT_DIR = "/input"
UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")


def split_csv_args(producer_args):
    """(the -csvPath values in order, the other arguments in order).

    Accepts -csvPath=x, -csvPath x and the double-dash forms. A -csvPath with no
    value stays among the other arguments so the producer reports it itself.
    """
    values, rest = [], []
    skip = False
    for index, arg in enumerate(producer_args):
        if skip:
            skip = False
            continue
        name, separator, value = arg.partition("=")
        if name not in CSV_FLAGS:
            rest.append(arg)
        elif separator:
            values.append(value)
        elif index + 1 < len(producer_args):
            values.append(producer_args[index + 1])
            skip = True
        else:
            rest.append(arg)
    return values, rest


def csv_inputs(producer_args):
    """The -csvPath values among the producer arguments, in order."""
    return split_csv_args(producer_args)[0]


def resolve_inputs(producer_args, base=ROOT):
    """The CSV files the producer will read, in order; relative paths are relative to `base`.

    Uses the producer's default CSV when no -csvPath is given, and nothing for a
    help flag. Rejects a missing path, an empty directory and one file listed
    twice (for example an alias symlink next to the file it points to), which
    the producer would replay twice.
    """
    if any(arg in HELP_FLAGS for arg in producer_args):
        return []
    inputs = csv_inputs(producer_args) or [DEFAULT_CSV]
    paths = [value if not value.strip() or os.path.isabs(value) else str(Path(base) / value) for value in inputs]
    try:
        files = resolve_csv_paths(paths)
    except (ValueError, OSError) as error:
        raise RuntimeError(f"Cannot use the CSV input: {error}")
    seen = {}
    for path in files:
        real = path.resolve()
        if real in seen:
            raise RuntimeError(f"{seen[real]} and {path} are the same file ({real}) and would be replayed twice; "
                               "list each file once, e.g. CSV=\"dataset/day08.csv dataset/day09.csv\".")
        seen[real] = path
    return files


def producer_command(producer_args, name, compose=COMPOSE, base=ROOT):
    """`docker compose run` for the producer container with every CSV input mounted read-only.

    Each file is mounted at /input/<NN>-<name> and the -csvPath arguments are
    rewritten to those paths in the original order. The container is named so
    it can be stopped later: a killed `compose run` client leaves it running.
    """
    prefix = [*compose, "run", "--rm", "-T", "--name", name]
    files = resolve_inputs(producer_args, base)
    if not files:
        return [*prefix, "data-producer", *producer_args]
    mounts, targets = [], []
    for index, path in enumerate(files):
        real = path.resolve()
        if ":" in str(real):
            raise RuntimeError(f"Cannot mount {real}: a ':' in the path breaks Docker's volume syntax; rename or move the file")
        target = f"{INPUT_DIR}/{index:02d}-{UNSAFE_NAME.sub('_', path.name)}"
        mounts += ["-v", f"{real}:{target}:ro"]
        targets.append(f"-csvPath={target}")
    return [*prefix, *mounts, "data-producer", *split_csv_args(producer_args)[1], *targets]


def stop_container(name, timeout=10, run=subprocess.run):
    """Stop a container by name; a container that is already gone is not an error."""
    run(["docker", "stop", "-t", str(timeout), name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
