"""Commands for running the producer and materializer containers, shared by the flow scripts."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "validation"))
from csv_inputs import resolve_csv_paths

COMPOSE = ["docker", "compose"]
# The producer's built-in default; it is a path relative to the host, so the container needs it spelled out.
DEFAULT_CSV = "dataset/debs2022-gc-trading-day-12-11-21.csv"
HELP_FLAGS = ("-h", "-help", "--help")
CSV_FLAGS = ("-csvPath", "--csvPath")
BROKER_FLAGS = ("-brokers", "--brokers")
# Inside the container these name the container itself, not the machine Redpanda is published on.
LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "0.0.0.0")
INPUT_DIR = "/input"
UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")
# Exit status reported for a materializer container that was started and then disappeared.
VANISHED = -1


def split_flag(producer_args, flags):
    """(the values of the given flag in order, the other arguments in order).

    Accepts -flag=x, -flag x and the double-dash forms. A flag with no value
    stays among the other arguments so the producer reports it itself.
    """
    values, rest = [], []
    skip = False
    for index, arg in enumerate(producer_args):
        if skip:
            skip = False
            continue
        name, separator, value = arg.partition("=")
        if name not in flags:
            rest.append(arg)
        elif separator:
            values.append(value)
        elif index + 1 < len(producer_args):
            values.append(producer_args[index + 1])
            skip = True
        else:
            rest.append(arg)
    return values, rest


def split_csv_args(producer_args):
    """(the -csvPath values in order, the other arguments in order)."""
    return split_flag(producer_args, CSV_FLAGS)


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


def check_brokers(producer_args):
    """Reject a -brokers address that is loopback: in the container it is the container itself.

    The container already defaults to redpanda-0:9092, so the old host-side
    `-brokers=localhost:19092` would only fail later with a connection timeout.
    """
    if any(arg in HELP_FLAGS for arg in producer_args):
        return
    for value in split_flag(producer_args, BROKER_FLAGS)[0]:
        for address in value.split(","):
            address = address.strip()
            host = address[1:address.index("]")] if address.startswith("[") and "]" in address else address.rsplit(":", 1)[0]
            if host.lower() in LOOPBACK_HOSTS or host.startswith("127."):
                raise RuntimeError(
                    f"-brokers {address} points at the producer container itself: inside the container localhost is not "
                    "your machine. Drop -brokers (the container's default is redpanda-0:9092) or use a Compose service address.")


def preflight(producer_args, base=ROOT):
    """Check what can be checked about the producer arguments before anything starts.

    The broker addresses and the CSV inputs; returns the CSV files. (A path that
    cannot be mounted is only found when the command is built.)
    """
    check_brokers(producer_args)
    return resolve_inputs(producer_args, base)


def producer_command(producer_args, name, compose=COMPOSE, base=ROOT):
    """`docker compose run` for the producer container with every CSV input mounted read-only.

    Each file is mounted at /input/<NN>-<name> and the -csvPath arguments are
    rewritten to those paths in the original order. The container is named so
    it can be stopped later: a killed `compose run` client leaves it running.
    """
    prefix = [*compose, "run", "--rm", "-T", "--name", name]
    files = preflight(producer_args, base)
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


def stop_run(producer_name, materializer, stop_producer=stop_container):
    """Stop what a flow started: the producer container by name, then the materializer.

    A cleanup that fails is reported on stderr and does not skip the other step
    or hide the error that ended the run.
    """
    for cleanup in (lambda: stop_producer(producer_name), materializer.stop):
        try:
            cleanup()
        except (subprocess.SubprocessError, OSError) as error:
            print(f"Cleanup failed: {error}", file=sys.stderr)


def materializer_command(*args, compose=COMPOSE):
    """`docker compose run` for a one-off materializer task such as -init-db or -reset-db."""
    return [*compose, "run", "--rm", "-T", "materializer", *args]


def parse_ps(output):
    """The container objects in `docker compose ps --format json` output.

    Compose prints one JSON object per line (older versions print an array);
    nothing is printed when the service has no container.
    """
    output = output.strip()
    if not output:
        return []
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return [json.loads(line) for line in output.splitlines() if line.strip()]
    return parsed if isinstance(parsed, list) else [parsed]


class MaterializerContainer:
    """The materializer service's container with the Popen methods the flow scripts use.

    poll() is None while the container runs (or before it was started) and the
    exit code once it has exited; a started container that disappears reports
    VANISHED, so wait() cannot hang on it. While it runs, poll() asks Docker at
    most once a second, so a script can poll in a tight loop.
    """

    POLL_INTERVAL_S = 1.0
    WAIT_INTERVAL_S = 0.5

    def __init__(self, compose=COMPOSE, run=subprocess.run, clock=time.monotonic, sleep=time.sleep, cwd=ROOT):
        self._compose = list(compose)
        self._run = run
        self._clock = clock
        self._sleep = sleep
        self._cwd = cwd
        self.returncode = None
        self._checked_at = None
        self._started = False

    def _docker(self, *args, capture=True, check=False):
        return self._run([*self._compose, *args], cwd=self._cwd, text=True, capture_output=capture, check=check)

    def _container(self):
        """The service's container as `compose ps` reports it, or None when there is none."""
        found = parse_ps(self._docker("ps", "-a", "--format", "json", "materializer", check=True).stdout)
        return found[0] if found else None

    def _poll(self, force):
        if self.returncode is not None:
            return self.returncode
        now = self._clock()
        if not force and self._checked_at is not None and now - self._checked_at < self.POLL_INTERVAL_S:
            return None
        self._checked_at = now
        container = self._container()
        if container is not None and container.get("State") in ("exited", "dead"):
            self.returncode = int(container.get("ExitCode", 0))
        elif container is None and self._started:
            self.returncode = VANISHED
        return self.returncode

    def start(self):
        """Start (or restart) the container in the background, forgetting any earlier exit status."""
        self._docker("up", "-d", "materializer", capture=False, check=True)
        self.returncode = None
        self._checked_at = None
        self._started = True

    def poll(self):
        return self._poll(force=False)

    def terminate(self):
        self._docker("kill", "-s", "SIGTERM", "materializer")

    def kill(self):
        self._docker("kill", "-s", "SIGKILL", "materializer")

    def wait(self, timeout=None):
        """The exit code once the container has exited; raises subprocess.TimeoutExpired after `timeout` seconds."""
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            code = self._poll(force=True)
            if code is not None:
                return code
            if deadline is not None and self._clock() >= deadline:
                raise subprocess.TimeoutExpired([*self._compose, "materializer"], timeout)
            self._sleep(self.WAIT_INTERVAL_S)

    def stop(self, timeout=10):
        """SIGTERM, then SIGKILL after `timeout` seconds; nothing to do when it is not running."""
        if self.returncode is not None:
            return
        container = self._container()
        if container is None:
            return
        if container.get("State") in ("exited", "dead"):
            self.returncode = int(container.get("ExitCode", 0))
            return
        self.terminate()
        try:
            self.wait(timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            self.wait()
