#!/usr/bin/env python3
"""Run the data producer container in the foreground and exit with its status.

Every argument goes to the producer. CSV inputs may be any host paths: they are
mounted read-only into the container and the -csvPath flags rewritten to match
(see containers.py). Ctrl+C, SIGTERM and SIGHUP stop the container, which a killed
`compose run` client would otherwise leave publishing.
"""

import os
from pathlib import Path
import signal
import subprocess
import sys

import containers

ROOT = Path(__file__).resolve().parent.parent


def main(argv):
    name = f"data-producer-run-{os.getpid()}"
    try:
        command = containers.producer_command(argv, name)
    except RuntimeError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    # Ctrl+C already raises KeyboardInterrupt; a kill or a closed terminal must stop the container too.
    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGTERM, signal.SIGHUP)}
    try:
        return subprocess.run(command, cwd=ROOT).returncode
    except KeyboardInterrupt:
        return 130
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        containers.stop_container(name)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
