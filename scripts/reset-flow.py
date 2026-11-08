#!/usr/bin/env python3
"""Stop the local flow and permanently delete its Compose data and checkpoints."""

import subprocess
import sys

from flow import stop_flow


if __name__ == "__main__":
    try:
        stop_flow(reset=True)
    except (RuntimeError, subprocess.CalledProcessError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
