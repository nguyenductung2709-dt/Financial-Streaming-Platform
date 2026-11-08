#!/usr/bin/env python3
"""Stop the supervised flow and Compose services while preserving data."""

import subprocess
import sys

from flow import stop_flow


if __name__ == "__main__":
    try:
        stop_flow()
    except (RuntimeError, subprocess.CalledProcessError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
