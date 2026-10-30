#!/usr/bin/env python3
"""Regenerate the Python protobuf bindings in gen/python.

Same schemas as the Go/Java bindings (Makefile target generate-protobuf)
but via the grpcio-tools wheel's bundled protoc, since the local system
has no protoc.  grpc_tools emits *_pb2.py files whose cross-file imports
look like `from market.v1 import ema_pb2`, so the package directories
need __init__.py files that grpc_tools does not create.  The generated
bindings are committed with the schemas, like gen/go and gen/java.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GEN_DIR = REPO_ROOT / "gen" / "python"
PROTO_FILES = sorted((REPO_ROOT / "schemas" / "market" / "v1").glob("*.proto"))


def main():
    (GEN_DIR / "market" / "v1").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [sys.executable, "-m", "grpc_tools.protoc", "-I", "schemas",
         "--python_out=gen/python",
         *[str(p.relative_to(REPO_ROOT)) for p in PROTO_FILES]],
        check=True, cwd=REPO_ROOT)
    # The generated cross-imports (`from market.v1 import ...`) need these;
    # grpc_tools does not emit them.
    (GEN_DIR / "market" / "__init__.py").touch()
    (GEN_DIR / "market" / "v1" / "__init__.py").touch()
    outputs = [GEN_DIR / "market" / "v1" / f"{stem}_pb2.py"
               for stem in ("events", "ema", "advisory", "dlq")]
    missing = [p for p in outputs if not p.is_file()]
    assert not missing, f"missing generated bindings: {missing}"
    for p in outputs:
        print(f"Generated {p.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())