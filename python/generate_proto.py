#!/usr/bin/env python3
"""Generate gRPC stubs from ../proto/queue.proto into cangling_broker/proto."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PROTO_DIR = ROOT.parent / "proto"
OUT_DIR = ROOT / "cangling_broker" / "proto"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").write_text("# Generated gRPC stubs live in this package.\n")
    proto = PROTO_DIR / "queue.proto"
    if not proto.is_file():
        print(f"missing {proto}", file=sys.stderr)
        return 1
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
            str(proto),
        ]
    )
    grpc_file = OUT_DIR / "queue_pb2_grpc.py"
    text = grpc_file.read_text()
    text = text.replace("import queue_pb2 as", "from . import queue_pb2 as")
    grpc_file.write_text(text)
    print(f"wrote {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
