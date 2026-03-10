from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoTag CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_daily = sub.add_parser("run-daily", help="run daily/replay/realtime pipeline")
    p_daily.add_argument("args", nargs=argparse.REMAINDER, help="forwarded to pipeline_runner")

    ns = parser.parse_args()
    if ns.cmd == "run-daily":
        forwarded = ns.args or []
        cmd = [sys.executable, "-m", "autotag.ops.pipeline_runner", *forwarded]
        raise SystemExit(subprocess.call(cmd))

    parser.print_help()


if __name__ == "__main__":
    main()
