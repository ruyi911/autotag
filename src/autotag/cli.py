from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoTag CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_daily = sub.add_parser("run-daily", help="run daily/replay/realtime pipeline")
    p_daily.add_argument("args", nargs=argparse.REMAINDER, help="forwarded to pipeline_runner")

    p_mobile_sync = sub.add_parser("mobile-sync", help="sync missing phones from user_login_*")
    p_mobile_sync.add_argument("--dt", default=None)
    p_mobile_sync.add_argument("--mode", choices=["daily", "realtime", "all"], default="daily")
    p_mobile_sync.add_argument("--include-masked", action="store_true")
    p_mobile_sync.add_argument("--limit", type=int, default=0)
    p_mobile_sync.add_argument("--dry-run", action="store_true")

    p_mobile_import = sub.add_parser("mobile-import-dir", help="import mobile data from a directory")
    p_mobile_import.add_argument("--dir", required=True, dest="directory")
    p_mobile_import.add_argument("--no-recursive", action="store_true")

    ns = parser.parse_args()
    if ns.cmd == "run-daily":
        forwarded = ns.args or []
        cmd = [sys.executable, "-m", "autotag.ops.pipeline_runner", *forwarded]
        raise SystemExit(subprocess.call(cmd))
    if ns.cmd == "mobile-sync":
        cmd = [sys.executable, "-m", "autotag.ingest.mobile_sync", "sync-missing", "--mode", ns.mode]
        if ns.dt:
            cmd.extend(["--dt", ns.dt])
        if ns.include_masked:
            cmd.append("--include-masked")
        if ns.limit > 0:
            cmd.extend(["--limit", str(ns.limit)])
        if ns.dry_run:
            cmd.append("--dry-run")
        raise SystemExit(subprocess.call(cmd))
    if ns.cmd == "mobile-import-dir":
        cmd = [sys.executable, "-m", "autotag.ingest.mobile_sync", "import-dir", "--dir", ns.directory]
        if ns.no_recursive:
            cmd.append("--no-recursive")
        raise SystemExit(subprocess.call(cmd))

    parser.print_help()


if __name__ == "__main__":
    main()
