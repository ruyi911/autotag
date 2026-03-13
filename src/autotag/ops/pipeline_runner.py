from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import duckdb
from dotenv import find_dotenv, load_dotenv

from autotag.ops.run_history import finish_run, start_run
from autotag.utils.alert import send_alert
from autotag.utils.paths import get_metabase_db_path, get_serving_db_path
from autotag.utils.time import default_business_dt


@dataclass
class StepFailure(Exception):
    step: str
    code: int
    message: str = ""


class Logger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fp = self.path.open("a", encoding="utf-8")

    def close(self) -> None:
        self.fp.close()

    def event(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line, flush=True)
        self.fp.write(line + "\n")
        self.fp.flush()

    def stream_line(self, line: str) -> None:
        print(line, end="", flush=True)
        self.fp.write(line)
        self.fp.flush()


@contextmanager
def _acquire_lock(lock_file: Path, pid_file: Path):
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        import fcntl

        fp = lock_file.open("w")
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fp.close()
            raise RuntimeError("another run is active (flock)") from exc
        try:
            yield
        finally:
            try:
                fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            finally:
                fp.close()
        return
    except ImportError:
        pass

    if pid_file.exists():
        old_pid = pid_file.read_text(encoding="utf-8").strip()
        if old_pid and old_pid.isdigit():
            try:
                os.kill(int(old_pid), 0)
                raise RuntimeError(f"another run is active (pid={old_pid})")
            except ProcessLookupError:
                pass
    pid_file.write_text(str(os.getpid()), encoding="utf-8")
    try:
        yield
    finally:
        pid_file.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily pipeline orchestrator")
    parser.add_argument("dt", nargs="?", default=None)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--sources", default="")
    parser.add_argument("--mode", choices=["daily", "replay", "realtime"], default="daily")
    parser.add_argument("--no-publish", action="store_true")
    return parser.parse_args()


def _parse_non_fatal_steps() -> set[str]:
    raw = os.getenv("NON_FATAL_STEPS", "")
    if not raw.strip():
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _write_source_split_files(status_file: Path, ok_file: Path, fail_file: Path) -> None:
    if not status_file.exists():
        return
    payload = json.loads(status_file.read_text(encoding="utf-8"))
    ok_file.write_text(json.dumps(payload.get("source_success", []), ensure_ascii=False), encoding="utf-8")
    fail_file.write_text(json.dumps(payload.get("source_fail", {}), ensure_ascii=False), encoding="utf-8")


def _run_command(
    *,
    cmd: list[str],
    step: str,
    logger: Logger,
    non_fatal: set[str],
    extra_env: dict[str, str] | None = None,
) -> None:
    start_ts = datetime.now().timestamp()
    logger.event(f"start {step}")
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        logger.stream_line(line)
    rc = proc.wait()
    dur = int(datetime.now().timestamp() - start_ts)
    if rc != 0:
        if step in non_fatal:
            logger.event(f"warn {step} failed rc={rc}, tolerated by NON_FATAL_STEPS")
            return
        raise StepFailure(step=step, code=rc, message=f"command failed: {' '.join(cmd)}")
    logger.event(f"done {step} duration_s={dur}")


def _build_summary(dt: str, status_file: Path) -> str:
    payload = {}
    try:
        payload = json.loads(status_file.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

    ops_rows = -1
    max_dt = None
    user_login_updates = 0
    order_state_updates = 0
    try:
        con = duckdb.connect(str(get_serving_db_path()), read_only=True)
        ops_rows = con.execute('select count(*) from ops."用户状态总览"').fetchone()[0]
        max_dt = con.execute('select max("统计日期") from ops."用户状态总览"').fetchone()[0]
        user_login_updates = con.execute(
            """
            select count(*)
            from stg.stg_user
            where dt = ?::date and last_login_time is not null
            """,
            [dt],
        ).fetchone()[0]
        order_state_updates = con.execute(
            """
            select (
              select count(*) from (
                select order_id from stg.stg_recharge
                where dt = ?::date and order_id is not null and order_id <> ''
                group by 1 having count(distinct status_raw) > 1
              ) t
            ) + (
              select count(*) from (
                select withdraw_id from stg.stg_withdraw
                where dt = ?::date and withdraw_id is not null and withdraw_id <> ''
                group by 1 having count(distinct status_raw) > 1
              ) t
            )
            """,
            [dt, dt],
        ).fetchone()[0]
        con.close()
    except Exception:
        pass

    mb = get_metabase_db_path()
    mb_size = mb.stat().st_size if mb.exists() else 0
    variant_ok = len(payload.get("task_variant_success", []))
    variant_fail = len(payload.get("task_variant_fail", []))
    return (
        f"dt={dt};ops_rows={ops_rows};max_ops_dt={max_dt};metabase_size_bytes={mb_size};"
        f"variant_ok={variant_ok};variant_fail={variant_fail};"
        f"user_login_updates={user_login_updates};order_state_updates={order_state_updates}"
    )


def _runtime_config_json(
    *,
    dt: str,
    mode: str,
    skip_download: bool,
    no_publish: bool,
    sources: str,
    non_fatal: Iterable[str],
) -> str:
    config = {
        "dt": dt,
        "mode": mode,
        "skip_download": skip_download,
        "no_publish": no_publish,
        "sources": sources or "all",
        "non_fatal_steps": sorted(list(non_fatal)),
        "effective_env": {
            "ENABLE_REMOTE_FETCH": os.getenv("ENABLE_REMOTE_FETCH", ""),
            "ENABLE_LOGIN_FRESHNESS_GATE": os.getenv("ENABLE_LOGIN_FRESHNESS_GATE", ""),
            "ENABLE_STATUS_DRIFT_GATE": os.getenv("ENABLE_STATUS_DRIFT_GATE", ""),
            "ENABLE_MOBILE_SYNC": os.getenv("ENABLE_MOBILE_SYNC", ""),
            "STATUS_DRIFT_MIN_ORDERS": os.getenv("STATUS_DRIFT_MIN_ORDERS", ""),
            "ALERT_TELEGRAM_ENABLED": os.getenv("ALERT_TELEGRAM_ENABLED", ""),
            "ALERT_ON_SUCCESS": os.getenv("ALERT_ON_SUCCESS", ""),
        },
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    return json.dumps(config, ensure_ascii=False)


def main() -> None:
    load_dotenv(find_dotenv())
    args = parse_args()

    dt = args.dt or default_business_dt()
    run_id = uuid.uuid4().hex
    log_file = Path(f"logs/daily/dt={dt}.log")
    source_status_file = Path(f"logs/daily/source_status_{dt}_{run_id}.json")
    source_success_file = Path(f"logs/daily/source_success_{dt}_{run_id}.json")
    source_fail_file = Path(f"logs/daily/source_fail_{dt}_{run_id}.json")
    lock_file = Path(".locks/run_daily.lock")
    pid_file = Path(".locks/run_daily.pid")

    logger = Logger(log_file)
    python_exe = sys.executable
    non_fatal = _parse_non_fatal_steps()
    start_ts = int(datetime.now().timestamp())
    last_step = "init"

    def _status_payload() -> dict:
        if not source_status_file.exists():
            return {}
        return json.loads(source_status_file.read_text(encoding="utf-8"))

    def _finish_failed(step: str, code: int, message: str) -> None:
        _write_source_split_files(source_status_file, source_success_file, source_fail_file)
        status_payload = _status_payload()
        runtime_cfg = _runtime_config_json(
            dt=dt,
            mode=args.mode,
            skip_download=args.skip_download,
            no_publish=args.no_publish,
            sources=args.sources,
            non_fatal=non_fatal,
        )
        finish_run(
            run_id=run_id,
            status="FAILED",
            failed_step=step,
            message=f"exit_code={code} {message}",
            source_success_json=source_success_file.read_text(encoding="utf-8") if source_success_file.exists() else None,
            source_fail_json=source_fail_file.read_text(encoding="utf-8") if source_fail_file.exists() else None,
            task_variant_success_json=json.dumps(status_payload.get("task_variant_success", []), ensure_ascii=False),
            task_variant_fail_json=json.dumps(status_payload.get("task_variant_fail", []), ensure_ascii=False),
            runtime_config_json=runtime_cfg,
            window_start=str(status_payload.get("window_start", "")),
            window_end=str(status_payload.get("window_end", "")),
        )
        logger.event(f"FAILED dt={dt} run_id={run_id} step={step} code={code}")
        sent = send_alert(
            f"[AutoTag] FAILED dt={dt} step={step}",
            f"run_id={run_id}\nstep={step}\ncode={code}\nlog={log_file}\ndt={dt}",
        )
        logger.event(f"failure alert sent={int(bool(sent))}")

    try:
        with _acquire_lock(lock_file=lock_file, pid_file=pid_file):
            start_run(dt=dt, mode=args.mode, run_id=run_id)
            logger.event(
                f"run_daily dt={dt} run_id={run_id} mode={args.mode} "
                f"skip_download={int(args.skip_download)} no_publish={int(args.no_publish)} "
                f"sources={args.sources or 'all'}"
            )

            if not args.skip_download:
                last_step = "ingest.downloader"
                downloader_cmd = [
                    python_exe,
                    "-m",
                    "autotag.ingest.downloader",
                    "--dt",
                    dt,
                    "--mode",
                    args.mode,
                    "--fetch",
                    "--status-out",
                    str(source_status_file),
                ]
                if args.sources:
                    downloader_cmd.extend(["--sources", args.sources])
                _run_command(cmd=downloader_cmd, step=last_step, logger=logger, non_fatal=non_fatal)
                _write_source_split_files(source_status_file, source_success_file, source_fail_file)
            else:
                logger.event("skip ingest.downloader by --skip-download")
                source_success_file.write_text("[]", encoding="utf-8")
                source_fail_file.write_text("{}", encoding="utf-8")
                source_status_file.write_text(
                    json.dumps(
                        {
                            "mode": args.mode,
                            "task_variant_success": [],
                            "task_variant_fail": [],
                            "window_start": "",
                            "window_end": "",
                            "source_success": [],
                            "source_fail": {},
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                os.environ["ENABLE_LOGIN_FRESHNESS_GATE"] = "0"
                os.environ["ENABLE_STATUS_DRIFT_GATE"] = "0"

            steps: list[tuple[str, list[str]]] = [
                ("load.raw_import", [python_exe, "-m", "autotag.load.raw_import", "--dt", dt]),
            ]
            if os.getenv("ENABLE_MOBILE_SYNC", "1") == "1":
                steps.append(
                    (
                        "ingest.mobile_sync",
                        [
                            python_exe,
                            "-m",
                            "autotag.ingest.mobile_sync",
                            "sync-missing",
                            "--dt",
                            dt,
                            "--mode",
                            "daily" if args.mode == "daily" else ("realtime" if args.mode == "realtime" else "all"),
                        ],
                    )
                )
            else:
                logger.event("skip ingest.mobile_sync by ENABLE_MOBILE_SYNC=0")

            steps.extend(
                [
                    ("load.normalize", [python_exe, "-m", "autotag.load.normalize", "--dt", dt]),
                    ("load.build_mart", [python_exe, "-m", "autotag.load.build_mart", "--dt", dt]),
                    ("model.features", [python_exe, "-m", "autotag.model.features", "--dt", dt]),
                    ("model.labeling", [python_exe, "-m", "autotag.model.labeling", "--dt", dt]),
                    ("model.views_ops", [python_exe, "-m", "autotag.model.views_ops", "--dt", dt]),
                    ("model.snapshot_daily", [python_exe, "-m", "autotag.model.snapshot_daily", "--dt", dt]),
                ]
            )
            for step, cmd in steps:
                last_step = step
                _run_command(
                    cmd=cmd,
                    step=step,
                    logger=logger,
                    non_fatal=non_fatal,
                )

            if not args.no_publish:
                last_step = "publish.validate"
                validate_cmd = [python_exe, "-m", "autotag.publish.validate", "--dt", dt]
                if args.sources:
                    validate_cmd.extend(["--sources", args.sources])
                _run_command(
                    cmd=validate_cmd,
                    step=last_step,
                    logger=logger,
                    non_fatal=non_fatal,
                )
                last_step = "pytest.publish_gating"
                _run_command(
                    cmd=[python_exe, "-m", "pytest", "tests/test_publish_gating.py", "-q"],
                    step=last_step,
                    logger=logger,
                    non_fatal=non_fatal,
                )
                last_step = "publish.snapshot"
                _run_command(
                    cmd=[python_exe, "-m", "autotag.publish.snapshot", "--dt", dt],
                    step=last_step,
                    logger=logger,
                    non_fatal=non_fatal,
                )

            status_payload = _status_payload()
            runtime_cfg = _runtime_config_json(
                dt=dt,
                mode=args.mode,
                skip_download=args.skip_download,
                no_publish=args.no_publish,
                sources=args.sources,
                non_fatal=non_fatal,
            )
            finish_run(
                run_id=run_id,
                status="SUCCESS",
                failed_step=None,
                message=f"ok mode={args.mode} no_publish={int(args.no_publish)}",
                source_success_json=source_success_file.read_text(encoding="utf-8"),
                source_fail_json=source_fail_file.read_text(encoding="utf-8"),
                task_variant_success_json=json.dumps(status_payload.get("task_variant_success", []), ensure_ascii=False),
                task_variant_fail_json=json.dumps(status_payload.get("task_variant_fail", []), ensure_ascii=False),
                runtime_config_json=runtime_cfg,
                window_start=str(status_payload.get("window_start", "")),
                window_end=str(status_payload.get("window_end", "")),
            )

            total_dur = int(datetime.now().timestamp()) - start_ts
            summary = _build_summary(dt, source_status_file)
            logger.event(f"completed dt={dt} run_id={run_id} total_duration_s={total_dur} {summary}")

            if os.getenv("ALERT_ON_SUCCESS", "1") == "1":
                sent = send_alert(
                    f"[AutoTag] SUCCESS dt={dt}",
                    f"run_id={run_id}\nmode={args.mode}\nno_publish={int(args.no_publish)}\nduration_s={total_dur}\n{summary}\nlog={log_file}",
                )
                logger.event(f"success alert sent={int(bool(sent))}")
    except RuntimeError as exc:
        if "another run is active" in str(exc):
            logger.event(str(exc))
            raise SystemExit(2)
        _finish_failed(last_step, 1, str(exc))
        raise SystemExit(1)
    except StepFailure as exc:
        _finish_failed(exc.step, exc.code, exc.message)
        raise SystemExit(exc.code)
    except Exception as exc:
        _finish_failed(last_step, 1, repr(exc))
        raise SystemExit(1)
    finally:
        logger.close()


if __name__ == "__main__":
    main()
