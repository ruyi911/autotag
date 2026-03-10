from __future__ import annotations

import argparse
import csv
import json
import os
import random
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
import yaml
from dotenv import find_dotenv, load_dotenv

from autotag.ingest.discover import discover_files
from autotag.ingest.manifest import ManifestItem, count_csv_rows, sha256_file, write_manifest
from autotag.ingest.token_cache import TokenCache
from autotag.utils.paths import get_config_path, get_dropbox_dir, get_raw_files_dir
from autotag.utils.time import INDIA_TZ, iter_dates, parse_date

load_dotenv(find_dotenv())

API_LOGIN = "/user/login"
API_TASK_LIST = "/exportTask/list"
API_EXPORT_USER = "/userManage/exportUsers"
API_EXPORT_WITHDRAW = "/withdrawOrder/exportOrder"
API_EXPORT_CHARGE = "/chargeOrder/exportOrder"
API_EXPORT_BONUS = "/statTable/exportUserBonus"
API_EXPORT_BET = "/statBg/statExportBg"

POLL_MAX_RETRIES = int(os.getenv("POLL_MAX_RETRIES", "30"))
POLL_MAX_RETRIES_LARGE = int(os.getenv("POLL_MAX_RETRIES_LARGE", "400"))
POLL_INTERVAL_SEC_LARGE = int(os.getenv("POLL_INTERVAL_SEC_LARGE", "10"))
HTTP_RETRY_MAX = int(os.getenv("HTTP_RETRY_MAX", "4"))
HTTP_RETRY_BASE_SEC = float(os.getenv("HTTP_RETRY_BASE_SEC", "1.0"))

CORE_SOURCES_DEFAULT = {"user", "bet", "recharge", "withdraw"}
OPTIONAL_SOURCES_DEFAULT = {"bonus"}


@dataclass
class TaskVariant:
    source: str
    variant: str
    task_name: str
    path: str
    window_start: str
    window_end: str
    payload: dict[str, Any]
    mode: str = "daily"


def _load_sources() -> list[str]:
    with open(get_config_path("pipelines.yaml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["pipelines"]["daily"]["sources"]


def _policy_sets() -> tuple[set[str], set[str]]:
    core = {s.strip() for s in os.getenv("CORE_SOURCES", ",".join(sorted(CORE_SOURCES_DEFAULT))).split(",") if s.strip()}
    optional = {s.strip() for s in os.getenv("OPTIONAL_SOURCES", ",".join(sorted(OPTIONAL_SOURCES_DEFAULT))).split(",") if s.strip()}
    return core, optional


def _known_sources() -> set[str]:
    return {"user", "recharge", "withdraw", "bet", "bonus"}


def _validate_policy_and_sources(sources: list[str]) -> None:
    core, optional = _policy_sets()
    known = _known_sources()
    unknown = [s for s in sources if s not in known]
    if unknown:
        raise RuntimeError(f"unknown sources: {unknown}, known={sorted(known)}")
    if not core:
        raise RuntimeError("CORE_SOURCES cannot be empty")
    overlap = core.intersection(optional)
    if overlap:
        raise RuntimeError(f"CORE_SOURCES and OPTIONAL_SOURCES overlap: {sorted(overlap)}")


def _request_with_retry(method: str, url: str, **kwargs):
    last_exc: Exception | None = None
    for i in range(HTTP_RETRY_MAX):
        try:
            resp = requests.request(method=method, url=url, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if i == HTTP_RETRY_MAX - 1:
                break
            sleep_s = (HTTP_RETRY_BASE_SEC * (2**i)) + random.uniform(0, 0.3)
            time.sleep(sleep_s)
    raise RuntimeError(f"http request failed after retries: {method} {url}: {last_exc}") from last_exc


def _copy_with_no_overwrite(src: Path, target_dir: Path, file_hash: str) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / f"{src.stem}_{file_hash[:8]}{src.suffix.lower()}"
    if not dst.exists():
        shutil.copy2(src, dst)
    return dst


def _select_latest_per_source(discovered: dict[str, list[Path]]) -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {}
    for source, files in discovered.items():
        if not files:
            result[source] = []
            continue
        latest = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        result[source] = [latest]
    return result


def _day_window(d: date) -> tuple[str, str]:
    return f"{d.strftime('%Y-%m-%d')} 00:00:00", f"{d.strftime('%Y-%m-%d')} 23:59:59"


def _range_window(start: date, end: date) -> tuple[str, str]:
    return f"{start.strftime('%Y-%m-%d')} 00:00:00", f"{end.strftime('%Y-%m-%d')} 23:59:59"


def _user_payload_reg(window_start: str, window_end: str) -> dict[str, Any]:
    return {
        "page": 1,
        "size": 10,
        "vip_lvl": "",
        "bingMobile": "",
        "channel_source": "",
        "regTime": [window_start, window_end],
    }


def _user_payload_login(window_start: str, window_end: str) -> dict[str, Any]:
    return {
        "page": 1,
        "size": 10,
        "vip_lvl": "",
        "bingMobile": "",
        "channel_source": "",
        "loginTime": [window_start, window_end],
    }


def _recharge_payload(window_start: str, window_end: str) -> dict[str, Any]:
    return {"condition": {"channel_source": "", "pay_mode": [], "create_time": [window_start, window_end]}}


def _withdraw_payload(window_start: str, window_end: str) -> dict[str, Any]:
    return {"condition": {"status": "", "channel_source": [], "create_time": [window_start, window_end]}}


def _bonus_payload(window_start: str, window_end: str) -> dict[str, Any]:
    return {
        "sortKey": "bonus_amount",
        "sortOrder": "descending",
        "page": 1,
        "size": 10,
        "createTime": [window_start, window_end],
        "uid": "",
        "type": "",
        "channel": [],
    }


def _bet_payload(window_start: str, window_end: str) -> dict[str, Any]:
    return {
        "page": 1,
        "size": 10,
        "column": "winlose",
        "direction": "asc",
        "game_name": "",
        "channel_source": "",
        "getDate": [window_start, window_end],
    }


def _task_variants_for_dt(
    dt: str,
    sources: list[str],
    mode: str,
    use_day_window: bool = False,
    user_range_start: str | None = None,
    user_range_end: str | None = None,
) -> list[TaskVariant]:
    d = parse_date(dt)
    is_sunday = d.weekday() == 6
    user_full_days = int(os.getenv("USER_FULL_LOOKBACK_DAYS", "3650"))
    order_full_days = int(os.getenv("ORDER_FULL_LOOKBACK_DAYS", "30"))

    variants: list[TaskVariant] = []
    include_weekly_full = mode == "daily" and is_sunday and os.getenv("ENABLE_WEEKLY_FULL_VARIANTS", "0") == "1"

    if mode == "realtime":
        now = datetime.now(INDIA_TZ).replace(microsecond=0)
        minutes = int(os.getenv("REALTIME_WINDOW_MINUTES", "30"))
        rt_start = now - timedelta(minutes=minutes)
        rt_start_s = rt_start.strftime("%Y-%m-%d %H:%M:%S")
        rt_end_s = now.strftime("%Y-%m-%d %H:%M:%S")
    else:
        rt_start_s = ""
        rt_end_s = ""

    if "user" in sources:
        if mode == "realtime":
            reg_start, reg_end = rt_start_s, rt_end_s
            login_start, login_end = rt_start_s, rt_end_s
        elif user_range_start and user_range_end:
            # 用户全量回溯模式：指定时间范围，只导出注册用户，不需要登录信息
            reg_start, reg_end = _range_window(parse_date(user_range_start), parse_date(user_range_end))
            login_start, login_end = _range_window(parse_date(user_range_start), parse_date(user_range_end))
        else:
            reg_start, reg_end = _day_window(d)
            login_start, login_end = _day_window(d)
        variants.append(
            TaskVariant(
                "user",
                "user_reg_backfill" if (user_range_start and user_range_end) else ("user_reg_daily" if mode != "realtime" else "user_reg_realtime"),
                "用户导出",
                API_EXPORT_USER,
                reg_start,
                reg_end,
                _user_payload_reg(reg_start, reg_end),
                mode=mode,
            )
        )
        
        # 用户全量回溯模式只导出注册信息，不需要登录用户数据
        # 回放模式和全量回溯模式都不需要登录用户信息
        if mode != "replay" and not (user_range_start and user_range_end):
            variants.append(
                TaskVariant(
                    "user",
                    "user_login_daily" if mode != "realtime" else "user_login_realtime",
                    "用户导出",
                    API_EXPORT_USER,
                    login_start,
                    login_end,
                    _user_payload_login(login_start, login_end),
                    mode=mode,
                )
            )

        if include_weekly_full:
            full_start, full_end = _range_window(d - timedelta(days=user_full_days - 1), d)
            variants.append(
                TaskVariant(
                    "user",
                    "user_full_weekly",
                    "用户导出",
                    API_EXPORT_USER,
                    full_start,
                    full_end,
                    _user_payload_reg(full_start, full_end),
                    mode=mode,
                )
            )

    if "recharge" in sources:
        if mode == "realtime":
            win_start, win_end = rt_start_s, rt_end_s
            variant_name = "recharge_realtime"
        elif use_day_window:
            # 回放模式：只拉取当天数据
            win_start, win_end = _day_window(d)
            variant_name = "recharge_daily"
        else:
            # 日常模式：拉取3天窗口数据
            win_start, win_end = _range_window(d - timedelta(days=2), d)
            variant_name = "recharge_window_3d"
        variants.append(
            TaskVariant(
                "recharge",
                variant_name,
                "充值订单导出",
                API_EXPORT_CHARGE,
                win_start,
                win_end,
                _recharge_payload(win_start, win_end),
                mode=mode,
            )
        )
        if include_weekly_full:
            full_start, full_end = _range_window(d - timedelta(days=order_full_days - 1), d)
            variants.append(
                TaskVariant(
                    "recharge",
                    "recharge_full_weekly",
                    "充值订单导出",
                    API_EXPORT_CHARGE,
                    full_start,
                    full_end,
                    _recharge_payload(full_start, full_end),
                    mode=mode,
                )
            )

    if "withdraw" in sources:
        if mode == "realtime":
            win_start, win_end = rt_start_s, rt_end_s
            variant_name = "withdraw_realtime"
        elif use_day_window:
            # 回放模式：只拉取当天数据
            win_start, win_end = _day_window(d)
            variant_name = "withdraw_daily"
        else:
            # 日常模式：拉取3天窗口数据
            win_start, win_end = _range_window(d - timedelta(days=2), d)
            variant_name = "withdraw_window_3d"
        variants.append(
            TaskVariant(
                "withdraw",
                variant_name,
                "提现订单导出",
                API_EXPORT_WITHDRAW,
                win_start,
                win_end,
                _withdraw_payload(win_start, win_end),
                mode=mode,
            )
        )
        if include_weekly_full:
            full_start, full_end = _range_window(d - timedelta(days=order_full_days - 1), d)
            variants.append(
                TaskVariant(
                    "withdraw",
                    "withdraw_full_weekly",
                    "提现订单导出",
                    API_EXPORT_WITHDRAW,
                    full_start,
                    full_end,
                    _withdraw_payload(full_start, full_end),
                    mode=mode,
                )
            )

    if "bet" in sources:
        if mode == "realtime":
            bs, be = rt_start_s, rt_end_s
            variant_name = "bet_realtime"
        else:
            bs, be = _day_window(d)
            variant_name = "bet_daily"
        variants.append(TaskVariant("bet", variant_name, "投注统计导出", API_EXPORT_BET, bs, be, _bet_payload(bs, be), mode=mode))

    if "bonus" in sources:
        if mode == "realtime":
            bs, be = rt_start_s, rt_end_s
            variant_name = "bonus_realtime"
        else:
            bs, be = _day_window(d)
            variant_name = "bonus_daily"
        variants.append(
            TaskVariant("bonus", variant_name, "用户彩金数据导出", API_EXPORT_BONUS, bs, be, _bonus_payload(bs, be), mode=mode)
        )

    return variants


def _ensure_bet_date_column(path: Path, dt: str) -> None:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return
    header = [c.strip() for c in rows[0]]
    if header and header[0] == "日期":
        return
    date_text = f"{parse_date(dt).year}/{parse_date(dt).month}/{parse_date(dt).day}"
    rows[0] = ["日期"] + rows[0]
    for i in range(1, len(rows)):
        rows[i] = [date_text] + rows[i]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerows(rows)


def _remote_enabled(args: argparse.Namespace) -> bool:
    if args.fetch:
        return True
    return os.getenv("ENABLE_REMOTE_FETCH", "0") == "1"


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"missing env: {key}")
    return value


def _login_with_retry(base_url: str, username: str, password: str, totp_secret: str, max_retries: int = 3) -> str:
    """执行登录并重试。

    Args:
        base_url: API基础URL
        username: 用户名
        password: 密码
        totp_secret: TOTP密钥
        max_retries: 最大重试次数

    Returns:
        access_token

    Raises:
        RuntimeError: 所有重试都失败
    """
    try:
        import pyotp
    except Exception as exc:
        raise RuntimeError("login requires pyotp") from exc

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            totp = pyotp.TOTP(totp_secret).now()
            login_resp = _request_with_retry(
                "POST",
                f"{base_url}{API_LOGIN}",
                json={"username": username, "password": password, "googleVcode": totp},
                timeout=30,
            )
            payload = login_resp.json()
            if payload.get("code") != 0:
                raise RuntimeError(f"login api failed: code={payload.get('code')}, msg={payload.get('message', 'unknown')}")

            token = payload["data"]["access_token"]
            print(f"[ingest] login success at attempt {attempt}/{max_retries}", flush=True)
            return token
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                print(f"[ingest] login attempt {attempt}/{max_retries} failed: {exc}, retrying...", flush=True)
                time.sleep(2 ** attempt)  # 指数退避：2s, 4s, 8s
            else:
                print(f"[ingest] login failed after {max_retries} attempts", flush=True)

    raise RuntimeError(f"login failed after {max_retries} retries: {last_exc}") from last_exc


def _list_task_download_urls(base_url: str, headers: dict[str, str], task_type: str) -> set[str]:
    task_url = f"{base_url}{API_TASK_LIST}"
    payload = {"page": 1, "size": 50}
    resp = _request_with_retry("POST", task_url, headers=headers, json=payload, timeout=30)
    body = resp.json()
    urls: set[str] = set()
    if body.get("code") != 0:
        return urls
    for item in body.get("data", {}).get("list", []):
        if item.get("type") == task_type and item.get("download"):
            urls.add(item["download"])
    return urls


def _parse_window(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _build_payload(source: str, window_start: str, window_end: str, login_mode: bool = False) -> dict[str, Any]:
    if source == "user":
        if login_mode:
            return _user_payload_login(window_start, window_end)
        return _user_payload_reg(window_start, window_end)
    if source == "recharge":
        return _recharge_payload(window_start, window_end)
    if source == "withdraw":
        return _withdraw_payload(window_start, window_end)
    if source == "bet":
        return _bet_payload(window_start, window_end)
    if source == "bonus":
        return _bonus_payload(window_start, window_end)
    raise RuntimeError(f"unknown source payload: {source}")


def _split_variant(var: TaskVariant) -> list[TaskVariant]:
    start_dt = _parse_window(var.window_start)
    end_dt = _parse_window(var.window_end)
    mid_dt = start_dt + (end_dt - start_dt) / 2
    left_end = (mid_dt - timedelta(seconds=1)).replace(microsecond=0)
    right_start = mid_dt.replace(microsecond=0)

    left_ws, left_we = start_dt.strftime("%Y-%m-%d %H:%M:%S"), left_end.strftime("%Y-%m-%d %H:%M:%S")
    right_ws, right_we = right_start.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")
    login_mode = "login" in var.variant
    return [
        replace(
            var,
            variant=f"{var.variant}__partA",
            window_start=left_ws,
            window_end=left_we,
            payload=_build_payload(var.source, left_ws, left_we, login_mode=login_mode),
        ),
        replace(
            var,
            variant=f"{var.variant}__partB",
            window_start=right_ws,
            window_end=right_we,
            payload=_build_payload(var.source, right_ws, right_we, login_mode=login_mode),
        ),
    ]


def _can_split(var: TaskVariant) -> bool:
    if var.source not in {"recharge", "withdraw"}:
        return False
    if var.mode not in {"replay", "realtime"}:
        return False
    if os.getenv("EXPORT_SPLIT_ENABLED", "1") != "1":
        return False
    min_minutes = int(os.getenv("EXPORT_SPLIT_MINUTES", "60"))
    start_dt = _parse_window(var.window_start)
    end_dt = _parse_window(var.window_end)
    return (end_dt - start_dt).total_seconds() / 60 >= (min_minutes * 2)


def _run_remote_variant(
    *,
    base_url: str,
    headers: dict[str, str],
    var: TaskVariant,
    dt: str,
    dropbox: Path,
) -> Path:
    old_urls = _list_task_download_urls(base_url=base_url, headers=headers, task_type=var.task_name)
    _request_with_retry("POST", f"{base_url}{var.path}", headers=headers, json=var.payload, timeout=80)

    task_url = f"{base_url}{API_TASK_LIST}"
    task_payload = {"page": 1, "size": 20}
    download_url = None
    # 使用动态轮询次数和间隔：全量/回溯/周期性任务需要更多时间
    is_large_task = any(kw in var.variant for kw in ["full", "backfill", "weekly", "window_3d"])
    max_retries = POLL_MAX_RETRIES_LARGE if is_large_task else POLL_MAX_RETRIES
    poll_interval = POLL_INTERVAL_SEC_LARGE if is_large_task else POLL_INTERVAL_SEC
    for i in range(max_retries):
        list_resp = _request_with_retry("POST", task_url, headers=headers, json=task_payload, timeout=1200)
        body = list_resp.json()
        found_processing = False
        for item in body.get("data", {}).get("list", []):
            item_url = item.get("download")
            if item.get("type") == var.task_name and item.get("status") == "正在处理":
                found_processing = True
            if (
                item.get("type") == var.task_name
                and item.get("status") == "处理成功"
                and item_url
                and item_url not in old_urls
            ):
                download_url = item_url
                break
        if download_url:
            break
        status = "processing" if found_processing else "waiting_new_result"
        print(f"[ingest] poll variant={var.variant} try={i+1}/{max_retries} status={status}", flush=True)
        time.sleep(poll_interval)

    if not download_url:
        raise RuntimeError(f"remote task not ready: {var.variant}")

    filename = os.path.basename(urlparse(download_url).path)
    target = dropbox / filename
    file_resp = _request_with_retry("GET", download_url, stream=True, timeout=120)
    with open(target, "wb") as f:
        for chunk in file_resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    if var.source == "bet":
        _ensure_bet_date_column(target, dt=dt)
    return target


def _remote_fetch(
    dt: str,
    sources: list[str],
    mode: str,
    use_day_window: bool = False,
    user_range_start: str | None = None,
    user_range_end: str | None = None,
) -> tuple[dict[str, list[Path]], dict[str, Any]]:
    base_url = _require_env("BASE_URL")
    username = _require_env("API_USERNAME")
    password = _require_env("API_PASSWORD")
    totp_secret = _require_env("TOTP_SECRET")
    _validate_policy_and_sources(sources)

    # 尝试使用缓存的token
    token_cache = TokenCache()
    token = token_cache.get_or_refresh()

    # 如果缓存中没有有效token，执行登录
    if not token:
        print(f"[ingest] no valid cached token, performing login", flush=True)
        token = _login_with_retry(
            base_url=base_url,
            username=username,
            password=password,
            totp_secret=totp_secret,
            max_retries=3,
        )
        # 保存token到缓存（48小时有效期）
        token_cache.save_token(token, ttl_hours=48)
    else:
        print(f"[ingest] using cached token, skipping login", flush=True)

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print(f"[ingest] remote fetch started, dt={dt}, sources={sources}", flush=True)

    dropbox = get_dropbox_dir()
    dropbox.mkdir(parents=True, exist_ok=True)

    variants = _task_variants_for_dt(
        dt=dt,
        sources=sources,
        mode=mode,
        use_day_window=use_day_window,
        user_range_start=user_range_start,
        user_range_end=user_range_end,
    )
    core_sources, optional_sources = _policy_sets()

    downloaded: dict[str, list[Path]] = {s: [] for s in sources}
    variant_success: list[dict[str, str]] = []
    variant_fail: list[dict[str, str]] = []

    max_depth = int(os.getenv("EXPORT_SPLIT_MAX_DEPTH", "4"))

    def run_variant_recursive(var: TaskVariant, depth: int = 0) -> bool:
        print(f"[ingest] submit variant={var.variant} source={var.source} window=[{var.window_start},{var.window_end}]", flush=True)
        try:
            target = _run_remote_variant(base_url=base_url, headers=headers, var=var, dt=dt, dropbox=dropbox)
            downloaded[var.source].append(target)
            variant_success.append(
                {
                    "variant": var.variant,
                    "source": var.source,
                    "window_start": var.window_start,
                    "window_end": var.window_end,
                    "filename": target.name,
                }
            )
            return True
        except Exception as exc:
            if _can_split(var) and depth < max_depth:
                print(f"[ingest] split-retry variant={var.variant} depth={depth+1} cause={exc}", flush=True)
                children = _split_variant(var)
                ok = True
                for child in children:
                    if not run_variant_recursive(child, depth + 1):
                        ok = False
                return ok

            if mode == "realtime" and os.getenv("REALTIME_FALLBACK_TO_DAY", "1") == "1":
                day_start, day_end = _day_window(parse_date(dt))
                if var.window_start != day_start or var.window_end != day_end:
                    login_mode = "login" in var.variant
                    fallback = replace(
                        var,
                        variant=f"{var.variant}__fallback_day",
                        window_start=day_start,
                        window_end=day_end,
                        payload=_build_payload(var.source, day_start, day_end, login_mode=login_mode),
                    )
                    print(f"[ingest] realtime fallback to day window variant={fallback.variant}", flush=True)
                    return run_variant_recursive(fallback, depth + 1)

            variant_fail.append(
                {
                    "variant": var.variant,
                    "source": var.source,
                    "window_start": var.window_start,
                    "window_end": var.window_end,
                    "error": str(exc),
                }
            )
            return False

    max_concurrent = int(os.getenv("API_MAX_CONCURRENT", "3"))
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        # 提交所有任务
        future_to_var = {
            executor.submit(run_variant_recursive, var, 0): var
            for var in variants
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_var):
            var = future_to_var[future]
            try:
                ok = future.result()
                if not ok:
                    last_error = next((x["error"] for x in reversed(variant_fail) if x["variant"] == var.variant), "unknown")
                    if var.source in core_sources:
                        raise RuntimeError(f"core source failed [{var.source}] variant={var.variant}: {last_error}")
                    if var.source in optional_sources:
                        print(f"[ingest] optional source failed variant={var.variant}, continue: {last_error}", flush=True)
                    else:
                        raise RuntimeError(f"source failed [{var.source}] variant={var.variant}: {last_error}")
            except Exception as exc:
                # 如果是可选源失败，继续；如果是核心源失败，立即抛出
                if var.source in core_sources:
                    raise

    missing_core = [s for s in sources if s in core_sources and not downloaded.get(s)]
    if missing_core:
        raise RuntimeError(f"remote fetch missing core sources for dt={dt}: {missing_core}")

    status_payload = {
        "dt": dt,
        "mode": mode,
        "task_variant_success": variant_success,
        "task_variant_fail": variant_fail,
        "source_success": sorted([s for s in sources if downloaded.get(s)]),
        "source_fail": {
            s: "all_variants_failed"
            for s in sources
            if not downloaded.get(s)
        },
        "window_start": min([x["window_start"] for x in variant_success], default=""),
        "window_end": max([x["window_end"] for x in variant_success], default=""),
    }
    print("[ingest] remote fetch completed", flush=True)
    return downloaded, status_payload


def ingest_for_dt(
    dt: str,
    sources: list[str],
    include_initial: bool = False,
    preferred_files: dict[str, list[Path]] | None = None,
    variant_success_map: dict[str, list[dict[str, str]]] | None = None,
) -> Path:
    if preferred_files:
        discovered = {source: preferred_files.get(source, []) for source in sources}
    else:
        discovered = discover_files(
            sources=sources,
            include_initial=include_initial,
            include_dropbox=True,
            require_header_match=True,
        )
        discovered = _select_latest_per_source(discovered)

    items: list[ManifestItem] = []
    variant_index: dict[tuple[str, str], dict[str, str]] = {}
    if variant_success_map:
        for source, arr in variant_success_map.items():
            for rec in arr:
                variant_index[(source, rec.get("filename", ""))] = rec

    for source in sources:
        files = discovered.get(source, [])
        for src in files:
            file_hash = sha256_file(src)
            archived = _copy_with_no_overwrite(src=src, target_dir=get_raw_files_dir(dt) / source, file_hash=file_hash)
            rec = variant_index.get((source, src.name), {})
            items.append(
                ManifestItem(
                    dt=dt,
                    source=source,
                    task_variant=rec.get("variant", "local_discovered"),
                    window_start=rec.get("window_start", ""),
                    window_end=rec.get("window_end", ""),
                    filename=archived.name,
                    original_filename=src.name,
                    source_path=str(src),
                    archived_path=str(archived),
                    hash=file_hash,
                    rows=count_csv_rows(archived),
                    created_at=datetime.now(UTC).isoformat(timespec="seconds"),
                )
            )

    if not items:
        raise RuntimeError(f"no source files discovered for dt={dt}, dropbox={get_dropbox_dir()}")

    return write_manifest(dt=dt, items=items)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download/collect csv into raw archive and manifest")
    parser.add_argument("--dt", default=None)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--sources", help="comma separated")
    parser.add_argument("--mode", choices=["daily", "replay", "realtime"], default="daily")
    parser.add_argument("--fetch", action="store_true", help="enable remote backend fetch before ingest")
    parser.add_argument("--include-initial", action="store_true", help="also scan data/initial_csv")
    parser.add_argument("--status-out", help="write source fetch status json")
    parser.add_argument("--user-range-start", help="user registration range start date (YYYY-MM-DD)")
    parser.add_argument("--user-range-end", help="user registration range end date (YYYY-MM-DD)")
    return parser.parse_args()


def _parse_sources(args: argparse.Namespace) -> list[str]:
    if args.sources:
        return [s.strip() for s in args.sources.split(",") if s.strip()]
    return _load_sources()


def _run_single_dt(args: argparse.Namespace, dt: str, sources: list[str]) -> None:
    _validate_policy_and_sources(sources)
    preferred_files: dict[str, list[Path]] | None = None
    status_payload = {
        "dt": dt,
        "mode": args.mode,
        "task_variant_success": [],
        "task_variant_fail": [],
        "source_success": [],
        "source_fail": {},
        "window_start": "",
        "window_end": "",
    }

    variant_success_map: dict[str, list[dict[str, str]]] = {}
    if _remote_enabled(args):
        # 回放模式时使用当天数据窗口
        use_day_window = args.mode == "replay"
        # 获取用户时间范围（可选）
        user_range_start = getattr(args, "user_range_start", None)
        user_range_end = getattr(args, "user_range_end", None)
        preferred_files, status_payload = _remote_fetch(
            dt=dt,
            sources=sources,
            mode=args.mode,
            use_day_window=use_day_window,
            user_range_start=user_range_start,
            user_range_end=user_range_end,
        )
        for rec in status_payload.get("task_variant_success", []):
            variant_success_map.setdefault(rec["source"], []).append(rec)

    ingest_for_dt(
        dt=dt,
        sources=sources,
        include_initial=args.include_initial,
        preferred_files=preferred_files,
        variant_success_map=variant_success_map,
    )

    if args.status_out:
        with open(args.status_out, "w", encoding="utf-8") as f:
            json.dump(status_payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    if args.dt is None:
        args.dt = datetime.now(INDIA_TZ).strftime("%Y-%m-%d")

    sources = _parse_sources(args)

    if args.start_date and args.end_date:
        for dt in iter_dates(args.start_date, args.end_date):
            _run_single_dt(args=args, dt=dt, sources=sources)
        return

    _run_single_dt(args=args, dt=args.dt, sources=sources)


if __name__ == "__main__":
    main()
