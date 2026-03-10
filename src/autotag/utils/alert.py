from __future__ import annotations

import os
import sys

import requests


def _enabled() -> bool:
    return os.getenv("ALERT_TELEGRAM_ENABLED", "0") == "1"


def send_telegram_alert(subject: str, body: str) -> bool:
    if not _enabled():
        print("[alert] telegram disabled: ALERT_TELEGRAM_ENABLED!=1", file=sys.stderr, flush=True)
        return False

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("[alert] telegram disabled: missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID", file=sys.stderr, flush=True)
        return False

    text = f"{subject}\n\n{body}"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        requests.post(url, json=payload, timeout=15).raise_for_status()
    except Exception as exc:
        print(f"[alert] telegram send failed: {exc}", file=sys.stderr, flush=True)
        return False
    return True


def send_alert(subject: str, body: str) -> bool:
    return send_telegram_alert(subject, body)
