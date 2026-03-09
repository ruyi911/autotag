from __future__ import annotations

import os

import requests


def _enabled() -> bool:
    return os.getenv("ALERT_TELEGRAM_ENABLED", "0") == "1"


def send_telegram_alert(subject: str, body: str) -> None:
    if not _enabled():
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    text = f"{subject}\n\n{body}"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    requests.post(url, json=payload, timeout=15).raise_for_status()


def send_alert(subject: str, body: str) -> None:
    send_telegram_alert(subject, body)
