"""
Telegram alert system for the BTC prediction bot.

Uses raw requests.post to Telegram Bot API — no extra dependencies.
Alerts are queued and sent from a background thread to avoid blocking the main loop.

Usage:
    from notifications import send_alert, test_connection
    send_alert("TRADE_ENTRY", "Bought UP at 65c, stake $8.50")
    ok, err = test_connection(token, chat_id)
"""

import time
import sqlite3
import threading
import queue
from datetime import datetime, timezone

import requests

from bot_logging import get_logger

logger = get_logger("notifications")

DB_PATH = "data/trades.db"

# Background queue for non-blocking alert sends
_alert_queue = queue.Queue(maxsize=100)
_last_send_time = 0
_last_alert_key = None
_worker_started = False
_worker_lock = threading.Lock()

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MIN_SEND_INTERVAL = 1.0  # seconds between sends (rate limit)


def _get_telegram_config():
    """Read Telegram config from bot_config table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT key, value FROM bot_config WHERE key IN "
            "('telegram_bot_token', 'telegram_chat_id', 'telegram_alerts_enabled')"
        ).fetchall()
        conn.close()
        cfg = {r["key"]: r["value"] for r in rows}
        return {
            "token": cfg.get("telegram_bot_token", ""),
            "chat_id": cfg.get("telegram_chat_id", ""),
            "enabled": cfg.get("telegram_alerts_enabled", "0") == "1",
        }
    except Exception:
        return {"token": "", "chat_id": "", "enabled": False}


def _log_alert(alert_type, message, sent_ok, error_msg=None):
    """Log alert to alert_log table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO alert_log (alert_type, message, sent_ok, error_msg, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (alert_type, message, 1 if sent_ok else 0, error_msg,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _send_telegram(token, chat_id, text):
    """Send a message via Telegram Bot API. Returns (ok, error_msg)."""
    try:
        url = TELEGRAM_API.format(token=token)
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=10)
        if resp.status_code == 200:
            return True, None
        else:
            err = resp.text[:200]
            return False, err
    except Exception as e:
        return False, str(e)[:200]


def _worker():
    """Background thread: drain the alert queue and send to Telegram."""
    global _last_send_time, _last_alert_key
    while True:
        try:
            alert_type, message = _alert_queue.get(timeout=5)
        except queue.Empty:
            continue

        # Deduplicate: skip if same alert_type+message was just sent
        key = f"{alert_type}:{message}"
        if key == _last_alert_key:
            _alert_queue.task_done()
            continue

        # Rate limit
        elapsed = time.time() - _last_send_time
        if elapsed < MIN_SEND_INTERVAL:
            time.sleep(MIN_SEND_INTERVAL - elapsed)

        cfg = _get_telegram_config()
        if cfg["enabled"] and cfg["token"] and cfg["chat_id"]:
            prefix = f"<b>[{alert_type}]</b>\n"
            ok, err = _send_telegram(cfg["token"], cfg["chat_id"], prefix + message)
            _log_alert(alert_type, message, ok, err)
            if ok:
                logger.info(f"Alert sent: {alert_type}", extra={"data": {"message": message}})
            else:
                logger.warning(f"Alert send failed: {alert_type}", extra={"data": {"error": err}})
        else:
            _log_alert(alert_type, message, False, "Telegram not configured/enabled")

        _last_send_time = time.time()
        _last_alert_key = key
        _alert_queue.task_done()


def _ensure_worker():
    """Start the background worker thread if not already running."""
    global _worker_started
    with _worker_lock:
        if not _worker_started:
            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            _worker_started = True


def send_alert(alert_type, message):
    """Queue an alert for sending. Non-blocking.

    Args:
        alert_type: e.g. 'TRADE_ENTRY', 'TRADE_EXIT', 'DAILY_LOSS_LIMIT', 'DRAWDOWN'
        message: Human-readable alert text
    """
    _ensure_worker()
    try:
        _alert_queue.put_nowait((alert_type, message))
    except queue.Full:
        logger.warning("Alert queue full, dropping alert", extra={"data": {"type": alert_type}})


def test_connection(token, chat_id):
    """Send a test message to verify Telegram config. Blocking call.

    Returns:
        (ok: bool, error_msg: str or None)
    """
    text = "BTC Bot connected! Alerts are working."
    ok, err = _send_telegram(token, chat_id, text)
    _log_alert("TEST", text, ok, err)
    if ok:
        logger.info("Telegram test connection successful")
    else:
        logger.warning("Telegram test connection failed", extra={"data": {"error": err}})
    return ok, err
