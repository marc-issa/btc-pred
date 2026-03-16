"""
JSON rotating file logger for the BTC prediction bot.

Usage:
    from bot_logging import get_logger
    logger = get_logger("auto")
    logger.info("Trade entered", extra={"data": {"side": "UP", "price": 0.65}})
"""

import os
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone


LOG_DIR = "data"
LOG_FILE = os.path.join(LOG_DIR, "bot.log")
MAX_BYTES = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 5


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        # Include extra data if provided
        if hasattr(record, "data") and record.data:
            entry["data"] = record.data
        if record.exc_info and record.exc_info[0]:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def get_logger(name="bot"):
    """Get or create a JSON rotating file logger.

    Args:
        name: Logger name (e.g., 'auto', 'dashboard', 'notifications')

    Returns:
        logging.Logger configured with JSON rotating file handler
    """
    logger = logging.getLogger(f"btc_bot.{name}")

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    os.makedirs(LOG_DIR, exist_ok=True)

    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(JSONFormatter())

    logger.addHandler(handler)

    # Don't propagate to root logger (avoid duplicate console output)
    logger.propagate = False

    return logger
