"""
logging_config.py — Standard Python logging setup for the health platform.

Format: 2026-02-28 10:00:00 | INFO     | module_name | message
Output: console + rotating daily log file (append, no deletion of old files).
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_DEFAULT_LOG_ROOT = "/Users/Shared/data_lake/logs"


def get_logger(name: str, log_root: Optional[str] = None) -> logging.Logger:
    """
    Return a named logger with console + file handlers.

    The file handler appends to health_platform_{date}.log in log_root.
    Safe to call multiple times — handlers are not duplicated.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler — append, daily rotation via filename
    root = Path(log_root or os.environ.get("HEALTH_LOG_ROOT", _DEFAULT_LOG_ROOT))
    root.mkdir(parents=True, exist_ok=True)
    log_file = root / f"health_platform_{date.today().isoformat()}.log"

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
