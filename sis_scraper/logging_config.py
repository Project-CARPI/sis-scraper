import datetime as dt
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """
    Simple wrapper class that adds colors to logging.

    Requires a format, and otherwise accepts any keyword arguments that are
    accepted by logging.Formatter().
    """

    def __init__(self, fmt: str, **kwargs):
        self._fmt = fmt
        self._kwargs = kwargs
        self._reset_color = "\x1b[0m"
        self._COLORS = {
            logging.DEBUG: "\x1b[38;20m",  # Gray
            logging.INFO: "\x1b[38;20m",  # Gray
            logging.WARNING: "\x1b[33;20m",  # Yellow
            logging.ERROR: "\x1b[31;20m",  # Red
            logging.CRITICAL: "\x1b[31;1m",  # Dark red
        }

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS[record.levelno]
        formatter = logging.Formatter(
            **self._kwargs, fmt=f"{color}{self._fmt}{self._reset_color}"
        )
        return formatter.format(record)


def init_logging(
    logs_dir: Path | str,
    log_level: int = logging.INFO,
    retention_days: int = 5,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """
    Initializes logging settings once on startup; these settings determine the
    behavior of all logging calls within this program.
    """
    if logs_dir is None:
        raise ValueError("logs_dir must be specified")
    if isinstance(logs_dir, str):
        logs_dir = Path(logs_dir)

    # Logging format config
    formatter_config = {
        "fmt": "[%(asctime)s %(levelname)s] %(message)s",
        "datefmt": "%H:%M:%S",
    }
    color_formatter = ColoredFormatter(**formatter_config)
    default_formatter = logging.Formatter(**formatter_config)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(color_formatter)

    # Rotating file handler
    if not logs_dir.exists():
        logs_dir.mkdir()
        logging.info(f"No logs directory detected, creating one at {logs_dir}")
    for log in logs_dir.iterdir():
        create_time = dt.datetime.fromtimestamp(os.path.getctime(log))
        if create_time < dt.datetime.now() - dt.timedelta(days=retention_days):
            log.unlink()
    curr_time = dt.datetime.now().strftime("%Y.%m.%d %H.%M.%S")
    logfile_path = logs_dir / f"{curr_time}.log"
    logfile_path.touch()
    file_handler = RotatingFileHandler(
        filename=logfile_path,
        encoding="utf-8",
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setLevel(log_level)
    # Normal Formatter is used instead of ColoredFormatter for file
    # logging because colors would just render as text.
    file_handler.setFormatter(default_formatter)

    # Add handlers to root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
