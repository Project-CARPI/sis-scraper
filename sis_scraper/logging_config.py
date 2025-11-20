import datetime as dt
import logging
import os
import sys
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


def init_logging(logs_dir: Path | str, log_level: int = logging.INFO) -> None:
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
    formatter = logging.Formatter(**formatter_config)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Console logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(color_formatter)
    root_logger.addHandler(console_handler)

    # File logging
    if not logs_dir.exists():
        logs_dir.mkdir()
        logging.info(f"No logs directory detected, creating one at {logs_dir}")
    for log in logs_dir.iterdir():
        create_time = dt.datetime.fromtimestamp(os.path.getctime(log))
        if create_time < dt.datetime.now() - dt.timedelta(days=5):
            log.unlink()
    curr_time = dt.datetime.now().strftime("%Y.%m.%d %H.%M.%S")
    logfile_path = logs_dir / f"{curr_time}.log"
    logfile_path.touch()
    file_handler = logging.FileHandler(filename=logfile_path, encoding="utf-8")
    file_handler.setLevel(log_level)
    # Normal Formatter is used instead of ColoredFormatter for file
    # logging because colors would just render as text.
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
