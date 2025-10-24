import argparse
import asyncio
import datetime as dt
import logging
import os
import sys
from pathlib import Path

import postprocess
from dotenv import load_dotenv

import sis_scraper


class ColoredFormatter(logging.Formatter):
    """
    Simple wrapper class that adds colors to logging.

    Requires a format, and otherwise accepts any keyword arguments that are accepted by
    logging.Formatter().
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


def logging_init(logs_dir: Path | str, log_level: int = logging.INFO) -> None:
    """
    Initializes logging settings once on startup; these settings determine the behavior
    of all logging calls within this program.
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape and process course data from the RPI SIS."
    )
    parser.add_argument(
        "start_year", type=int, help="The year at which to start scraping from."
    )
    parser.add_argument(
        "end_year", type=int, help="The year at which to stop scraping, inclusive."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only run the scraping step.",
    )
    group.add_argument(
        "--postprocess-only",
        action="store_true",
        help="Only run the postprocessing step.",
    )
    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year

    # Ensure script has a valid parent directory
    parent_dir = Path(__file__).parent
    if parent_dir == Path(__file__):
        print(
            "ERROR: Could not determine this script's parent directory. "
            "Ensure that the script is not being run from within a zip file."
        )
        sys.exit(1)

    # Load environment variables from .env file if it exists
    if not load_dotenv():
        print(
            "ERROR: No environment variables found. Ensure that an .env file exists in "
            "the same directory as this script and that all required variables are set."
        )
        sys.exit(1)

    try:
        logs_dir = parent_dir / os.getenv("SCRAPER_LOGS_DIR")
        output_data_dir = parent_dir / os.getenv("SCRAPER_RAW_OUTPUT_DATA_DIR")
        processed_data_dir = parent_dir / os.getenv("SCRAPER_PROCESSED_OUTPUT_DATA_DIR")
        code_maps_dir = parent_dir / os.getenv("SCRAPER_CODE_MAPS_DIR")
        attribute_code_name_map_path = code_maps_dir / os.getenv(
            "ATTRIBUTE_CODE_NAME_MAP_FILENAME"
        )
        instructor_rcsid_name_map_path = code_maps_dir / os.getenv(
            "INSTRUCTOR_RCSID_NAME_MAP_FILENAME"
        )
        restriction_code_name_map_path = code_maps_dir / os.getenv(
            "RESTRICTION_CODE_NAME_MAP_FILENAME"
        )
        subject_code_name_map_path = code_maps_dir / os.getenv(
            "SUBJECT_CODE_NAME_MAP_FILENAME"
        )
    except TypeError as e:
        print(
            "ERROR: One or more required environment variables are not set. "
            "Ensure all required variables are set in the .env file."
        )
        sys.exit(1)

    logging_init(logs_dir, log_level=logging.INFO)

    if not args.postprocess_only:
        asyncio.run(
            sis_scraper.main(
                output_data_dir=output_data_dir,
                start_year=start_year,
                end_year=end_year,
                attribute_code_name_map_path=attribute_code_name_map_path,
                instructor_rcsid_name_map_path=instructor_rcsid_name_map_path,
                restriction_code_name_map_path=restriction_code_name_map_path,
                subject_code_name_map_path=subject_code_name_map_path,
            )
        )

    if not args.scrape_only:
        postprocess.main(
            output_data_dir=output_data_dir,
            processed_output_data_dir=processed_data_dir,
            attribute_code_name_map_path=attribute_code_name_map_path,
            instructor_rcsid_name_map_path=instructor_rcsid_name_map_path,
            restriction_code_name_map_path=restriction_code_name_map_path,
            subject_code_name_map_path=subject_code_name_map_path,
        )
