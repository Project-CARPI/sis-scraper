import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import postprocess
from dotenv import load_dotenv
from logging_config import init_logging

import sis_scraper

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

    init_logging(logs_dir, log_level=logging.INFO)

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
