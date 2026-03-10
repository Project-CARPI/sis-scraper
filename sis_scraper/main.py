import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import sis_scraper
import json_to_sql
import postprocess
from dotenv import load_dotenv
from logging_config import init_logging

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Scrape and process course data from the RPI Student Information "
        "System (SIS)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    scrape_parser = subparsers.add_parser("scrape", help="Scrape course data from SIS.")
    scrape_parser.add_argument(
        "start_year", type=int, help="The year at which to start scraping from."
    )
    scrape_parser.add_argument(
        "end_year", type=int, help="The year at which to stop scraping, inclusive."
    )
    subparsers.add_parser("postprocess", help="Process scraped JSON data.")
    subparsers.add_parser("commitdb", help="Commit processed data to the database.")
    args = parser.parse_args()

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
        generated_instructor_rcsid_name_map_path = code_maps_dir / (
            "generated_" + os.getenv("INSTRUCTOR_RCSID_NAME_MAP_FILENAME")
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

    if args.command == "scrape":
        if not asyncio.run(
            sis_scraper.main(
                output_data_dir=output_data_dir,
                start_year=args.start_year,
                end_year=args.end_year,
            )
        ):
            sys.exit(1)

    elif args.command == "postprocess":
        if not postprocess.main(
            output_data_dir=output_data_dir,
            processed_output_data_dir=processed_data_dir,
            attribute_code_name_map_path=attribute_code_name_map_path,
            generated_instructor_rcsid_name_map_path=generated_instructor_rcsid_name_map_path,
            instructor_rcsid_name_map_path=instructor_rcsid_name_map_path,
            restriction_code_name_map_path=restriction_code_name_map_path,
            subject_code_name_map_path=subject_code_name_map_path,
        ):
            sys.exit(1)

    elif args.command == "commitdb":
        db_dialect = os.getenv("DB_DIALECT")
        db_api = os.getenv("DB_API")
        db_hostname = os.getenv("DB_HOSTNAME")
        db_username = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_schema = os.getenv("DB_SCHEMA")
        if not all(
            [db_dialect, db_api, db_hostname, db_username, db_password, db_schema]
        ):
            print(
                "ERROR: One or more database environment variables are not set. "
                "Ensure all required DB variables are set in the .env file."
            )
            sys.exit(1)
        if not json_to_sql.main(
            processed_data_dir=processed_data_dir,
            db_dialect=db_dialect,
            db_api=db_api,
            db_hostname=db_hostname,
            db_username=db_username,
            db_password=db_password,
            db_schema=db_schema,
            attribute_code_name_map_path=attribute_code_name_map_path,
            instructor_rcsid_name_map_path=instructor_rcsid_name_map_path,
            generated_instructor_rcsid_name_map_path=generated_instructor_rcsid_name_map_path,
            restriction_code_name_map_path=restriction_code_name_map_path,
            subject_code_name_map_path=subject_code_name_map_path,
        ):
            sys.exit(1)
