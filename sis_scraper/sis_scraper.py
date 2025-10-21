import asyncio
import datetime as dt
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import aiohttp
from sis_api import (
    class_search,
    get_class_attributes,
    get_class_corequisites,
    get_class_crosslists,
    get_class_description,
    get_class_prerequisites,
    get_class_restrictions,
    get_term_subjects,
    reset_class_search,
)


def get_term_code(year: str | int, season: str) -> str:
    """
    Converts a year and academic season into a term code used by SIS.

    @param year: Year as a string or integer, e.g. "2023" or 2023.
    @param season: Academic season as a string, e.g. "Fall", "Spring", "Summer".
    @return: Term code as a string, e.g. "202309" for Fall 2023.
    """
    if year is None or season is None:
        return ""
    if not isinstance(season, str):
        return ""
    try:
        year_int = int(year)
        if year_int < 1000 or year_int > 9999:
            return ""
    except (ValueError, TypeError):
        return ""
    season_lower = season.lower().strip()
    season_map = {
        "fall": f"{year_int}09",
        "summer": f"{year_int}05",
        "spring": f"{year_int}01",
    }
    return season_map.get(season_lower, "")


async def process_class_details(
    session: aiohttp.ClientSession,
    course_data: dict[str, Any],
    class_entry: dict[str, Any],
    instructor_rcsid_name_map: dict[str, str] = None,
    attribute_code_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
) -> None:
    """
    Fetches and parses all details for a given class, populating the provided course
    data dictionary or adding to existing entries as appropriate.

    Takes as input class data fetched from SIS's class search endpoint.

    @param session: aiohttp client session to use for requests.
    @param course_data: Dictionary to populate with course data.
    @param class_entry: Class data fetched from SIS's class search endpoint.
    @param known_rcsid_set: Optional set to populate with known instructor RCSIDs.
    @param attribute_code_name_map: Optional map to populate with attribute codes to names.
    @param restriction_code_name_map: Optional map to populate with restriction codes to names.
    @return: None
    """
    # Example course code: CSCI 1100
    course_code = f"{class_entry['subject']} {class_entry['courseNumber']}"
    term = class_entry["term"]
    crn = class_entry["courseReferenceNumber"]

    # Fetch class details not included in main class details
    # Only fetch if course not already in course data
    if course_code not in course_data:

        # Initialize empty course entry
        course_data[course_code] = {
            "course_name": class_entry["courseTitle"],
            "course_detail": {
                "description": "",
                "corequisite": [],
                "prerequisite": [],
                "crosslist": [],
                "attributes": [],
                "restrictions": [],
                "credits": {
                    "min": float("inf"),
                    "max": 0,
                },
                "sections": [],
            },
        }

        async with asyncio.TaskGroup() as tg:
            description_task = tg.create_task(get_class_description(session, term, crn))
            attributes_task = tg.create_task(get_class_attributes(session, term, crn))
            restrictions_task = tg.create_task(
                get_class_restrictions(session, term, crn)
            )
            prerequisites_task = tg.create_task(
                get_class_prerequisites(session, term, crn)
            )
            corequisites_task = tg.create_task(
                get_class_corequisites(session, term, crn)
            )
            crosslists_task = tg.create_task(get_class_crosslists(session, term, crn))

        # Wait for tasks to complete and get results
        description_data = description_task.result()
        attributes_data = attributes_task.result()
        restrictions_data = restrictions_task.result()
        prerequisites_data = prerequisites_task.result()
        corequisites_data = corequisites_task.result()
        crosslists_data = crosslists_task.result()

        # Build attribute code to name map
        # Attributes are known to be in the format "Attribute Name  CODE"
        # Note the double space between name and code
        if attribute_code_name_map is not None:
            for attribute in attributes_data:
                attribute_split = attribute.split()
                if len(attribute_split) < 2:
                    logging.warning(
                        f"Unexpected attribute format for CRN {crn} in term {term}: {attribute}"
                    )
                    continue
                attribute_code = attribute_split[-1].strip()
                attribute_name = " ".join(attribute_split[:-1]).strip()
                if (
                    attribute_code in attribute_code_name_map
                    and attribute_code_name_map[attribute_code] != attribute_name
                ):
                    logging.warning(
                        f"Conflicting attribute names for {attribute_code} in term {term}: {attribute_code_name_map[attribute_code]} vs. {attribute_name}"
                    )
                attribute_code_name_map[attribute_code] = attribute_name

        # Build restriction code to name map
        # Restrictions are known to be in the format "Restriction Name (CODE)"
        # Note the parentheses around the code
        if restriction_code_name_map is not None:
            restriction_pattern = r"(.*)\((.*)\)"
            for restriction_type in restrictions_data:
                restriction_type = restriction_type.lower().replace("not_", "")
                if restriction_type not in restriction_code_name_map:
                    restriction_code_name_map[restriction_type] = {}
                for restriction in restrictions_data[restriction_type]:
                    restriction_match = re.match(restriction_pattern, restriction)
                    if restriction_match is None or len(restriction_match.groups()) < 2:
                        logging.warning(
                            f"Unexpected restriction format for CRN {crn} in term {term}: {restriction}"
                        )
                        continue
                    restriction_name = restriction_match.group(1).strip()
                    restriction_code = restriction_match.group(2).strip()
                    if (
                        restriction_name in restriction_code_name_map[restriction_type]
                        and restriction_code_name_map[restriction_type][
                            restriction_code
                        ]
                        != restriction_name
                    ):
                        logging.warning(
                            f"Conflicting restriction names for {restriction_code} in term {term}: {restriction_code_name_map[restriction_type][restriction_code]} vs. {restriction_name}"
                        )
                    restriction_code_name_map[restriction_type][
                        restriction_code
                    ] = restriction_name

        # Initialize course entry with details
        course_details = course_data[course_code]["course_detail"]
        course_details["description"] = description_data
        course_details["attributes"] = attributes_data
        course_details["restrictions"] = restrictions_data
        course_details["prerequisite"] = prerequisites_data
        course_details["corequisite"] = corequisites_data
        course_details["crosslist"] = crosslists_data

    course_details = course_data[course_code]["course_detail"]

    course_credits = course_details["credits"]
    course_credits["min"] = min(
        course_credits["min"], class_entry["creditHourLow"] or 0
    )
    course_credits["max"] = max(
        course_credits["max"], class_entry["creditHourHigh"] or 0
    )

    course_sections = course_details["sections"]
    class_faculty = class_entry["faculty"]
    class_faculty_rcsids = []
    for instructor in class_faculty:
        instructor_name = instructor["displayName"]
        rcsid = "Unknown RCSID"
        if "emailAddress" in instructor:
            email_address = instructor["emailAddress"]
            if email_address is not None and email_address.endswith("@rpi.edu"):
                rcsid = email_address.split("@")[0].lower()
                # Add to known RCSID set if provided
                if instructor_rcsid_name_map is not None:
                    instructor_rcsid_name_map[rcsid] = instructor["displayName"]
        else:
            logging.warning(
                f"Missing instructor email address field for CRN {crn} in term {term}: {instructor_name}"
            )
        class_faculty_rcsids.append(f"{instructor_name} ({rcsid})")

    course_sections.append(
        {
            "CRN": class_entry["courseReferenceNumber"],
            "instructor": class_faculty_rcsids,
            "schedule": {},
            "capacity": class_entry["maximumEnrollment"],
            "registered": class_entry["enrollment"],
            "open": class_entry["seatsAvailable"],
        }
    )


async def get_course_data(
    term: str,
    subject: str,
    instructor_rcsid_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
    attribute_code_name_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    limit_per_host: int = 5,
    timeout: int = 60,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term and subject.

    This function spawns its own client session to avoid session state conflicts with
    other subjects that may be processing concurrently.

    In the context of this scraper, a "class" refers to a section of a course, while a
    "course" refers to the overarching course that may have multiple classes.

    The data returned from SIS is keyed by classes, not courses. This function
    manipulates and aggregates this data such that the returned structure is keyed by
    courses instead, with classes as a sub-field of each course.

    @param term: Term code to fetch data for.
    @param subject: Subject code to fetch data for.
    @param instructor_rcsid_name_map: Optional map to populate with instructor RCSIDs to names.
    @param restriction_code_name_map: Optional map to populate with restriction codes to names.
    @param attribute_code_name_map: Optional map to populate with attribute codes to names.
    @param semaphore: Semaphore to limit number of concurrent sessions between multiple calls to this function.
    @param limit_per_host: Maximum number of simultaneous connections a session can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: Dictionary of course data keyed by course code.
    """
    async with semaphore:
        # Limit simultaneous connections to SIS server per session
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=500, limit_per_host=limit_per_host
        )
        timeout = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            try:
                # Reset search state on server before fetching class data
                await reset_class_search(session, term)
                class_data = await class_search(session, term, subject)
                course_data = {}
                async with asyncio.TaskGroup() as tg:
                    for class_entry in class_data:
                        tg.create_task(
                            process_class_details(
                                session,
                                course_data,
                                class_entry,
                                instructor_rcsid_name_map=instructor_rcsid_name_map,
                                restriction_code_name_map=restriction_code_name_map,
                                attribute_code_name_map=attribute_code_name_map,
                            )
                        )
                # Return data sorted by course code
                return dict(sorted(course_data.items()))
            except aiohttp.ClientError as e:
                logging.error(f"Error processing subject {subject} in term {term}: {e}")
                return {}


async def get_term_course_data(
    term: str,
    output_path: Path | str,
    subject_code_name_map: dict[str, str] = None,
    instructor_rcsid_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
    attribute_code_name_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    limit_per_host: int = 5,
    timeout: int = 60,
) -> None:
    """
    Gets all course data for a given term, which includes all subjects in the term.

    This function spawns a client session for each subject to be processed in the term.
    Writes data as JSON after all subjects in the term have been processed.

    @param term: Term code to fetch data for.
    @param output_path: Path to write term course data JSON file to.
    @param subject_code_name_map: Optional map to populate with subject codes to names.
    @param instructor_rcsid_name_map: Optional map to populate with instructor RCSIDs to names.
    @param restriction_code_name_map: Optional map to populate with restriction codes to names.
    @param attribute_code_name_map: Optional map to populate with attribute codes to names.
    @param semaphore: Semaphore to limit number of concurrent sessions.
    @param limit_per_host: Maximum number of simultaneous connections a session can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: None
    """
    async with aiohttp.ClientSession() as session:
        subjects = await get_term_subjects(session, term)

    # Build subject code to name map
    if subject_code_name_map is not None:
        for subject in subjects:
            if (
                subject["code"] in subject_code_name_map
                and subject_code_name_map[subject["code"]] != subject["description"]
            ):
                logging.warning(
                    f"Conflicting subject names for {subject['code']} in term {term}: {subject_code_name_map[subject['code']]} vs. {subject['description']}"
                )
            subject_code_name_map[subject["code"]] = subject["description"]
    logging.info(f"Processing {len(subjects)} subjects for term: {term}")

    # Stores all course data for the term
    term_course_data = {}

    # Process subjects in parallel, each with its own session
    tasks: list[asyncio.Task] = []
    async with asyncio.TaskGroup() as tg:
        for subject in subjects:
            subject_code = subject["code"]
            term_course_data[subject_code] = {
                "subject_name": subject["description"],
                "courses": {},
            }
            task = tg.create_task(
                get_course_data(
                    term,
                    subject_code,
                    instructor_rcsid_name_map=instructor_rcsid_name_map,
                    restriction_code_name_map=restriction_code_name_map,
                    attribute_code_name_map=attribute_code_name_map,
                    semaphore=semaphore,
                    limit_per_host=limit_per_host,
                    timeout=timeout,
                )
            )
            tasks.append(task)

    # Wait for all tasks to complete and gather results
    for i, subject in enumerate(subjects):
        course_data = tasks[i].result()
        term_course_data[subject["code"]]["courses"] = course_data

    if len(term_course_data) == 0:
        return

    # Write all data for term to JSON file
    if isinstance(output_path, str):
        output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Writing data to {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(term_course_data, f, indent=4, ensure_ascii=False)


async def main(
    output_data_dir: Path | str,
    start_year: int = 1998,
    end_year: int = dt.datetime.now().year,
    seasons: list[str] | None = None,
    attribute_code_name_map_path: Path | str | None = None,
    instructor_rcsid_name_map_path: Path | str | None = None,
    restriction_code_name_map_path: Path | str | None = None,
    subject_code_name_map_path: Path | str | None = None,
    semaphore_val: int = 10,
    limit_per_host: int = 5,
    timeout: int = 120,
) -> bool:
    """
    Runs the SIS scraper for the specified range of years and seasons. The earliest
    available term is Summer 1998 (199805).

    Spawns multiple client sessions to process subjects in parallel, with each session
    responsible for processing one subject.

    Course data including restrictions, attributes, instructor names, and subject names
    are codified using code-to-name maps whose file paths may be provided. For each map
    file path parameter:
    - If not provided, the map will be constructed and stored only in memory during
    scraping.
    - If provided but doesn't exist, the map will be constructed and written to the
    file as JSON after scraping.
    - If provided and does exist, the map will be loaded from the file before
    scraping and updated after scraping.

    @param output_data_dir: Directory to write term course data JSON files to.
    @param start_year: Starting year (inclusive) to scrape data for. Defaults to 1998.
    @param end_year: Ending year (inclusive) to scrape data for. Defaults to current year.
    @param seasons: List of academic seasons to scrape data for. Can be any combination of
        "spring", "summer", and "fall". If not specified, all three seasons will be processed.
    @param attribute_code_name_map_path: Path to load/save attribute code mapping JSON file.
    @param instructor_rcsid_name_map_path: Path to load/save instructor RCSID mapping JSON file.
    @param restriction_code_name_map_path: Path to load/save restriction code mapping JSON file.
    @param subject_code_name_map_path: Path to load/save subject code mapping JSON file.
    @param semaphore_val: Maximum number of concurrent client sessions to spawn.
    @param limit_per_host: Maximum number of simultaneous connections a session can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: True on success, False on any unhandled failure.
    """

    if output_data_dir is None:
        logging.error("No data output directory specified")
        return False

    # Convert paths to Path objects if given as strings
    if isinstance(output_data_dir, str):
        output_data_dir = Path(output_data_dir)
    if isinstance(attribute_code_name_map_path, str):
        attribute_code_name_map_path = Path(attribute_code_name_map_path)
    if isinstance(instructor_rcsid_name_map_path, str):
        instructor_rcsid_name_map_path = Path(instructor_rcsid_name_map_path)
    if isinstance(restriction_code_name_map_path, str):
        restriction_code_name_map_path = Path(restriction_code_name_map_path)
    if isinstance(subject_code_name_map_path, str):
        subject_code_name_map_path = Path(subject_code_name_map_path)

    start_time = time.time()

    if seasons is None:
        seasons = ["spring", "summer", "fall"]

    # Create code to name maps for codifying scraped data in post-processing
    subject_code_name_map = {}
    instructor_rcsid_name_map = {}
    restriction_code_name_map = {}
    attribute_code_name_map = {}

    # Load code maps for codifying scraped data in post-processing
    try:
        if attribute_code_name_map_path and attribute_code_name_map_path.exists():
            with attribute_code_name_map_path.open("r", encoding="utf-8") as f:
                attribute_code_name_map = json.load(f)
            logging.info(
                f"Loaded {len(attribute_code_name_map)} attribute code mappings from {attribute_code_name_map_path}"
            )
        elif attribute_code_name_map_path:
            logging.info(
                f"No existing attribute code mappings found at {attribute_code_name_map_path}"
            )

        if instructor_rcsid_name_map_path and instructor_rcsid_name_map_path.exists():
            with instructor_rcsid_name_map_path.open("r", encoding="utf-8") as f:
                instructor_rcsid_name_map = json.load(f)
            logging.info(
                f"Loaded {len(instructor_rcsid_name_map)} instructor RCSID mappings from {instructor_rcsid_name_map_path}"
            )
        elif instructor_rcsid_name_map_path:
            logging.info(
                f"No existing instructor RCSID mappings found at {instructor_rcsid_name_map_path}"
            )

        if restriction_code_name_map_path and restriction_code_name_map_path.exists():
            with restriction_code_name_map_path.open("r", encoding="utf-8") as f:
                restriction_code_name_map = json.load(f)
            logging.info(
                f"Loaded {len(restriction_code_name_map)} restriction code mappings from {restriction_code_name_map_path}"
            )
        elif restriction_code_name_map_path:
            logging.info(
                f"No existing restriction code mappings found at {restriction_code_name_map_path}"
            )

        if subject_code_name_map_path and subject_code_name_map_path.exists():
            with subject_code_name_map_path.open("r", encoding="utf-8") as f:
                subject_code_name_map = json.load(f)
            logging.info(
                f"Loaded {len(subject_code_name_map)} subject code mappings from {subject_code_name_map_path}"
            )
        elif subject_code_name_map_path:
            logging.info(
                f"No existing subject code mappings found at {subject_code_name_map_path}"
            )
    except Exception as e:
        logging.error(f"Error loading code mapping files: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Limit concurrent client sessions and simultaneous connections
    semaphore = asyncio.Semaphore(semaphore_val)

    logging.info("Starting SIS scraper with settings:")
    logging.info(f"  Years: {start_year} - {end_year}")
    logging.info(f"  Seasons: {', '.join(season.capitalize() for season in seasons)}")
    logging.info(f"  Max concurrent sessions: {semaphore._value}")
    logging.info(f"  Max concurrent connections per session: {limit_per_host}")

    try:
        # Process terms in parallel
        async with asyncio.TaskGroup() as tg:
            for year in range(start_year, end_year + 1):
                for season in seasons:
                    term = get_term_code(year, season)
                    if term == "":
                        continue
                    output_path = Path(output_data_dir) / f"{term}.json"
                    tg.create_task(
                        get_term_course_data(
                            term,
                            output_path=output_path,
                            subject_code_name_map=subject_code_name_map,
                            instructor_rcsid_name_map=instructor_rcsid_name_map,
                            restriction_code_name_map=restriction_code_name_map,
                            attribute_code_name_map=attribute_code_name_map,
                            semaphore=semaphore,
                            limit_per_host=limit_per_host,
                            timeout=timeout,
                        )
                    )
    except Exception as e:
        logging.error(f"Error in SIS scraper: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Write code maps to JSON files if code mapping paths are provided
    try:
        if attribute_code_name_map_path:
            attribute_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            attribute_code_name_map = dict(sorted(attribute_code_name_map.items()))
            logging.info(
                f"Writing {len(attribute_code_name_map)} attribute code mappings to {attribute_code_name_map_path}"
            )
            with attribute_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(attribute_code_name_map, f, indent=4, ensure_ascii=False)

        if instructor_rcsid_name_map_path:
            instructor_rcsid_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            instructor_rcsid_name_map = dict(sorted(instructor_rcsid_name_map.items()))
            logging.info(
                f"Writing {len(instructor_rcsid_name_map)} instructor RCSID mappings to {instructor_rcsid_name_map_path}"
            )
            with instructor_rcsid_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(instructor_rcsid_name_map, f, indent=4, ensure_ascii=False)

        if restriction_code_name_map_path:
            restriction_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            restriction_code_name_map = dict(sorted(restriction_code_name_map.items()))
            logging.info(
                f"Writing {len(restriction_code_name_map)} restriction code mappings to {restriction_code_name_map_path}"
            )
            with restriction_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(restriction_code_name_map, f, indent=4, ensure_ascii=False)

        if subject_code_name_map_path:
            subject_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            subject_code_name_map = dict(sorted(subject_code_name_map.items()))
            logging.info(
                f"Writing {len(subject_code_name_map)} subject code mappings to {subject_code_name_map_path}"
            )
            with subject_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(subject_code_name_map, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error writing code mapping files: {e}")
        import traceback

        traceback.print_exc()
        return False

    end_time = time.time()
    logging.info("SIS scraper completed")
    logging.info(f"  Time elapsed: {end_time - start_time:.2f} seconds")

    return True
