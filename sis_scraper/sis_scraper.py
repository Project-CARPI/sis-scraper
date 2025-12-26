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
    _process_class_meetings,
    reset_class_search,
)

logger = logging.getLogger(__name__)


def get_term_code(year: str | int, season: str) -> str:
    """
    Converts a year and academic season into a term code used by SIS.

    @param year: Year as a string or integer, e.g. "2023" or 2023.
    @param season: Academic season as a string, e.g. "Fall", "Spring",
        "Summer".
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
    sis_class_entry: dict[str, Any],
    instructor_rcsid_name_map: dict[str, str] = None,
    attribute_code_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
) -> None:
    """
    Fetches and parses all details for a given class, populating the provided
    course data dictionary or adding to existing entries as appropriate.

    Takes as input class data fetched from SIS's class search endpoint.

    @param session: aiohttp client session to use for requests.
    @param course_data: Dictionary to populate with course data.
    @param sis_class_entry: Class data fetched from SIS's class search
        endpoint.
    @param known_rcsid_set: Optional set to populate with known instructor
        RCSIDs.
    @param attribute_code_name_map: Optional map to populate with attribute
        codes to names.
    @param restriction_code_name_map: Optional map to populate with restriction
        codes to names.
    @return: None
    """
    course_num = sis_class_entry["courseNumber"]
    term = sis_class_entry["term"]
    crn = sis_class_entry["courseReferenceNumber"]
    sis_meetings_list = sis_class_entry["meetingsFaculty"]

    # Initialize course entry if not already present
    if course_num not in course_data:
        course_data[course_num] = []

    # Initialize empty class entry
    class_entry = {
        "courseReferenceNumber": sis_class_entry["courseReferenceNumber"],
        "sectionNumber": sis_class_entry["sequenceNumber"],
        "title": sis_class_entry["courseTitle"],
        "description": "",
        "attributes": [],
        "restrictions": {},
        "prerequisites": [],
        "corequisites": [],
        "crosslists": [],
        "creditMin": sis_class_entry["creditHourLow"],
        "creditMax": sis_class_entry["creditHourHigh"],
        "seatsCapacity": sis_class_entry["maximumEnrollment"],
        "seatsRegistered": sis_class_entry["enrollment"],
        "seatsAvailable": sis_class_entry["seatsAvailable"],
        "waitlistCapacity": sis_class_entry["waitCapacity"],
        "waitlistRegistered": sis_class_entry["waitCount"],
        "waitlistAvailable": sis_class_entry["waitAvailable"],
        "faculty": [],
        "meetingInfo": _process_class_meetings(sis_meetings_list),
    }

    # Fetch class details not included in main class details
    async with asyncio.TaskGroup() as tg:
        description_task = tg.create_task(get_class_description(session, term, crn))
        attributes_task = tg.create_task(get_class_attributes(session, term, crn))
        restrictions_task = tg.create_task(get_class_restrictions(session, term, crn))
        prerequisites_task = tg.create_task(get_class_prerequisites(session, term, crn))
        corequisites_task = tg.create_task(get_class_corequisites(session, term, crn))
        crosslists_task = tg.create_task(get_class_crosslists(session, term, crn))

    # Wait for tasks to complete and get results
    description_data = description_task.result()
    attributes_data = attributes_task.result()
    restrictions_data = restrictions_task.result()
    prerequisites_data = prerequisites_task.result()
    corequisites_data = corequisites_task.result()
    crosslists_data = crosslists_task.result()

    # Fill class entry with fetched details
    class_entry["description"] = description_data
    class_entry["attributes"] = attributes_data
    class_entry["restrictions"] = restrictions_data
    class_entry["prerequisites"] = prerequisites_data
    class_entry["corequisites"] = corequisites_data
    class_entry["crosslists"] = crosslists_data

    # Process instructor RCSIDs and names
    class_faculty = class_entry["faculty"]
    for instructor in sis_class_entry["faculty"]:
        instructor_name = instructor["displayName"]
        email_address = instructor["emailAddress"]
        # Add faculty entry to class faculty list
        class_faculty.append(
            {
                "bannerId": instructor["bannerId"],
                "displayName": instructor_name,
                "emailAddress": email_address,
                "primaryFaculty": instructor["primaryIndicator"],
            }
        )
        if "emailAddress" not in instructor:
            logger.warning(
                f"Missing instructor email address field for CRN {crn} "
                f"in term {term}: {instructor_name}"
            )
            continue
        # Add faculty RCSID to known RCSID map if provided
        if (
            email_address is not None
            and email_address.endswith("@rpi.edu")
            and instructor_rcsid_name_map is not None
        ):
            rcsid = email_address.split("@")[0].lower()
            instructor_rcsid_name_map[rcsid] = instructor_name

    # Append class entry to course data
    course_data[course_num].append(class_entry)

    # Add to attribute code-to-name map
    # Attributes are known to be in the format "Attribute Name  CODE"
    # Note the double space between name and code
    if attribute_code_name_map is not None:
        for attribute in attributes_data:
            attribute_split = attribute.split()
            if len(attribute_split) < 2:
                logger.warning(
                    f"Skipping unexpected attribute format for CRN {crn} "
                    f"in term {term}: {attribute}"
                )
                continue
            attribute_code = attribute_split[-1].strip()
            attribute_name = " ".join(attribute_split[:-1]).strip()
            if (
                attribute_code in attribute_code_name_map
                and attribute_code_name_map[attribute_code] != attribute_name
            ):
                logger.warning(
                    f"Conflicting attribute names for {attribute_code} "
                    f"in term {term}: "
                    f"{attribute_code_name_map[attribute_code]} vs. {attribute_name}"
                )
            attribute_code_name_map[attribute_code] = attribute_name

    # Add to restriction code-to-name map
    # Restrictions are known to be in the format "Restriction Name (CODE)" except
    # for special approvals, which are handled explicitly as a special case.
    if restriction_code_name_map is not None:
        restriction_pattern = r"(.*)\((.*)\)"
        for restriction_type in restrictions_data:
            restriction_type = restriction_type.lower().replace("not_", "")
            if restriction_type not in restriction_code_name_map:
                restriction_code_name_map[restriction_type] = {}
            for restriction in restrictions_data[restriction_type]:
                restriction_match = re.match(restriction_pattern, restriction)
                if restriction_match is None or len(restriction_match.groups()) < 2:
                    # Skip unexpected restriction formats or special approvals
                    continue
                restriction_name = restriction_match.group(1).strip()
                restriction_code = restriction_match.group(2).strip()
                if (
                    restriction_name in restriction_code_name_map[restriction_type]
                    and restriction_code_name_map[restriction_type][restriction_code]
                    != restriction_name
                ):
                    logger.warning(
                        f"Conflicting restriction names for {restriction_code} "
                        f"in term {term}: "
                        f"{restriction_code_name_map[
                            restriction_type
                        ][restriction_code]} vs. {restriction_name}"
                    )
                restriction_code_name_map[restriction_type][
                    restriction_code
                ] = restriction_name


async def get_subj_course_data(
    term: str,
    subject: str,
    instructor_rcsid_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
    attribute_code_name_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    tcp_connector: aiohttp.TCPConnector = None,
    timeout: int = 30,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term and subject.

    This function spawns its own client session to avoid session state conflicts with
    other subjects that may be processing concurrently.

    @param term: Term code to fetch data for.
    @param subject: Subject code to fetch data for.
    @param instructor_rcsid_name_map: Optional map to populate with instructor
        RCSIDs to names.
    @param restriction_code_name_map: Optional map to populate with restriction
        codes to names.
    @param attribute_code_name_map: Optional map to populate with attribute
        codes to names.
    @param semaphore: Semaphore to limit number of concurrent sessions between
        multiple calls to this function.
    @param limit_per_host: Maximum number of simultaneous connections a session
        can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: Dictionary of course data keyed by course code.
    """
    async with semaphore:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(
            connector=tcp_connector, timeout=timeout_obj
        ) as session:
            try:
                # Reset search state on server before fetching class data
                await reset_class_search(session, term)
                class_data = await class_search(session, term, subject)
                subj_class_data = {}
                async with asyncio.TaskGroup() as tg:
                    for class_entry in class_data:
                        tg.create_task(
                            process_class_details(
                                session,
                                subj_class_data,
                                class_entry,
                                instructor_rcsid_name_map=instructor_rcsid_name_map,
                                restriction_code_name_map=restriction_code_name_map,
                                attribute_code_name_map=attribute_code_name_map,
                            )
                        )
                # Sort class entries by section number
                for course_num in subj_class_data:
                    subj_class_data[course_num] = sorted(
                        subj_class_data[course_num], key=lambda x: x["sectionNumber"]
                    )
                # Return data sorted by course code
                return dict(sorted(subj_class_data.items()))
            except aiohttp.ClientError as e:
                logger.error(f"Error processing subject {subject} in term {term}: {e}")
                return {}


async def get_term_course_data(
    term: str,
    output_path: Path | str,
    subject_code_name_map: dict[str, str] = None,
    instructor_rcsid_name_map: dict[str, str] = None,
    restriction_code_name_map: dict[str, dict[str, str]] = None,
    attribute_code_name_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    tcp_connector: aiohttp.TCPConnector = None,
    timeout: int = 30,
) -> None:
    """
    Gets all course data for a given term, which includes all subjects in the
    term.

    This function spawns a client session for each subject to be processed in the
    term. Writes data as JSON after all subjects in the term have been processed.

    @param term: Term code to fetch data for.
    @param output_path: Path to write term course data JSON file to.
    @param subject_code_name_map: Optional map to populate with subject codes to
        names.
    @param instructor_rcsid_name_map: Optional map to populate with instructor
        RCSIDs to names.
    @param restriction_code_name_map: Optional map to populate with restriction
        codes to names.
    @param attribute_code_name_map: Optional map to populate with attribute
        codes to names.
    @param semaphore: Semaphore to limit number of concurrent sessions.
    @param limit_per_host: Maximum number of simultaneous connections a session
        can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: None
    """
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    try:
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            subjects = await get_term_subjects(session, term)
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching subjects for term {term}: {e}")
        return False

    # Build subject code to name map
    if subject_code_name_map is not None:
        for subject in subjects:
            if (
                subject["code"] in subject_code_name_map
                and subject_code_name_map[subject["code"]] != subject["description"]
            ):
                logger.warning(
                    f"Conflicting subject names for {subject['code']} "
                    f"in term {term}: "
                    f"{subject_code_name_map[subject['code']]} "
                    f"vs. {subject['description']}"
                )
            subject_code_name_map[subject["code"]] = subject["description"]
    logger.info(f"Processing {len(subjects)} subjects for term: {term}")

    # Stores all course data for the term
    term_course_data = {}

    # Process subjects in parallel, each with its own session
    tasks: list[asyncio.Task] = []
    try:
        async with asyncio.TaskGroup() as tg:
            for subject in subjects:
                subject_code = subject["code"]
                term_course_data[subject_code] = {
                    "subjectName": subject["description"],
                    "courses": {},
                }
                task = tg.create_task(
                    get_subj_course_data(
                        term,
                        subject_code,
                        instructor_rcsid_name_map=instructor_rcsid_name_map,
                        restriction_code_name_map=restriction_code_name_map,
                        attribute_code_name_map=attribute_code_name_map,
                        semaphore=semaphore,
                        tcp_connector=tcp_connector,
                        timeout=timeout,
                    )
                )
                tasks.append(task)
    except Exception as e:
        logger.error(f"Error processing subjects for term {term}: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Wait for all tasks to complete and gather results
    for i, subject in enumerate(subjects):
        course_data = tasks[i].result()
        term_course_data[subject["code"]]["courses"] = course_data

    if len(term_course_data) == 0:
        return False

    # Write all data for term to JSON file
    if isinstance(output_path, str):
        output_path = Path(output_path)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing data to {output_path}")
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(term_course_data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error writing data to {output_path}: {e}")
        return False

    return True


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
    timeout: int = 30,
) -> bool:
    """
    Runs the SIS scraper for the specified range of years and seasons. The
    earliest available term is Summer 1998 (199805).

    Spawns multiple client sessions to process subjects in parallel, with each
    session responsible for processing one subject.

    Course data including restrictions, attributes, instructor names, and
    subject names are codified using code-to-name maps whose file paths may be
    provided. For each map file path parameter:
    - If not provided, the map will be constructed and stored only in memory
      during scraping.
    - If provided but doesn't exist, the map will be constructed and written to
      the file as JSON after scraping.
    - If provided and does exist, the map will be loaded from the file before
      scraping and updated after scraping.

    @param output_data_dir: Directory to write term course data JSON files to.
    @param start_year: Starting year (inclusive) to scrape data for. Defaults
        to 1998.
    @param end_year: Ending year (inclusive) to scrape data for. Defaults to
        current year.
    @param seasons: List of academic seasons to scrape data for. Can be any
        combination of "spring", "summer", and "fall". If not specified, all
        three seasons will be processed.
    @param attribute_code_name_map_path: Path to load/save attribute code
        mapping JSON file.
    @param instructor_rcsid_name_map_path: Path to load/save instructor RCSID
        mapping JSON file.
    @param restriction_code_name_map_path: Path to load/save restriction code
        mapping JSON file.
    @param subject_code_name_map_path: Path to load/save subject code mapping
        JSON file.
    @param semaphore_val: Maximum number of concurrent client sessions to
        spawn.
    @param limit_per_host: Maximum number of simultaneous connections a session
        can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: True on success, False on any unhandled failure.
    """

    if output_data_dir is None:
        logger.fatal("No data output directory specified")
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
            logger.info(
                f"Loaded {len(attribute_code_name_map)} attribute code mappings "
                f"from {attribute_code_name_map_path}"
            )
        elif attribute_code_name_map_path:
            logger.info(
                f"No existing attribute code mappings found "
                f"at {attribute_code_name_map_path}"
            )

        if instructor_rcsid_name_map_path and instructor_rcsid_name_map_path.exists():
            with instructor_rcsid_name_map_path.open("r", encoding="utf-8") as f:
                instructor_rcsid_name_map = json.load(f)
            logger.info(
                f"Loaded {len(instructor_rcsid_name_map)} instructor RCSID mappings "
                f"from {instructor_rcsid_name_map_path}"
            )
        elif instructor_rcsid_name_map_path:
            logger.info(
                f"No existing instructor RCSID mappings found "
                f"at {instructor_rcsid_name_map_path}"
            )

        if restriction_code_name_map_path and restriction_code_name_map_path.exists():
            with restriction_code_name_map_path.open("r", encoding="utf-8") as f:
                restriction_code_name_map = json.load(f)
            logger.info(
                f"Loaded {len(restriction_code_name_map)} restriction code mappings "
                f"from {restriction_code_name_map_path}"
            )
        elif restriction_code_name_map_path:
            logger.info(
                f"No existing restriction code mappings found "
                f"at {restriction_code_name_map_path}"
            )

        if subject_code_name_map_path and subject_code_name_map_path.exists():
            with subject_code_name_map_path.open("r", encoding="utf-8") as f:
                subject_code_name_map = json.load(f)
            logger.info(
                f"Loaded {len(subject_code_name_map)} subject code mappings "
                f"from {subject_code_name_map_path}"
            )
        elif subject_code_name_map_path:
            logger.info(
                f"No existing subject code mappings found "
                f"at {subject_code_name_map_path}"
            )
    except Exception as e:
        logger.fatal(f"Error loading code mapping files: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Limit concurrent client sessions and simultaneous connections
    semaphore = asyncio.Semaphore(semaphore_val)

    logger.info("Starting SIS scraper with settings:")
    logger.info(f"  Years: {start_year} - {end_year}")
    logger.info(f"  Seasons: {', '.join(season.capitalize() for season in seasons)}")
    logger.info(f"  Max concurrent sessions: {semaphore._value}")
    logger.info(f"  Max concurrent connections per session: {limit_per_host}")

    tasks: list[asyncio.Task] = []
    num_terms_processed = 0
    try:
        # Global TCP connector for all sessions
        async with aiohttp.TCPConnector(
            ttl_dns_cache=500, limit_per_host=limit_per_host
        ) as tcp_connector:
            # Process terms in parallel
            async with asyncio.TaskGroup() as tg:
                for year in range(start_year, end_year + 1):
                    for season in seasons:
                        term = get_term_code(year, season)
                        if term == "":
                            continue
                        output_path = Path(output_data_dir) / f"{term}.json"
                        task = tg.create_task(
                            get_term_course_data(
                                term,
                                output_path=output_path,
                                subject_code_name_map=subject_code_name_map,
                                instructor_rcsid_name_map=instructor_rcsid_name_map,
                                restriction_code_name_map=restriction_code_name_map,
                                attribute_code_name_map=attribute_code_name_map,
                                semaphore=semaphore,
                                tcp_connector=tcp_connector,
                                timeout=timeout,
                            )
                        )
                    )
                    tasks.append(task)

        # Wait for all tasks to complete
        for task in tasks:
            success = task.result()
            if success:
                num_terms_processed += 1

    except Exception as e:
        logger.fatal(f"Error in SIS scraper: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Write code maps to JSON files if code mapping paths are provided
    try:
        if attribute_code_name_map_path:
            attribute_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            attribute_code_name_map = dict(sorted(attribute_code_name_map.items()))
            logger.info(
                f"Writing {len(attribute_code_name_map)} attribute code mappings "
                f"to {attribute_code_name_map_path}"
            )
            with attribute_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(attribute_code_name_map, f, indent=4, ensure_ascii=False)

        if instructor_rcsid_name_map_path:
            instructor_rcsid_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            instructor_rcsid_name_map = dict(sorted(instructor_rcsid_name_map.items()))
            logger.info(
                f"Writing {len(instructor_rcsid_name_map)} instructor RCSID mappings "
                f"to {instructor_rcsid_name_map_path}"
            )
            with instructor_rcsid_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(instructor_rcsid_name_map, f, indent=4, ensure_ascii=False)

        if restriction_code_name_map_path:
            restriction_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            restriction_code_name_map = dict(sorted(restriction_code_name_map.items()))
            logger.info(
                f"Writing {len(restriction_code_name_map)} restriction code mappings "
                f"to {restriction_code_name_map_path}"
            )
            with restriction_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(restriction_code_name_map, f, indent=4, ensure_ascii=False)

        if subject_code_name_map_path:
            subject_code_name_map_path.parent.mkdir(parents=True, exist_ok=True)
            subject_code_name_map = dict(sorted(subject_code_name_map.items()))
            logger.info(
                f"Writing {len(subject_code_name_map)} subject code mappings "
                f"to {subject_code_name_map_path}"
            )
            with subject_code_name_map_path.open("w", encoding="utf-8") as f:
                json.dump(subject_code_name_map, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error writing code mapping files: {e}")
        import traceback

        traceback.print_exc()
        return False

    end_time = time.time()
    logger.info("SIS scraper completed")
    logger.info(f"  Terms processed: {num_terms_processed}")
    logger.info(f"  Time elapsed: {end_time - start_time:.2f} seconds")

    return True
