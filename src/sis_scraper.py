import asyncio
import datetime as dt
import json
import logging
import time
import traceback
from pathlib import Path
from typing import Any

import aiohttp
from sis_api import (
    class_search,
    get_class_attributes,
    get_class_corequisites,
    get_class_crosslists,
    get_class_description,
    get_class_details,
    get_class_enrollment,
    get_class_faculty_meetings,
    get_class_prerequisites,
    get_class_restrictions,
    get_term_subjects,
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


def write_json(json_data: dict[str, Any], output_path: Path | str) -> None:
    """
    Helper function to write JSON data to a file.

    @param json_data: The JSON data to write to the file.
    @param output_path: The path to the file to write the JSON data to.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing data to {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=4, ensure_ascii=False)


async def process_class_details(
    session: aiohttp.ClientSession,
    term_crn_set: set[str],
    sis_class_entry: dict[str, Any] | None = None,
    term: str | None = None,
    crn: str | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """
    Fetches and parses all details for a given class. Returns a tuple containing
    the subject description, course number, and the fully populated class entry.

    Takes as input class data fetched from SIS's class search endpoint.

    @param session: aiohttp client session to use for requests.
    @param term_crn_set: Set of all CRNs processed in the term.
    @param sis_class_entry: Class data fetched from SIS's class search
            endpoint.
    @param term: Term code of the class to fetch details for. Required if
        sis_class_entry is not provided.
    @param crn: CRN of the class to fetch details for. Required if
        sis_class_entry is not provided.
    @return: A tuple of (subject description, course number, class entry data),
        or None on error.
    """
    if sis_class_entry is None and (term is None or crn is None):
        raise ValueError("Either sis_class_entry or both term and crn must be provided")

    # Extract basic class details from SIS class entry if provided
    if sis_class_entry is not None:
        subject_desc = sis_class_entry["subjectDescription"]
        course_num = sis_class_entry["courseNumber"]
        term = sis_class_entry["term"]
        crn = sis_class_entry["courseReferenceNumber"]

    # Add CRN to term CRN set
    if crn in term_crn_set:
        logger.warning(f"Duplicate CRN {crn} found in term {term}")
    else:
        term_crn_set.add(crn)

    # Initialize empty class entry
    class_entry = {
        "courseReferenceNumber": crn,
        "sectionNumber": "",
        "title": "",
        "description": "",
        "attributes": [],
        "restrictions": {},
        "prerequisites": [],
        "corequisites": [],
        "crosslists": [],
        "creditMin": -1,
        "creditMax": -1,
        "seatsCapacity": -1,
        "seatsRegistered": -1,
        "seatsAvailable": -1,
        "waitlistCapacity": -1,
        "waitlistRegistered": -1,
        "waitlistAvailable": -1,
        "faculty": [],
        "meetingInfo": [],
    }

    # Fetch class details not included in SIS class search
    async with asyncio.TaskGroup() as tg:
        description_task = tg.create_task(get_class_description(session, term, crn))
        attributes_task = tg.create_task(get_class_attributes(session, term, crn))
        restrictions_task = tg.create_task(get_class_restrictions(session, term, crn))
        prerequisites_task = tg.create_task(get_class_prerequisites(session, term, crn))
        corequisites_task = tg.create_task(get_class_corequisites(session, term, crn))
        crosslists_task = tg.create_task(get_class_crosslists(session, term, crn))
        faculty_meetings_task = tg.create_task(
            get_class_faculty_meetings(session, term, crn)
        )
        # Fetch full class details if not provided from SIS class search
        if sis_class_entry is None:
            details_task = tg.create_task(get_class_details(session, term, crn))
            enrollment_task = tg.create_task(get_class_enrollment(session, term, crn))

    # Wait for tasks to complete and get results
    description_data = description_task.result()
    attributes_data = attributes_task.result()
    restrictions_data = restrictions_task.result()
    prerequisites_data = prerequisites_task.result()
    corequisites_data = corequisites_task.result()
    crosslists_data = crosslists_task.result()
    faculty_meetings_data = faculty_meetings_task.result()
    if sis_class_entry is None:
        details_data = details_task.result()
        enrollment_data = enrollment_task.result()

    # Extract subject and course number from full details if SIS class entry not provided
    if sis_class_entry is None:
        subject_desc = details_data["subjectName"]
        course_num = details_data["courseNumber"]

    # Fill class entry with fetched details
    class_entry["description"] = description_data
    class_entry["attributes"] = attributes_data
    class_entry["restrictions"] = restrictions_data
    class_entry["prerequisites"] = prerequisites_data
    class_entry["corequisites"] = corequisites_data
    class_entry["crosslists"] = crosslists_data
    class_entry["faculty"] = faculty_meetings_data["faculty"]
    class_entry["meetingInfo"] = faculty_meetings_data["meetings"]
    # Fill class entry with SIS class search data if provided
    if sis_class_entry is not None:
        class_entry["sectionNumber"] = sis_class_entry["sequenceNumber"]
        class_entry["title"] = sis_class_entry["courseTitle"]
        class_entry["creditMin"] = sis_class_entry["creditHourLow"]
        class_entry["creditMax"] = sis_class_entry["creditHourHigh"]
        class_entry["seatsCapacity"] = sis_class_entry["maximumEnrollment"]
        class_entry["seatsRegistered"] = sis_class_entry["enrollment"]
        class_entry["seatsAvailable"] = sis_class_entry["seatsAvailable"]
        class_entry["waitlistCapacity"] = sis_class_entry["waitCapacity"]
        class_entry["waitlistRegistered"] = sis_class_entry["waitCount"]
        class_entry["waitlistAvailable"] = sis_class_entry["waitAvailable"]
    else:
        class_entry["sectionNumber"] = details_data["sectionNumber"]
        class_entry["title"] = details_data["title"]
        class_entry["creditMin"] = details_data["creditMin"]
        class_entry["creditMax"] = details_data["creditMax"]
        class_entry["seatsCapacity"] = enrollment_data["enrollmentCapacity"]
        class_entry["seatsRegistered"] = enrollment_data["enrollmentActual"]
        class_entry["seatsAvailable"] = enrollment_data["enrollmentAvailable"]
        class_entry["waitlistCapacity"] = enrollment_data["waitlistCapacity"]
        class_entry["waitlistRegistered"] = enrollment_data["waitlistActual"]
        class_entry["waitlistAvailable"] = enrollment_data["waitlistAvailable"]

    # Return the subject description, course number, and populated class entry
    return subject_desc, course_num, class_entry


async def resolve_hidden_classes(
    term: str,
    term_course_data: dict[str, dict[str, Any]],
    term_crn_set: set[str],
    semaphore: asyncio.Semaphore,
    tcp_connector: aiohttp.TCPConnector,
    timeout: int,
) -> list[tuple[str, str, dict[str, Any]]]:
    """
    Checks all crosslist CRNs in the term course data for any hidden classes not shown in
    the main class search and fetches their details to add to the term course data.

    @param term: Term code to fetch hidden classes for.
    @param term_course_data: The current term course data to check for hidden classes.
    @param term_crn_set: Set of all CRNs processed in the term.
    @param semaphore: Semaphore to limit number of concurrent sessions between
        multiple calls to this function.
    @param tcp_connector: TCP connector to use for client sessions.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: List of tuples containing subject description, course number, and
        class entry data for each hidden class found.
    """
    async with semaphore:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(
            connector=tcp_connector, connector_owner=False, timeout=timeout_obj
        ) as session:
            hidden_crns = {
                crosslist["courseReferenceNumber"]
                for subject in term_course_data.values()
                for course in subject["courses"].values()
                for class_entry in course
                for crosslist in class_entry["crosslists"]
                if crosslist["courseReferenceNumber"] not in term_crn_set
            }
            if len(hidden_crns) > 0:
                hidden_class_tasks = []
                async with asyncio.TaskGroup() as tg:
                    for crn in hidden_crns:
                        task = tg.create_task(
                            process_class_details(
                                session,
                                term_crn_set,
                                term=term,
                                crn=crn,
                            )
                        )
                        hidden_class_tasks.append(task)
                        logger.info(
                            f"Processing hidden class with CRN {crn} in term {term}"
                        )
                return [task.result() for task in hidden_class_tasks]
        return []


async def get_subject_course_data(
    term: str,
    subject_code: str,
    term_crn_set: set[str],
    semaphore: asyncio.Semaphore = None,
    tcp_connector: aiohttp.TCPConnector = None,
    timeout: int = 30,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term and subject.

    This function spawns its own client session to avoid session state conflicts with
    other subjects that may be processing concurrently.

    @param term: Term code to fetch data for.
    @param subject_code: Subject code to fetch data for, e.g. "CSCI".
    @param term_crn_set: Set of all CRNs processed in the term.
    @param semaphore: Semaphore to limit number of concurrent sessions between
        multiple calls to this function.
    @param tcp_connector: Optional TCP connector to use for the session. If not
        provided, the session will use a default connector.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: Dictionary of course data keyed by course code.
    """
    # Create default semaphore if not provided
    if semaphore is None:
        semaphore = asyncio.Semaphore(1)

    async with semaphore:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(
            connector=tcp_connector, connector_owner=False, timeout=timeout_obj
        ) as session:
            try:
                # Reset search state on server before fetching class data
                await reset_class_search(session, term)
                sis_class_data = await class_search(session, term, subject_code)
                if len(sis_class_data) == 0:
                    logger.info(
                        f"No classes found for subject {subject_code} in term {term}"
                    )
                    return {}

                # Process class entries from the class search in parallel
                tasks: list[asyncio.Task] = []
                async with asyncio.TaskGroup() as tg:
                    for sis_class_entry in sis_class_data:
                        task = tg.create_task(
                            process_class_details(
                                session,
                                term_crn_set,
                                sis_class_entry,
                            )
                        )
                        tasks.append(task)

                # Build subject course data
                subj_course_data = {}
                for task in tasks:
                    result = task.result()
                    if result:
                        _, course_num, class_entry = result
                        if course_num not in subj_course_data:
                            subj_course_data[course_num] = []
                        subj_course_data[course_num].append(class_entry)

                # Sort class entries by section number
                for course_num in subj_course_data:
                    subj_course_data[course_num] = sorted(
                        subj_course_data[course_num],
                        key=lambda class_entry: class_entry["sectionNumber"],
                    )

                # Return data sorted by course code
                return dict(sorted(subj_course_data.items()))

            except Exception as e:
                raise RuntimeError(
                    f"Error fetching course data for subject {subject_code} "
                    f"in term {term}"
                ) from e


async def get_term_course_data(
    term: str,
    output_path: Path | str,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    tcp_connector: aiohttp.TCPConnector = None,
    timeout: int = 30,
) -> bool:
    """
    Gets all course data for a given term, which includes all subjects in the
    term.

    This function spawns a client session for each subject to be processed in the
    term. Writes data as JSON after all subjects in the term have been processed.

    @param term: Term code to fetch data for.
    @param output_path: Path to write term course data JSON file to.
    @param semaphore: Semaphore to limit number of concurrent sessions.
    @param tcp_connector: Optional TCP connector to use for all sessions. If not
        provided, sessions will use default connectors.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: True on success, False on any unhandled failure.
    """
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    try:
        async with aiohttp.ClientSession(
            connector=tcp_connector, connector_owner=False, timeout=timeout_obj
        ) as session:
            subjects = await get_term_subjects(session, term)

        if len(subjects) == 0:
            logger.info(f"No subjects found for term {term}")
            return False
        logger.info(f"Processing {len(subjects)} subjects for term {term}")

        # Stores all course data for the term
        term_course_data = {}
        # Stores all CRNs for the term
        term_crn_set = set()

        # Process subjects in parallel, each with its own session
        tasks: list[asyncio.Task] = []
        async with asyncio.TaskGroup() as tg:
            for subject in subjects:
                subject_code = subject["code"]
                subject_desc = subject["description"]
                term_course_data[subject_desc] = {
                    "subjectCode": subject_code,
                    "courses": {},
                }
                task = tg.create_task(
                    get_subject_course_data(
                        term,
                        subject_code,
                        term_crn_set,
                        semaphore=semaphore,
                        tcp_connector=tcp_connector,
                        timeout=timeout,
                    )
                )
                tasks.append(task)

        # Wait for all tasks to complete and gather results
        for i, subject in enumerate(subjects):
            course_data = tasks[i].result()
            term_course_data[subject["description"]]["courses"] = course_data

        # Stop if no course data was fetched for the term
        # Likely indicates a scraper error since every valid term should have some data
        if len(term_course_data) == 0:
            logger.warning(f"No course data found for term {term}")
            return False

        # Check for hidden classes via crosslists and add them to term course data
        hidden_classes = await resolve_hidden_classes(
            term, term_course_data, term_crn_set, semaphore, tcp_connector, timeout
        )
        for subject_desc, course_num, class_entry in hidden_classes:
            if subject_desc in term_course_data:
                subj_course_data = term_course_data[subject_desc]["courses"]
                if course_num not in subj_course_data:
                    subj_course_data[course_num] = []
                subj_course_data[course_num].append(class_entry)
            else:
                logger.warning(
                    f"Subject {subject_desc} not found in term_course_data "
                    f"for CRN {class_entry['courseReferenceNumber']}"
                )

        # Convert term course data to be keyed by subject code instead of description
        term_course_data_by_code = {}
        for subject_desc, data in term_course_data.items():
            subject_code = data["subjectCode"]
            term_course_data_by_code[subject_code] = {
                "subjectDescription": subject_desc,
                **data,
            }
            # Remove redundant subject code entry
            del term_course_data_by_code[subject_code]["subjectCode"]
        term_course_data = term_course_data_by_code

        # Write all term data to JSON file
        write_json(term_course_data, output_path)

    except Exception as e:
        logger.error(
            f"Error processing term {term}, aborting term: {e}\n{traceback.format_exc()}"
        )
        return False

    return True


async def main(
    output_data_dir: Path | str,
    start_year: int = 1998,
    end_year: int = dt.datetime.now().year,
    seasons: list[str] | None = None,
    max_concurrent_sessions: int = 25,
    limit_per_host: int = 75,
    timeout: int = 30,
) -> bool:
    """
    Runs the SIS scraper for the specified range of years and seasons. The
    earliest available term is Summer 1998 (199805).

    Spawns multiple client sessions to process subjects in parallel, with each
    session responsible for processing one subject.

    @param output_data_dir: Directory to write term course data JSON files to.
    @param start_year: Starting year (inclusive) to scrape data for. Defaults
        to 1998.
    @param end_year: Ending year (inclusive) to scrape data for. Defaults to
        current year.
    @param seasons: List of academic seasons to scrape data for. Can be any
        combination of "spring", "summer", and "fall". If not specified, all
        three seasons will be processed.
    @param max_concurrent_sessions: Maximum number of concurrent client sessions to
        spawn.
    @param limit_per_host: Maximum number of simultaneous connections a session
        can make to the SIS server.
    @param timeout: Timeout in seconds for all requests made by a session.
    @return: True on success, False on any unhandled failure.
    """

    if output_data_dir is None:
        logger.fatal("No data output directory specified")
        return False

    start_time = time.time()

    if seasons is None:
        seasons = ["spring", "summer", "fall"]

    # Limit concurrent client sessions and simultaneous connections
    semaphore = asyncio.Semaphore(max_concurrent_sessions)

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
            ttl_dns_cache=500,
            limit_per_host=limit_per_host,
            keepalive_timeout=60,
            force_close=False,
        ) as tcp_connector:
            # Process terms in parallel
            async with asyncio.TaskGroup() as tg:
                for year in range(start_year, end_year + 1):
                    for season in set(seasons):
                        term = get_term_code(year, season)
                        if term == "":
                            continue
                        output_path = Path(output_data_dir) / f"{term}.json"
                        task = tg.create_task(
                            get_term_course_data(
                                term,
                                output_path=output_path,
                                semaphore=semaphore,
                                tcp_connector=tcp_connector,
                                timeout=timeout,
                            )
                        )
                        tasks.append(task)

            # Wait for all tasks to complete
            for task in tasks:
                # Count number of successfully processed terms
                success = task.result()
                if success:
                    num_terms_processed += 1

    except Exception as e:
        logger.fatal(f"Fatal error in SIS scraper: {e}\n{traceback.format_exc()}")
        return False

    end_time = time.time()
    logger.info("SIS scraper completed")
    logger.info(f"  Terms processed: {num_terms_processed}")
    logger.info(f"  Time elapsed: {end_time - start_time:.2f} seconds")

    return True
