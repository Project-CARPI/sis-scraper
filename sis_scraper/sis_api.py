import asyncio
import html
import json
import logging
import re
from enum import Enum
from typing import Any

import aiohttp
import bs4
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

# Restriction data structure is dynamically generated based on values in this dictionary,
# except for "Special Approvals", which is handled explicitly as a special case.
_RESTRICTION_TYPE_MAP = {
    "Majors": "major",
    "Fields of Study (Major, Minor or Concentration)": "major",
    "Minors": "minor",
    "Levels": "level",
    "Classes": "classification",
    "Degrees": "degree",
    "Programs": "degree",
    "Departments": "department",
    "Campuses": "campus",
    "Colleges": "college",
    "Special Approvals": "special_approval",
}

_BASE_URL = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/"


class ClassColumn(str, Enum):
    COURSE_TITLE = "courseTitle"
    SUBJECT_DESCRIPTION = "subjectDescription"
    COURSE_NUMBER = "courseNumber"
    SECTION = "sequenceNumber"
    CRN = "courseReferenceNumber"
    TERM = "term"


def html_unescape(obj: Any) -> Any:
    """
    Recursively unescape HTML entities in all string values within a complex
    structure (dicts, lists, tuples, sets). Dictionary keys are unescaped too.
    """
    if isinstance(obj, str):
        return html.unescape(obj)
    if isinstance(obj, dict):
        return {html_unescape(k): html_unescape(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [html_unescape(i) for i in obj]
    if isinstance(obj, tuple):
        return tuple(html_unescape(i) for i in obj)
    if isinstance(obj, set):
        return {html_unescape(i) for i in obj}
    return obj


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1.5) + wait_random(min=0, max=2),
    retry=retry_if_exception_type((asyncio.TimeoutError, aiohttp.ClientError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Retrying failed request (attempt {retry_state.attempt_number})"
    ),
)
async def retry_get(
    session: aiohttp.ClientSession, url: str, params: dict[str, Any]
) -> Any:
    """
    Helper function to perform an HTTP GET request and return the raw text
    response, retrying up to 3 times on failure using exponential backoff and
    jitter.

    @param session: An aiohttp ClientSession to use for the request.
    @param url: The URL to send the GET request to.
    @param params: A dictionary of query parameters to include in the request.
    @return: The raw text response from the server.
    """
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    return raw_data


async def get_term_subjects(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of subjects and codes for a given term from SIS. If the
    term is invalid or doesn't exist, returns an empty list.

    Returned data format is as follows:
    ```
    [
        {
            "code": "ADMN",
            "description": "Administrative Courses"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "classSearch/get_subject"
    params = {"term": term, "offset": 1, "max": 2147483647}
    raw_data = raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_term_instructors(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of instructors for a given term from SIS. If the term is
    invalid or doesn't exist, returns an empty list.

    Returned data format is as follows:
    ```
    [
        {
            "code": "12345",
            "description": "Last, First"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "classSearch/get_instructor"
    params = {"term": term, "offset": 1, "max": 2147483647}
    raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_attributes(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of attributes from SIS.

    Note that this is not actually a comprehensive list of all attributes used
    by courses. For example, "FRSH" and "ONLI" are known attributes that are
    missing from this list.

    Returned data format is as follows:
    ```
    [
        {
            "code": "COMM",
            "description": "Communication Intensive"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "classSearch/get_attribute"
    params = {"searchTerm": search_term, "offset": 1, "max": 2147483647}
    raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_colleges(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of colleges (schools) and codes from SIS. Not to be
    confused with campuses.

    Returned data format is as follows:
    ```
    [
        {
            "code": "S",
            "description": "School of Science"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "classSearch/get_college"
    params = {"searchTerm": search_term, "offset": 1, "max": 2147483647}
    raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_campuses(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of campuses and codes from SIS. Not to be confused
    with colleges (schools).

    Returned data format is as follows:
    ```
    [
        {
            "code": "T",
            "description": "Troy"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "classSearch/get_campus"
    params = {"searchTerm": search_term}
    raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def reset_class_search(session: aiohttp.ClientSession, term: str) -> None:
    """
    Resets the term and subject search state on the SIS server.

    Must be called before each attempt to fetch classes from a subject in the
    given term. Otherwise, the server will continue returning the same results
    from the last subject accessed, or no data if attempting to access data
    from a different term.
    """
    url = _BASE_URL + "term/search"
    params = {"mode": "search", "term": term}
    await retry_get(session, url, params)


async def class_search(
    session: aiohttp.ClientSession,
    term: str,
    subject: str,
    max_size: int = 2147483647,
    sort_column: ClassColumn = ClassColumn.SUBJECT_DESCRIPTION,
    sort_asc: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetches the list of classes for a given subject and term from SIS.

    The term and subject search state on the SIS server must be reset before
    each call to this function.

    Returned data format is very large; see docs for details.
    """
    url = _BASE_URL + "searchResults/searchResults"
    params = {
        "pageOffset": 0,
        "txt_subject": subject,
        "txt_term": term,
        "pageMaxSize": max_size,
        "sortColumn": sort_column,
        "sortDirection": "asc" if sort_asc else "desc",
    }
    raw_data = await retry_get(session, url, params)
    data = json.loads(raw_data)
    data = html_unescape(data)
    course_data = data["data"]
    if course_data is None:
        return []
    return course_data


async def get_class_description(
    session: aiohttp.ClientSession, term: str, crn: str
) -> str:
    """
    Fetches and parses data from the "Course Description" tab of a class
    details page.

    Returns a string containing the course description, without any
    additional fields such as "When Offered", "Credit Hours", "Prerequisite",
    etc.
    """
    url = _BASE_URL + "searchResults/getCourseDescription"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    description_tag = soup.find("section", {"aria-labelledby": "courseDescription"})
    if description_tag is None:
        logger.warning(f"No description found for term and CRN: {term} - {crn}")
        return ""
    description_text_list = [
        text.strip() for text in description_tag.get_text(separator="\n").split("\n")
    ]
    for text in description_text_list:
        if text != "":
            return text


async def get_class_attributes(
    session: aiohttp.ClientSession, term: str, crn: str
) -> list[str]:
    """
    Fetches and parses data from the "Attributes" tab of a class details page.

    Returned data format is as follows:
    ```
    [
        "Communication Intensive  COMM",
        "Data Intensive I  DI1",
        "Introductory Level Course  FRSH",
        ...
    ]
    ```
    """
    url = _BASE_URL + "searchResults/getSectionAttributes"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    attributes = []
    attribute_tags = soup.find_all("span", {"class": "attribute-text"})
    for tag in attribute_tags:
        attributes.append(tag.text.strip())
    return attributes


async def get_class_restrictions(session: aiohttp.ClientSession, term: str, crn: str):
    """
    Fetches and parses data from the "Restrictions" tab of a class details
    page.

    Returned data format is as follows:
    ```
    {
        "major": [
            "Architecture (ARCH)",
            ...
        ],
        "not_major": [
            "Computer Science (CSCI)",
            ...
        ],
        "classification": [
            "Freshman (FR)",
            ...
        ],
        "not_classification": [
            "Senior (SR)",
            ...
        ],
        ...
    }
    ```
    """
    url = _BASE_URL + "searchResults/getRestrictions"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    # Dynamically build dict structure from RESTRICTION_TYPE_MAP values
    restrictions_data = {}
    bases = set(_RESTRICTION_TYPE_MAP.values())
    for base in sorted(bases):
        restrictions_data[base] = []
        # There is no "not_" key for "special_approval"
        if base == "special_approval":
            continue
        restrictions_data[f"not_{base}"] = []
    restrictions_tag = soup.find("section", {"aria-labelledby": "restrictions"})
    escaped_keys = [re.escape(key) for key in _RESTRICTION_TYPE_MAP.keys()]
    restriction_header_pattern = (
        r"(Must|Cannot) be enrolled in one of the following "
        f"({'|'.join(escaped_keys)}):"
    )
    # "Special Approvals:" is the only other known header pattern
    special_approvals_pattern = r"Special Approvals:"
    # All known children of the restrictions section are <div>, <span<>, or <br> tags
    # Tags relevant to restrictions are only known to be <span> tags
    restrictions_content = [
        child
        for child in restrictions_tag.children
        if isinstance(child, bs4.element.Tag) and child.name == "span"
    ]
    i = 0
    while i < len(restrictions_content):
        content = restrictions_content[i]
        if content.string is None:
            logger.warning(
                "Skipping unexpected restriction content with no string for "
                f"CRN {crn} in term {term}"
            )
            i += 1
            continue
        content_string = content.string.strip()
        header_match = re.match(restriction_header_pattern, content_string) or re.match(
            special_approvals_pattern, content_string
        )
        if header_match is None:
            i += 1
            continue
        # Special case for "Special Approvals:"
        if len(header_match.groups()) == 0:
            if "special_approval" not in restrictions_data:
                i += 1
                continue
            restriction_list = restrictions_data["special_approval"]
        else:
            must_or_cannot, type_plural = header_match.groups()
            if type_plural not in _RESTRICTION_TYPE_MAP:
                logger.warning(
                    f"Skipping unknown restriction type '{type_plural}' for CRN {crn} "
                    f"in term {term}"
                )
                i += 1
                continue
            key_base = _RESTRICTION_TYPE_MAP[type_plural]
            key = f"not_{key_base}" if must_or_cannot.lower() == "cannot" else key_base
            restriction_list = restrictions_data[key]
        i += 1
        next_content_string = ""
        while i < len(restrictions_content):
            next_content = restrictions_content[i]
            if next_content.string is None:
                logger.warning(
                    f"Skipping unexpected restriction content with no string for "
                    f"CRN {crn} in term {term}"
                )
                i += 1
                continue
            # SIS separates one restriction item into multiple <span> tags if it contains
            # commas, so a restriction item is only complete when parentheses are closed.
            #
            # For example, a restriction item "Communication, Media, & Design (COMD)"
            # would be split into three <span> tags:
            #
            # <span>Communication</span>
            # <span>Media</span>
            # <span> & Design (COMD)</span>
            if len(next_content_string) == 0:
                next_content_string = next_content.string.lstrip()
            else:
                next_content_string += f",{next_content.string}"
            # Stop if another restriction header is encountered
            if re.match(restriction_header_pattern, next_content_string) or re.match(
                special_approvals_pattern, next_content_string
            ):
                break
            if restriction_list is restrictions_data["special_approval"] or re.match(
                r".*\(.*\)", next_content_string
            ):
                restriction_list.append(next_content_string.strip())
                next_content_string = ""
            i += 1
    return restrictions_data


async def get_class_prerequisites(
    session: aiohttp.ClientSession, term: str, crn: str
) -> dict[str, Any]:
    """
    Fetches and parses data from the "Prerequisites" tab of a class details
    page.

    Returned data format is as follows:
    ```
    {
        "id": 0,
        "type": "and",
        "values": [
            {
                "id": 1,
                "type": "or",
                "values": [
                    "Physics 1200",
                    ...
                ]
            },
            "Computer Science 1100",
            "Mathematics 1010"
        ]
    }
    ```
    """
    url = _BASE_URL + "searchResults/getSectionPrerequisites"
    params = {"term": term, "courseReferenceNumber": crn}
    # async with session.get(url, params=params) as response:
    #     response.raise_for_status()
    #     text = await response.text()
    # text = html.unescape(text)
    # soup = bs4.BeautifulSoup(text, "html5lib")
    # data = ""
    # rows = soup.find_all("tr")
    # for row in rows:
    #     cols = row.find_all("td")
    #     if len(cols) == 0:
    #         continue
    #     data += (
    #         " and " if cols[0].text == "And" else " or " if cols[0].text == "Or" else ""
    #     )
    #     data += " ( " if cols[1].text != "" else ""
    #     if cols[2].text != "":
    #         data += f" {cols[2].text} {cols[3].text} "
    #     else:
    #         data += f" {cols[4].text} {cols[5].text} "
    #     data += " ) " if cols[8].text != "" else ""
    #     data = data.replace("  ", " ").strip()
    #     data = data.replace("  ", " ").strip()
    #     data = data.replace("( ", "(").strip()
    #     data = data.replace(" )", ")").strip()
    # if data:
    #     try:
    #         return parse_prereq(term, crn, data)
    #     except Exception as e:
    #         logger.error(
    #             f"Error parsing prerequisites for CRN {crn} in term {term} "
    #             f"with data: {data}\n{e}"
    #         )
    #         import traceback

    #         traceback.print_exc()
    return {}


async def get_class_corequisites(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
):
    """
    Fetches and parses data from the "Corequisites" tab of a class details
    page.

    Returned data format is as follows:
    ```
    [
        {
            "subjectName": "Computer Science",
            "courseNumber": "1100",
            "title": "COMPUTER SCIENCE I"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "searchResults/getCorequisites"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    coreqs_tag = soup.find("section", {"aria-labelledby": "coReqs"})
    coreqs_table = coreqs_tag.find("table", {"class": "basePreqTable"})
    if coreqs_table is None:
        return []
    coreqs_thead = coreqs_table.thead
    coreqs_tbody = coreqs_table.tbody
    if not coreqs_thead or not coreqs_tbody:
        return []
    thead_cols = [th.text.strip() for th in coreqs_thead.find_all("th")]
    # Known corequisite columns are Subject, Course Number, and Title
    if len(thead_cols) != 3:
        logger.warning(
            f"Unexpected number of corequisite columns for CRN {crn} in term {term}"
        )
        return []
    coreqs = []
    for tr in coreqs_tbody.find_all("tr"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if len(cols) != len(thead_cols):
            logger.warning(
                f"Skipping unexpected corequisite row with mismatched columns for "
                f"CRN {crn} in term {term}"
            )
            continue
        subject, course_num, title = cols
        coreqs.append(
            {"subjectName": subject, "courseNumber": course_num, "title": title}
        )
    return coreqs


async def get_class_crosslists(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
) -> list[dict[str, Any]]:
    """
    Fetches and parses data from the "Cross Listed" tab of a class details
    page.

    Returned data format is as follows:
    ```
    [
        {
            "courseReferenceNumber": "12345",
            "subjectName": "Computer Science",
            "courseNumber": "1100",
            "title": "COMPUTER SCIENCE I",
            "sectionNumber": "01"
        },
        ...
    ]
    ```
    """
    url = _BASE_URL + "searchResults/getXlstSections"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    crosslists_tag = soup.find("section", {"aria-labelledby": "xlstSections"})
    crosslists_table = crosslists_tag.table
    if crosslists_table is None:
        return []
    crosslists_thead = crosslists_table.thead
    crosslists_tbody = crosslists_table.tbody
    if not crosslists_thead or not crosslists_tbody:
        return []
    thead_cols = [th.text.strip() for th in crosslists_thead.find_all("th")]
    # Known crosslist columns are CRN, Subject, Course Number, Title, and Section
    if len(thead_cols) != 5:
        logger.warning(
            f"Unexpected number of crosslist columns for term and CRN: {term} - {crn}"
        )
        return []
    crosslists = []
    for tr in crosslists_tbody.find_all("tr"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if len(cols) != len(thead_cols):
            logger.warning(
                f"Skipping unexpected crosslist row with mismatched columns for "
                f"CRN {crn} in term {term}"
            )
            continue
        crn, subject, course_num, title, section_num = cols
        crosslists.append(
            {
                "courseReferenceNumber": crn,
                "subjectName": subject,
                "courseNumber": course_num,
                "title": title,
                "sectionNumber": section_num,
            }
        )
    return crosslists


async def get_class_meetings(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
) -> list[dict[str, Any]]:
    """
    Fetches and parses data from the "Instructor/Meeting Times" tab of a class details
    page.

    Returned data format is as follows:
    [
        {
            "beginTime": "0800",
            "endTime": "0950",
            "creditHours": 4,
            "campusCode": "T",
            "campusDescription": "Troy",
            "buildingCode": "SAGE",
            "buildingDescription": "Russell Sage Laboratory",
            "category": "L",
            "room": "303",
            "startDate": "01/15/2024",
            "endDate": "05/01/2024",
            "days": ["M", "W", "F"]
        },
        ...
    ]
    """
    url = _BASE_URL + "searchResults/getFacultyMeetingTimes"
    params = {"term": term, "courseReferenceNumber": crn}
    raw_data = await retry_get(session, url, params)
    json_data = json.loads(raw_data)
    json_data = html_unescape(json_data)
    sis_meetings_list = json_data["fmt"]
    meetings_list = _process_class_meetings(sis_meetings_list)
    return meetings_list


def _process_class_meetings(
    sis_meetings_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Processes raw class meeting data from SIS into a more usable format.

    Returned data format is as follows:
    [
        {
            "beginTime": "0800",
            "endTime": "0950",
            "creditHours": 4,
            "campusCode": "T",
            "campusDescription": "Troy",
            "buildingCode": "SAGE",
            "buildingDescription": "Russell Sage Laboratory",
            "category": "L",
            "room": "303",
            "startDate": "01/15/2024",
            "endDate": "05/01/2024",
            "days": ["M", "W", "F"]
        },
        ...
    ]
    """
    meetings_list = []
    day_codes = {
        "sunday": "U",
        "monday": "M",
        "tuesday": "T",
        "wednesday": "W",
        "thursday": "R",
        "friday": "F",
        "saturday": "S",
    }
    for meeting in sis_meetings_list:
        sis_meeting_info = meeting["meetingTime"]
        meeting_info = {
            "beginTime": sis_meeting_info["beginTime"],
            "endTime": sis_meeting_info["endTime"],
            "creditHours": sis_meeting_info["creditHourSession"],
            "campusCode": sis_meeting_info["campus"],
            "campusDescription": sis_meeting_info["campusDescription"],
            "buildingCode": sis_meeting_info["building"],
            "buildingDescription": sis_meeting_info["buildingDescription"],
            "category": sis_meeting_info["category"],
            "room": sis_meeting_info["room"],
            "startDate": sis_meeting_info["startDate"],
            "endDate": sis_meeting_info["endDate"],
            "days": [],
        }
        for day in day_codes:
            if sis_meeting_info[day]:
                meeting_info["days"].append(day_codes[day])
        meetings_list.append(meeting_info)
    return meetings_list
