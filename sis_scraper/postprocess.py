import json
import logging
import re
from pathlib import Path
from typing import Any


def codify_course_code(course_code: str, subject_code_name_map: dict[str, str]) -> str:
    course_pattern = r"(.+) (\d{4})"
    match = re.match(course_pattern, course_code)
    if match is None or len(match.groups()) != 2:
        logging.warning(f"Unexpected course code format: {course_code}")
        return course_code

    subject_name = match.group(1)
    course_number = match.group(2)
    # Translate subject_name (full name) back to its code using subject_code_name_map
    # subject_code_name_map: {code: name}
    # We need to find the code whose value matches subject_name
    code = next(
        (k for k, v in subject_code_name_map.items() if v == subject_name), subject_name
    )
    return f"{code} {course_number}"


def codify_attribute(attribute: str) -> str:
    attribute_pattern = r"(.+)  (.+)"
    match = re.match(attribute_pattern, attribute)
    if match is None or len(match.groups()) != 2:
        logging.warning(f"Unexpected attribute format: {attribute}")
        return attribute
    attribute_code = match.group(2)
    return attribute_code


def codify_restriction(restriction: str) -> str:
    restriction_pattern = r"(.+)\s*\((.+)\)"
    match = re.match(restriction_pattern, restriction)
    if match is None or len(match.groups()) != 2:
        logging.warning(f"Unexpected restriction format: {restriction}")
        return restriction
    restriction_code = match.group(2)
    return restriction_code


def generate_rcsid(
    instructor_name: str,
    instructor_rcsid_name_map: dict[str, str],
    generated_instructor_rcsid_name_map: dict[str, str],
) -> str:
    """
    Accepts an instructor name in the format `Last, First` and generates an RCSID.
    Assumes the instructor name does not have an associated RCSID in the SIS data.
    """
    instructor_name_pattern = r"(.+), (.+)"
    match = re.match(instructor_name_pattern, instructor_name)
    if match is None or len(match.groups()) != 2:
        logging.warning(f"Unexpected instructor name format: {instructor_name}")
        return instructor_name
    # An RCSID is composed of up to the first 5 letters of the last name, followed by
    # the first name initial, as well as a number if needed to ensure uniqueness.
    # For example, "Doe, John" would become "doej", or "doej2" if "doej" is taken.
    last_name = match.group(1)
    last_name_component = ""
    # Extract up to first 5 alphabetic characters from last name
    for char in last_name:
        if char.isalpha():
            last_name_component += char.lower()
        if len(last_name_component) == 5:
            break
    first_name = match.group(2)
    # Extract first alphabetic character from first name
    first_name_initial = ""
    for char in first_name:
        if char.isalpha():
            first_name_initial += char.lower()
            break
    rcsid = f"{last_name_component}{first_name_initial}"
    # Ensure uniqueness
    counter = 1
    while rcsid in instructor_rcsid_name_map:
        rcsid = f"{last_name_component}{first_name_initial}{counter}"
        counter += 1
    # The generated RCSID may already exist in the generated map, this is normal
    generated_instructor_rcsid_name_map[rcsid] = instructor_name
    return rcsid


def post_process(
    term_course_data: dict[str, Any],
    subject_code_name_map: dict[str, str],
    instructor_rcsid_name_map: dict[str, str],
    generated_instructor_rcsid_name_map: dict[str, str],
) -> None:
    for _, subject_data in term_course_data.items():
        subject_courses = subject_data["courses"]
        for _, course_data in subject_courses.items():
            course_detail = course_data["course_detail"]
            course_corequisites = course_detail["corequisite"]
            course_prerequisites = course_detail["prerequisite"]
            course_crosslists = course_detail["crosslist"]
            course_attributes = course_detail["attributes"]
            course_restriction_types = course_detail["restrictions"]
            course_sections = course_detail["sections"]

            # Corequisites
            for i, corequisite in enumerate(course_corequisites):
                course_corequisites[i] = codify_course_code(
                    corequisite, subject_code_name_map
                )

            # Prerequisites
            # Will implement when prerequisite parsing is done
            # for i, prerequisite in enumerate(course_prerequisites):
            #     pass

            # Crosslists
            for i, crosslist in enumerate(course_crosslists):
                course_crosslists[i] = codify_course_code(
                    crosslist, subject_code_name_map
                )

            # Attributes
            for i, attribute in enumerate(course_attributes):
                course_attributes[i] = codify_attribute(attribute)

            # Restrictions
            for restriction_type in course_restriction_types:
                restriction_type_list = course_restriction_types[restriction_type]
                for i, restriction in enumerate(restriction_type_list):
                    restriction_type_list[i] = codify_restriction(restriction)

            # Instructors
            for section in course_sections:
                instructor_list = section["instructor"]
                instructor_pattern = r"(.+), (.+) \((.+)\)"
                for i, instructor in enumerate(instructor_list):
                    match = re.match(instructor_pattern, instructor)
                    if match is None or len(match.groups()) != 3:
                        logging.warning(
                            f"Unexpected instructor name and RCSID format: {instructor}"
                        )
                        continue
                    instructor_name = f"{match.group(1)}, {match.group(2)}"
                    instructor_rcsid = match.group(3)
                    if instructor_rcsid == "Unknown RCSID":
                        instructor_rcsid = generate_rcsid(
                            instructor_name,
                            instructor_rcsid_name_map,
                            generated_instructor_rcsid_name_map,
                        )
                    instructor_list[i] = instructor_rcsid


def main(
    output_data_dir: Path | str,
    processed_output_data_dir: Path | str,
    attribute_code_name_map_path: Path | str,
    instructor_rcsid_name_map_path: Path | str,
    restriction_code_name_map_path: Path | str,
    subject_code_name_map_path: Path | str,
) -> bool:
    # Validate input directories
    if not all(
        (
            output_data_dir,
            processed_output_data_dir,
            attribute_code_name_map_path,
            instructor_rcsid_name_map_path,
            restriction_code_name_map_path,
            subject_code_name_map_path,
        )
    ):
        logging.error("One or more required directories are not specified.")
        return False

    # Convert to Path objects if necessary
    if isinstance(output_data_dir, str):
        output_data_dir = Path(output_data_dir)
    if isinstance(processed_output_data_dir, str):
        processed_output_data_dir = Path(processed_output_data_dir)
    if isinstance(attribute_code_name_map_path, str):
        attribute_code_name_map_path = Path(attribute_code_name_map_path)
    if isinstance(instructor_rcsid_name_map_path, str):
        instructor_rcsid_name_map_path = Path(instructor_rcsid_name_map_path)
    if isinstance(restriction_code_name_map_path, str):
        restriction_code_name_map_path = Path(restriction_code_name_map_path)
    if isinstance(subject_code_name_map_path, str):
        subject_code_name_map_path = Path(subject_code_name_map_path)

    # Validate input directories
    if not output_data_dir.exists() or not output_data_dir.is_dir():
        logging.error(f"Output data directory {output_data_dir} does not exist.")
        return False

    # Validate mapping files
    for map_path in [
        attribute_code_name_map_path,
        instructor_rcsid_name_map_path,
        restriction_code_name_map_path,
        subject_code_name_map_path,
    ]:
        if not map_path.exists() or map_path.is_dir():
            logging.error(f"Mapping file {map_path} does not exist or is a directory.")
            return False

    # Load code mappings
    with instructor_rcsid_name_map_path.open("r", encoding="utf-8") as f:
        instructor_rcsid_name_map = json.load(f)
    with subject_code_name_map_path.open("r", encoding="utf-8") as f:
        subject_code_name_map = json.load(f)

    # Initialize generated instructor RCSID map
    generated_instructor_rcsid_name_map = {}

    processed_output_data_dir.mkdir(exist_ok=True, parents=True)

    # Process each term course data file
    for term_file in output_data_dir.glob("*.json"):
        with term_file.open("r", encoding="utf-8") as f:
            term_course_data = json.load(f)

        post_process(
            term_course_data,
            subject_code_name_map,
            instructor_rcsid_name_map,
            generated_instructor_rcsid_name_map,
        )

        # Write processed data
        processed_file_path = processed_output_data_dir / term_file.name
        logging.info(f"Writing processed data to {processed_file_path}")
        with processed_file_path.open("w", encoding="utf-8") as f:
            json.dump(term_course_data, f, indent=4, ensure_ascii=False)

    # Write generated instructor RCSID map
    if len(generated_instructor_rcsid_name_map) > 0:
        generated_map_path = (
            instructor_rcsid_name_map_path.parent
            / "generated_instructor_rcsid_name_map.json"
        )
        logging.info(
            f"Writing {len(generated_instructor_rcsid_name_map)} generated instructor RCSID mappings to {generated_map_path}"
        )
        with generated_map_path.open("w", encoding="utf-8") as f:
            json.dump(
                generated_instructor_rcsid_name_map, f, indent=4, ensure_ascii=False
            )

    return True
