import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class CodeMapper:
    def __init__(
        self,
        attribute_path: Path | str,
        instructor_path: Path | str,
        restriction_path: Path | str,
        subject_path: Path | str,
    ) -> None:
        self.attribute_path = Path(attribute_path)
        self.instructor_path = Path(instructor_path)
        self.restriction_path = Path(restriction_path)
        self.subject_path = Path(subject_path)

        self.attributes = self._load_json(self.attribute_path)
        self.instructors = self._load_json(self.instructor_path)
        self.restrictions = self._load_json(self.restriction_path)
        self._normalize_restrictions()
        self.subjects = self._load_json(self.subject_path)

        # Reverse map for subject name to code lookup
        self.subject_name_to_code = {v: k for k, v in self.subjects.items()}

        # Reverse map for instructor name to RCSID lookup
        self.instructor_name_to_rcsid = {v: k for k, v in self.instructors.items()}

    def _normalize_restrictions(self) -> None:
        normalized = {}
        for r_type, codes in self.restrictions.items():
            target_type = r_type
            if r_type.startswith("not_"):
                target_type = r_type[4:]
            if target_type not in normalized:
                normalized[target_type] = {}
            for code, name in codes.items():
                normalized[target_type][code] = name.strip()
        self.restrictions = normalized

    def _load_json(self, path: Path | str) -> dict:
        path = Path(path)
        if path.exists() and not path.is_dir():
            try:
                with path.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from {path}: {e}")
        return {}

    def save(self) -> None:
        self._save_json(self.attribute_path, self.attributes)
        self._save_json(self.instructor_path, self.instructors)
        self._save_json(self.restriction_path, self.restrictions)
        self._save_json(self.subject_path, self.subjects)

    def _save_json(self, path: Path, data: dict) -> None:
        # Sort keys for consistent output
        sorted_data = dict(sorted(data.items()))
        # For nested dicts (restrictions), sort inner keys too
        if path == self.restriction_path:
            sorted_data = {k: dict(sorted(v.items())) for k, v in sorted_data.items()}

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(sorted_data, f, indent=4, ensure_ascii=False)

    def add_subject(self, code: str, name: str) -> None:
        if code in self.subjects and self.subjects[code] != name:
            logging.warning(
                f"Conflicting subject name for code {code}: "
                f"'{self.subjects[code]}' vs '{name}'"
            )
        # Update code to name mapping regardless of whether a conflict exists
        self.subjects[code] = name
        self.subject_name_to_code[name] = code

    def add_attribute(self, code: str, name: str) -> None:
        if code in self.attributes and self.attributes[code] != name:
            logging.warning(
                f"Conflicting attribute name for code {code}: "
                f"'{self.attributes[code]}' vs '{name}'"
            )
        # Update code to name mapping regardless of whether a conflict exists
        self.attributes[code] = name

    def add_restriction(self, r_type: str, code: str, name: str) -> None:
        if r_type.startswith("not_"):
            r_type = r_type[4:]
        if r_type not in self.restrictions:
            self.restrictions[r_type] = {}
        if (
            code in self.restrictions[r_type]
            and self.restrictions[r_type][code] != name
        ):
            logging.warning(
                f"Conflicting restriction name for type {r_type} code {code}: "
                f"'{self.restrictions[r_type][code]}' vs '{name}'"
            )
        # Update code to name mapping regardless of whether a conflict exists
        self.restrictions[r_type][code] = name.strip()

    def add_instructor(self, rcsid: str, name: str) -> None:
        if rcsid in self.instructors and self.instructors[rcsid] != name:
            logging.warning(
                f"Conflicting instructor name for RCSID {rcsid}: "
                f"'{self.instructors[rcsid]}' vs '{name}'"
            )
        # Update RCSID to name mapping regardless of whether a conflict exists
        self.instructors[rcsid] = name

    def get_subject_code(self, name: str) -> str | None:
        if name in self.subject_name_to_code:
            return self.subject_name_to_code[name]
        return None

    def get_or_generate_rcsid(self, name: str) -> str:
        # Check if name already maps to an RCSID (reverse lookup)
        if name in self.instructor_name_to_rcsid:
            return self.instructor_name_to_rcsid[name]
        # Otherwise, generate a new RCSID
        return self._generate_rcsid(name)

    def _generate_rcsid(self, instructor_name: str) -> str:
        instructor_name_pattern = r"(.+), (.+)"
        match = re.match(instructor_name_pattern, instructor_name)
        if match is None or len(match.groups()) != 2:
            logger.warning(f"Unexpected instructor name format: {instructor_name}")
            # Fallback: remove spaces and lowercase
            return re.sub(r"\s+", "", instructor_name).lower()[:8]
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
        # Ensure uniqueness against existing instructors
        counter = 1
        original_rcsid = rcsid
        while rcsid in self.instructors:
            # If the name matches, we can reuse this RCSID (handled in get_or_generate_rcsid)
            # But here we are generating a NEW one because we didn't find the name.
            # So we must ensure uniqueness.
            rcsid = f"{original_rcsid}{counter}"
            counter += 1
        return rcsid


def process_term(term: str, term_data: dict[str, Any], mapper: CodeMapper):
    for subject_code, subject_data in term_data.items():
        # Update Subject Map
        if "subjectDescription" in subject_data:
            mapper.add_subject(subject_code, subject_data["subjectDescription"])

        if "courses" not in subject_data:
            continue

        for _, class_list in subject_data["courses"].items():
            for class_entry in class_list:
                # Attributes
                if "attributes" in class_entry:
                    new_attributes = []
                    for attr in class_entry["attributes"]:
                        # Parse "Name  Code" (two spaces)
                        match = re.match(r"(.+)  (.+)", attr)
                        if match:
                            name, code = match.groups()
                            mapper.add_attribute(code, name.strip())
                            new_attributes.append(code)
                        else:
                            logger.warning(
                                f"Unexpected attribute format: '{attr}' "
                                f"for CRN {class_entry['courseReferenceNumber']} "
                                f"in term {term}"
                            )
                            new_attributes.append(attr)
                    class_entry["attributes"] = new_attributes

                # Restrictions
                if "restrictions" in class_entry:
                    for r_type, r_list in class_entry["restrictions"].items():
                        if r_type == "special_approval":
                            continue
                        new_r_list = []
                        for restriction in r_list:
                            # Parse "Name (Code)"
                            match = re.match(r"(.+)\s*\((.+)\)", restriction)
                            if match:
                                name, code = match.groups()
                                mapper.add_restriction(r_type, code, name.strip())
                                new_r_list.append(code)
                            else:
                                new_r_list.append(restriction)
                        class_entry["restrictions"][r_type] = new_r_list

                # Faculty
                if "faculty" in class_entry:
                    new_faculty = []
                    for faculty in class_entry["faculty"]:
                        name = faculty["displayName"]
                        email = faculty["emailAddress"]
                        rcsid = None
                        if email:
                            rcsid = email.split("@")[0]
                        if not rcsid and name:
                            rcsid = mapper.get_or_generate_rcsid(name)
                        if rcsid and name:
                            mapper.add_instructor(rcsid, name)
                        if rcsid:
                            new_faculty.append(rcsid)
                        elif name:
                            new_faculty.append(name)
                        else:
                            new_faculty.append(str(faculty))

                    class_entry["faculty"] = new_faculty

                # Crosslists & Corequisites
                for field in ["crosslists", "corequisites"]:
                    if field in class_entry:
                        new_list = []
                        for item in class_entry[field]:
                            subj_name = item["subjectName"]
                            course_num = item["courseNumber"]
                            subj_code = mapper.get_subject_code(subj_name)
                            new_list.append(f"{subj_code} {course_num}")
                        class_entry[field] = new_list


def main(
    output_data_dir: Path | str,
    processed_output_data_dir: Path | str,
    attribute_code_name_map_path: Path | str,
    instructor_rcsid_name_map_path: Path | str,
    restriction_code_name_map_path: Path | str,
    subject_code_name_map_path: Path | str,
) -> bool:
    """
    Runs post-processing on the raw output data from the SIS scraper. This includes
    codifying course codes, attributes, restrictions, and instructor RCSIDs,
    and updating the code mappings.
    """
    # Convert to Path objects
    output_data_dir = Path(output_data_dir)
    processed_output_data_dir = Path(processed_output_data_dir)
    attribute_code_name_map_path = Path(attribute_code_name_map_path)
    instructor_rcsid_name_map_path = Path(instructor_rcsid_name_map_path)
    restriction_code_name_map_path = Path(restriction_code_name_map_path)
    subject_code_name_map_path = Path(subject_code_name_map_path)

    if not output_data_dir.exists():
        logger.error(f"Output data directory {output_data_dir} does not exist.")
        return False

    # Initialize code mapper
    mapper = CodeMapper(
        attribute_code_name_map_path,
        instructor_rcsid_name_map_path,
        restriction_code_name_map_path,
        subject_code_name_map_path,
    )

    processed_output_data_dir.mkdir(exist_ok=True, parents=True)

    # Process each term course data file
    for term_file in output_data_dir.glob("*.json"):
        with term_file.open("r", encoding="utf-8") as f:
            term_course_data = json.load(f)

        process_term(term_file.stem, term_course_data, mapper)

        # Write processed data
        processed_file_path = processed_output_data_dir / term_file.name
        with processed_file_path.open("w", encoding="utf-8") as f:
            logger.info(f"Writing processed data to {processed_file_path}")
            json.dump(term_course_data, f, indent=4, ensure_ascii=False)

    # Save updated mappings
    num_attribute_codes = len(mapper.attributes)
    num_instructor_rcsids = len(mapper.instructors)
    num_restriction_codes = sum(len(codes) for codes in mapper.restrictions.values())
    num_subject_codes = len(mapper.subjects)
    logger.info(
        f"Saving {num_attribute_codes} attribute codes to "
        + str(attribute_code_name_map_path)
    )
    logger.info(
        f"Saving {num_instructor_rcsids} instructor RCSIDs to "
        + str(instructor_rcsid_name_map_path)
    )
    logger.info(
        f"Saving {num_restriction_codes} restriction codes to "
        + str(restriction_code_name_map_path)
    )
    logger.info(
        f"Saving {num_subject_codes} subject codes to "
        + str(subject_code_name_map_path)
    )
    mapper.save()

    return True
