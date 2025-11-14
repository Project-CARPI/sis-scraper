import json
from pathlib import Path

import carpi_data_model.models as models
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

_engine: Engine = None
_session_factory: sessionmaker = None


class SemesterAgnosticData:
    def __init__(self):
        self.course = []
        self.course_attribute = []
        self.course_relationship = []
        self.course_restriction = []


class SemesterSpecificData:
    def __init__(self):
        self.course_offering = []
        self.course_faculty = []


def init_db_connection(
    db_dialect: str,
    db_api: str,
    db_hostname: str,
    db_username: str,
    db_password: str,
    db_schema: str,
    echo: bool = False,
) -> tuple[Engine, sessionmaker]:
    global _engine, _session_factory
    if _engine is None:
        _engine = create_engine(
            f"{db_dialect}+{db_api}://{db_username}:{db_password}"
            f"@{db_hostname}/{db_schema}",
            echo=echo,
        )
    if _session_factory is None:
        _session_factory = sessionmaker(bind=_engine)
    return _engine, _session_factory


def generate_schema(engine: Engine) -> None:
    models.Base.metadata.create_all(engine)


def drop_all_tables(engine: Engine) -> None:
    models.Base.metadata.drop_all(engine)


def extract_semester_info_from_filename(json_path: Path) -> tuple[int, str]:
    stem = json_path.stem
    year = int(stem[:4])
    semester_code = stem[4:]
    match semester_code:
        case "01":
            semester = "SPRING"
        case "05":
            semester = "SUMMER"
        case "09":
            semester = "FALL"
    return year, semester


def get_subjects_from_json(json_path: Path) -> list[models.Attribute]:
    with open(json_path, "r", encoding="utf-8") as f:
        subject_data = json.load(f)
    return [
        models.Subject(
            subj_code=code,
            title=description,
        )
        for code, description in subject_data.items()
    ]


def get_attributes_from_json(json_path: Path) -> list[models.Attribute]:
    with open(json_path, "r", encoding="utf-8") as f:
        attribute_data = json.load(f)
    return [
        models.Attribute(
            attr_code=code,
            title=description,
        )
        for code, description in attribute_data.items()
    ]


def get_restrictions_from_json(json_path: Path) -> list[models.Restriction]:
    with open(json_path, "r", encoding="utf-8") as f:
        restriction_data = json.load(f)
    restriction_models = []
    for restriction_category, restriction_list in restriction_data.items():
        for restriction_code, description in restriction_list.items():
            restriction_models.append(
                models.Restriction(
                    category=restriction_category.upper(),
                    restr_code=restriction_code,
                    title=description,
                )
            )
    return restriction_models


def get_faculty_from_json(json_path: Path) -> list[models.Faculty]:
    with open(json_path, "r", encoding="utf-8") as f:
        faculty_data = json.load(f)
    return [
        models.Faculty(
            rcsid=rcsid,
            first_name=name.split(",")[1].strip(),
            last_name=name.split(",")[0].strip(),
        )
        for rcsid, name in faculty_data.items()
    ]


def compile_course_objects_from_json(
    course_objects: dict, sem_specific_data: SemesterSpecificData, json_path: Path
) -> None:
    with open(json_path, "r", encoding="utf-8") as f:
        term_course_data = json.load(f)
    for _, subject_data in term_course_data.items():
        for course_code, course_data in subject_data["courses"].items():
            course_details = course_data["course_detail"]
            # Add entire course object:
            if course_code not in course_objects:
                course_objects[course_code] = course_data
            # Extract semester-specific data
            year, semester = extract_semester_info_from_filename(json_path)
            subj_code = course_code.split(" ")[0]
            code_num = course_code.split(" ")[1]
            seats_total = 0
            seats_filled = 0
            for section in course_details["sections"]:
                seats_total += section["capacity"]
                seats_filled += section["registered"]
                for faculty_rcsid in section["instructor"]:
                    sem_specific_data.course_faculty.append(
                        models.Course_Faculty(
                            sem_year=year,
                            semester=semester,
                            subj_code=subj_code,
                            code_num=code_num,
                            rcsid=faculty_rcsid,
                        )
                    )
            sem_specific_data.course_offering.append(
                models.Course_Offering(
                    sem_year=year,
                    semester=semester,
                    subj_code=subj_code,
                    code_num=code_num,
                    seats_filled=seats_filled,
                    seats_total=seats_total,
                )
            )


def compile_semester_agnostic_data_from_course_objects(
    course_objects: dict,
) -> SemesterAgnosticData:
    semester_agnostic_data = SemesterAgnosticData()
    for course_code, course_data in course_objects.items():
        course_details = course_data["course_detail"]
        subj_code, code_num = course_code.split(" ")
        semester_agnostic_data.course.append(
            models.Course(
                subj_code=subj_code,
                code_num=code_num,
                title=course_data["course_name"],
                desc_text=course_details["description"],
                credit_min=course_details["credits"]["min"],
                credit_max=course_details["credits"]["max"],
            )
        )
        semester_agnostic_data.course_attribute.extend(
            [
                models.Course_Attribute(
                    subj_code=subj_code,
                    code_num=code_num,
                    attr_code=attribute_code,
                )
                for attribute_code in course_details["attributes"]
            ]
        )
        semester_agnostic_data.course_relationship.extend(
            [
                models.Course_Relationship(
                    subj_code=subj_code,
                    code_num=code_num,
                    relationship=models.RelationshipTypeEnum.COREQUISITE,
                    rel_subj=coreq.split(" ")[0],
                    rel_code_num=coreq.split(" ")[1],
                )
                for coreq in course_details["corequisite"]
            ]
        )
        semester_agnostic_data.course_relationship.extend(
            [
                models.Course_Relationship(
                    subj_code=subj_code,
                    code_num=code_num,
                    relationship=models.RelationshipTypeEnum.CROSSLIST,
                    rel_subj=cross.split(" ")[0],
                    rel_code_num=cross.split(" ")[1],
                )
                for cross in course_details["crosslist"]
            ]
        )
        for restriction_type, restriction_values in course_details[
            "restrictions"
        ].items():
            must_be = not restriction_type.startswith("not_")
            type_key = restriction_type.removeprefix("not_").upper()
            # Ignore special approvals as we don't currently handle them
            if type_key == "SPECIAL_APPROVAL":
                continue
            semester_agnostic_data.course_restriction.extend(
                [
                    models.Course_Restriction(
                        subj_code=subj_code,
                        code_num=code_num,
                        restr_rule=(
                            models.RestrictionRuleEnum.MUST_BE
                            if must_be
                            else models.RestrictionRuleEnum.CANNOT_BE
                        ),
                        category=type_key,
                        restr_code=restr_code,
                    )
                    for restr_code in restriction_values
                ]
            )
    return semester_agnostic_data


def main(
    processed_data_dir: Path | str,
    db_dialect: str,
    db_api: str,
    db_hostname: str,
    db_username: str,
    db_password: str,
    db_schema: str,
) -> None:
    if isinstance(processed_data_dir, str):
        processed_data_dir = Path(processed_data_dir)

    engine, session_factory = init_db_connection(
        db_dialect, db_api, db_hostname, db_username, db_password, db_schema, echo=True
    )
    # drop_all_tables(engine)
    generate_schema(engine)

    # Load and insert code mappings since other tables depend on them
    code_mapping_dir = os.getenv("SCRAPER_CODE_MAPS_DIR")
    subject_models = get_subjects_from_json(
        Path(code_mapping_dir) / os.getenv("SUBJECT_CODE_NAME_MAP_FILENAME")
    )
    attribute_models = get_attributes_from_json(
        Path(code_mapping_dir) / os.getenv("ATTRIBUTE_CODE_NAME_MAP_FILENAME")
    )
    restriction_models = get_restrictions_from_json(
        Path(code_mapping_dir) / os.getenv("RESTRICTION_CODE_NAME_MAP_FILENAME")
    )
    faculty_models = get_faculty_from_json(
        Path(code_mapping_dir) / os.getenv("INSTRUCTOR_RCSID_NAME_MAP_FILENAME")
    )
    generated_faculty_models = get_faculty_from_json(
        Path(code_mapping_dir) / "generated_instructor_rcsid_name_map.json"
    )
    with session_factory() as session:
        session.add_all(restriction_models)
        session.add_all(attribute_models)
        session.add_all(subject_models)
        session.add_all(faculty_models)
        session.add_all(generated_faculty_models)
        session.commit()

    sem_specific_data = SemesterSpecificData()
    course_objects = {}
    for json_path in sorted(processed_data_dir.glob("*.json"), reverse=True):
        compile_course_objects_from_json(course_objects, sem_specific_data, json_path)
    sem_agnostic_data = compile_semester_agnostic_data_from_course_objects(
        course_objects
    )
    with session_factory() as session:
        session.add_all(sem_agnostic_data.course)
        session.commit()
        session.add_all(sem_agnostic_data.course_attribute)
        session.add_all(sem_agnostic_data.course_relationship)
        session.add_all(sem_agnostic_data.course_restriction)
        session.add_all(sem_specific_data.course_offering)
        session.commit()
        # Remove duplicate course faculty models
        unique_course_faculty = {
            (
                course_faculty.sem_year,
                course_faculty.semester,
                course_faculty.subj_code,
                course_faculty.code_num,
                course_faculty.rcsid,
            ): course_faculty
            for course_faculty in sem_specific_data.course_faculty
        }
        session.add_all(unique_course_faculty.values())
        session.commit()

    engine.dispose()


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    processed_data_dir = Path(__file__).parent / os.getenv(
        "SCRAPER_PROCESSED_OUTPUT_DATA_DIR"
    )
    main(
        processed_data_dir=processed_data_dir,
        db_dialect=os.getenv("DB_DIALECT"),
        db_api=os.getenv("DB_API"),
        db_hostname=os.getenv("DB_HOSTNAME"),
        db_username=os.getenv("DB_USERNAME"),
        db_password=os.getenv("DB_PASSWORD"),
        db_schema=os.getenv("DB_SCHEMA"),
    )
