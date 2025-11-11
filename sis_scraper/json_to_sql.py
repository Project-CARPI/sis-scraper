import json
from pathlib import Path

import carpi_data_model.models as models
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

_engine: Engine = None
_session_factory: sessionmaker = None


class SemesterAgnosticData:
    def __init__(self):
        self.course = {}
        self.course_attribute = {}
        self.course_relationship = {}
        self.course_restriction = {}


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
    with engine.connect() as conn:
        trans = conn.begin()
        for table in reversed(models.Base.metadata.sorted_tables):
            conn.execute(table.delete())
        trans.commit()


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


def compile_course_objects_from_json(
    course_objects: dict, sem_specific_data: SemesterSpecificData, json_path: Path
) -> None:
    with open(json_path, "r") as f:
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
                seats_total += section["seats_total"]
                seats_filled += section["seats_filled"]
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


def main(
    processed_data_dir: Path | str,
    db_dialect: str,
    db_api: str,
    db_hostname: str,
    db_username: str,
    db_password: str,
    db_schema: str,
) -> None:
    """
    PSEUDOCODE (temporary)
    initialize big data structure with lists for all models
    for each json file (IN REVERSE CHRONOLOGICAL ORDER): DONE
        for each department:
            for each course:
                add the entire course json object to the "big data structure"
                    on duplicate ignore
                extract semester-specific data and add that separately to the "big data structure"
    after all json files processed:
        for each model list in the "big data structure":
            bulk insert into database
            commit
    have a nice day
    """

    if isinstance(processed_data_dir, str):
        processed_data_dir = Path(processed_data_dir)

    engine, session_factory = init_db_connection(
        db_dialect, db_api, db_hostname, db_username, db_password, db_schema, echo=True
    )
    # drop_all_tables(engine)
    generate_schema(engine)

    # TODO: insert subjects, attributes, restrictions, and faculty from their separate
    # JSON files first

    sem_specific_data = SemesterSpecificData()
    course_objects = {}
    for json_path in sorted(processed_data_dir.glob("*.json"), reverse=True):
        compile_course_objects_from_json(course_objects, sem_specific_data, json_path)

    # sem_agnostic_data = SemesterAgnosticData()
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
