import json
from pathlib import Path

import carpi_data_model.models as models
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

_engine: Engine = None
_session_factory: sessionmaker = None


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


def insert_models_from_json(session: Session, json_path: Path) -> None:
    with open(json_path, "r") as f:
        term_course_data = json.load(f)
    for subject_code, subject_data in term_course_data.items():
        # Insert Subject
        subject_name = subject_data["subject_name"]
        subject_model = models.Subject(subj_code=subject_code, title=subject_name)
        session.merge(subject_model)
        subject_courses = subject_data["courses"]
        for course_code, course_data in subject_courses.items():
            course_num = course_code.split(" ")[1]
            course_details = course_data["course_detail"]
            course_credits = course_details["credits"]
            if course_data["course_name"] == "GRAD ARCH DESIGN 4":
                print(subject_code, course_num)
            # Insert Course
            course_model = models.Course(
                subj_code=subject_code,
                code_num=course_num,
                title=course_data["course_name"],
                desc_text=course_details["description"],
                credit_min=course_credits["min"],
                credit_max=course_credits["max"],
            )
            session.merge(course_model)


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
        db_dialect, db_api, db_hostname, db_username, db_password, db_schema, echo=False
    )
    drop_all_tables(engine)
    generate_schema(engine)

    for json_path in ["processed_data/202509.json"]:
        print(f"Processing {json_path}...")
        with session_factory() as session:
            insert_models_from_json(session, json_path)
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
