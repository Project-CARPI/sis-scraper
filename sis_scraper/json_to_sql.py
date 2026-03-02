import json
from pathlib import Path

import carpi_data_model.models as models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DatabaseManager:
    """
    A class to manage database connection and operations. It automatically initializes the
    database connection on instantiation, and provides common database operations as
    methods.
    """

    def __init__(
        self,
        db_dialect: str,
        db_api: str,
        db_hostname: str,
        db_username: str,
        db_password: str,
        db_schema: str,
        echo: bool = False,
    ):
        """
        Initializes the database connection and session factory.

        @param db_dialect: Database dialect (e.g., "mysql").
        @param db_api: Database API (e.g., "mysqlconnector").
        @param db_hostname: Database hostname (e.g., "localhost:3306").
        @param db_username: Database username.
        @param db_password: Database password.
        @param db_schema: Database schema name.
        @param echo: Whether to log SQL statements.
        @return: Tuple of (Engine, sessionmaker).
        """
        self._db_dialect = db_dialect
        self._db_api = db_api
        self._db_hostname = db_hostname
        self._db_username = db_username
        self._db_password = db_password
        self._db_schema = db_schema
        self._echo = echo
        self._engine, self._session_factory = self._init_connection()

    def _init_connection(self) -> bool:
        """
        Initializes the database engine and session factory if they haven't been
        initialized yet.
        """
        try:
            if self._engine is None:
                # Create the database engine using a string in the format:
                # dialect+api://username:password@hostname/schema
                self._engine = create_engine(
                    f"{self._db_dialect}+{self._db_api}://"
                    f"{self._db_username}:{self._db_password}"
                    f"@{self._db_hostname}/{self._db_schema}",
                    echo=self._echo,
                )
            if self._session_factory is None:
                self._session_factory = sessionmaker(bind=self._engine)
        except Exception as e:
            print(f"Error initializing database connection: {e}")
            return False
        return True

    def close_connection(self) -> None:
        """
        Closes the database connection by disposing the engine.
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    def generate_schema(self) -> bool:
        """
        Generates the database schema based on the models defined in models.py.
        """
        try:
            models.Base.metadata.create_all(self._engine)
        except Exception as e:
            print(f"Error generating database schema: {e}")
            return False
        return True

    def drop_all_tables(self) -> bool:
        """
        Drops all tables in the database.

        WARNING: This will delete all data in the database. Use with caution.
        """
        try:
            models.Base.metadata.drop_all(self._engine)
        except Exception as e:
            print(f"Error dropping all tables: {e}")
            return False
        return True

    def commit_all(self, *models: list[models.Base]) -> bool:
        """
        Commits all provided models to the database in a single transaction.

        @param models: Variable number of lists of model instances to commit.
        @return: True if commit was successful, False otherwise.
        """
        try:
            with self._session_factory() as session:
                for model_list in models:
                    session.add_all(model_list)
                session.commit()
        except Exception as e:
            print(f"Error committing to database: {e}")
            return False
        return True


def get_semester_info_from_filename(file_path: Path) -> tuple[int, str]:
    """
    Converts a filename like "202409.json" to a tuple of (2024, "FALL").

    @param file_path: Path to the JSON file.
    @return: Tuple of (year, semester).
    """
    stem = file_path.stem
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


def load_code_mapping(json_path: Path) -> dict:
    """
    Simple helper to load a code mapping from a JSON file.

    @param json_path: Path to the JSON file containing the code mapping.
    @return: Dictionary representing the code mapping.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def process_term(
    term_data: dict,
    year: int,
    semester: str,
    course_models: list[models.Course],
    course_attribute_models: list[models.Course_Attribute],
    course_relationship_models: list[models.Course_Relationship],
    course_restriction_models: list[models.Course_Restriction],
    course_offering_models: list[models.Course_Offering],
    course_faculty_models: list[models.Course_Faculty],
    processed_courses: set[str],
) -> None:
    for subject_code, subject_data in term_data.items():
        for course_num, course_sections in subject_data["courses"].items():
            # Skip if this course has already been processed in a later semester
            if f"{subject_code} {course_num}" in processed_courses:
                continue
            processed_courses.add(f"{subject_code} {course_num}")
            # Use the first section's data to represent course-level information
            main_section = course_sections[0]
            # Add course model
            course_models.append(
                models.Course(
                    subj_code=subject_code,
                    code_num=course_num,
                    title=main_section["title"],
                    desc_text=main_section["description"],
                    credit_min=main_section["creditMin"],
                    credit_max=main_section["creditMax"],
                )
            )
            # Add course attribute models
            course_attribute_models.extend(
                [
                    models.Course_Attribute(
                        subj_code=subject_code,
                        code_num=course_num,
                        attr_code=attribute_code,
                    )
                    for attribute_code in main_section["attributes"]
                ]
            )
            # Add course corequisite relationship models
            course_relationship_models.extend(
                [
                    models.Course_Relationship(
                        subj_code=subject_code,
                        code_num=course_num,
                        relationship=models.RelationshipTypeEnum.COREQUISITE,
                        rel_subj=coreq.split(" ")[0],
                        rel_code_num=coreq.split(" ")[1],
                    )
                    for coreq in main_section["corequisites"]
                ]
            )
            # Add course crosslist relationship models
            course_relationship_models.extend(
                [
                    models.Course_Relationship(
                        subj_code=subject_code,
                        code_num=course_num,
                        relationship=models.RelationshipTypeEnum.CROSSLIST,
                        rel_subj=cross.split(" ")[0],
                        rel_code_num=cross.split(" ")[1],
                    )
                    for cross in main_section["crosslists"]
                ]
            )
            # Add course restriction models
            for restriction_type, restriction_values in main_section[
                "restrictions"
            ].items():
                must_be = not restriction_type.startswith("not_")
                type_key = restriction_type.removeprefix("not_").upper()
                # Ignore special approvals as we don't currently handle them
                if type_key == "SPECIAL_APPROVAL":
                    continue
                course_restriction_models.extend(
                    [
                        models.Course_Restriction(
                            subj_code=subject_code,
                            code_num=course_num,
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
            # Add course offering models
            seats_total = 0
            seats_filled = 0
            for section in course_sections:
                seats_total += section["seatsCapacity"]
                seats_filled += section["seatsRegistered"]
            course_offering_models.append(
                models.Course_Offering(
                    sem_year=year,
                    semester=semester,
                    subj_code=subject_code,
                    code_num=course_num,
                    seats_total=seats_total,
                    seats_filled=seats_filled,
                )
            )
            # Add course faculty models
            for faculty in main_section["faculty"]:
                course_faculty_models.append(
                    models.Course_Faculty(
                        sem_year=year,
                        semester=semester,
                        subj_code=subject_code,
                        code_num=course_num,
                        rcsid=faculty["rcsid"],
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
    attribute_code_name_map_path: Path | str,
    instructor_rcsid_name_map_path: Path | str,
    generated_instructor_rcsid_name_map_path: Path | str,
    restriction_code_name_map_path: Path | str,
    subject_code_name_map_path: Path | str,
) -> None:
    # Convert to Path objects if needed
    processed_data_dir = Path(processed_data_dir)
    attribute_code_name_map_path = Path(attribute_code_name_map_path)
    instructor_rcsid_name_map_path = Path(instructor_rcsid_name_map_path)
    generated_instructor_rcsid_name_map_path = Path(
        generated_instructor_rcsid_name_map_path
    )
    restriction_code_name_map_path = Path(restriction_code_name_map_path)
    subject_code_name_map_path = Path(subject_code_name_map_path)

    # Initialize database connection
    db_manager = DatabaseManager(
        db_dialect=db_dialect,
        db_api=db_api,
        db_hostname=db_hostname,
        db_username=db_username,
        db_password=db_password,
        db_schema=db_schema,
        echo=False,
    )
    # Reset database schema
    db_manager.drop_all_tables()
    db_manager.generate_schema()

    # Load and insert code mappings first since other tables depend on them
    attribute_data: dict[str, str] = load_code_mapping(attribute_code_name_map_path)
    attribute_models = [
        models.Attribute(
            attr_code=code,
            title=description,
        )
        for code, description in attribute_data.items()
    ]
    faculty_data: dict[str, list[str]] = load_code_mapping(
        instructor_rcsid_name_map_path
    )
    faculty_models = [
        models.Faculty(
            rcsid=rcsid,
            # Assume first name is everything after the first space
            first_name=" ".join(name.split()[1:]).strip(),
            last_name=name.split()[0].strip(),
        )
        for rcsid, [name, _] in faculty_data.items()
    ]
    generated_faculty_data: dict[str, list[str]] = load_code_mapping(
        generated_instructor_rcsid_name_map_path
    )
    generated_faculty_models = [
        models.Faculty(
            rcsid=rcsid,
            # Assume first name is everything after the first space
            first_name=" ".join(name.split()[1:]).strip(),
            last_name=name.split()[0].strip(),
        )
        for rcsid, [name, _] in generated_faculty_data.items()
    ]
    restriction_data: dict[str, dict[str, str]] = load_code_mapping(
        restriction_code_name_map_path
    )
    restriction_models = [
        models.Restriction(
            category=restriction_category.upper(),
            restr_code=restriction_code,
            title=description,
        )
        for restriction_category, restriction_list in restriction_data.items()
        for restriction_code, description in restriction_list.items()
    ]
    subject_data: dict[str, str] = load_code_mapping(subject_code_name_map_path)
    subject_models = [
        models.Subject(
            subj_code=code,
            title=description,
        )
        for code, description in subject_data.items()
    ]

    # Insert code mappings into the database
    db_manager.commit_all(
        restriction_models,
        attribute_models,
        subject_models,
        faculty_models,
        generated_faculty_models,
    )

    # Semester-agnostic data models
    course = []
    course_attribute = []
    course_relationship = []
    course_restriction = []

    # Track processed courses to prioritize latest data across multiple semesters
    processed_courses = set()

    # Semester-specific data models
    course_offering = []
    course_faculty = []

    # Process JSON files in reverse chronological order to prioritize latest data
    for json_path in sorted(processed_data_dir.glob("*.json"), reverse=True):
        year, semester = get_semester_info_from_filename(json_path)
        with open(json_path, "r", encoding="utf-8") as f:
            term_data = json.load(f)
        process_term(
            term_data=term_data,
            year=year,
            semester=semester,
            course_models=course,
            course_attribute_models=course_attribute,
            course_relationship_models=course_relationship,
            course_restriction_models=course_restriction,
            course_offering_models=course_offering,
            course_faculty_models=course_faculty,
            processed_courses=processed_courses,
        )

    # Insert course data into the database
    db_manager.commit_all(course)
    db_manager.commit_all(
        course_attribute,
        course_relationship,
        course_restriction,
        course_offering,
        course_faculty,
    )

    db_manager.close_connection()


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    load_dotenv()
    processed_data_dir = Path(__file__).parent / os.getenv(
        "SCRAPER_PROCESSED_OUTPUT_DATA_DIR"
    )
    output_data_dir = Path(__file__).parent / os.getenv("SCRAPER_RAW_OUTPUT_DATA_DIR")
    processed_data_dir = Path(__file__).parent / os.getenv(
        "SCRAPER_PROCESSED_OUTPUT_DATA_DIR"
    )
    code_maps_dir = Path(__file__).parent / os.getenv("SCRAPER_CODE_MAPS_DIR")
    attribute_code_name_map_path = code_maps_dir / os.getenv(
        "ATTRIBUTE_CODE_NAME_MAP_FILENAME"
    )
    instructor_rcsid_name_map_path = code_maps_dir / os.getenv(
        "INSTRUCTOR_RCSID_NAME_MAP_FILENAME"
    )
    generated_instructor_rcsid_name_map_path = (
        code_maps_dir / "generated_instructor_rcsid_name_map.json"
    )
    restriction_code_name_map_path = code_maps_dir / os.getenv(
        "RESTRICTION_CODE_NAME_MAP_FILENAME"
    )
    subject_code_name_map_path = code_maps_dir / os.getenv(
        "SUBJECT_CODE_NAME_MAP_FILENAME"
    )

    db_dialect = os.getenv("DB_DIALECT")
    db_api = os.getenv("DB_API")
    db_hostname = os.getenv("DB_HOSTNAME")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_schema = os.getenv("DB_SCHEMA")

    main(
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
    )
