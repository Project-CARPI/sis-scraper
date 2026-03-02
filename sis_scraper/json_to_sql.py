import json
import logging
from pathlib import Path

import carpi_data_model.models as models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    A class to manage database connection and operations. Provides common database
    operations as methods.
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
        self._db_dialect = db_dialect
        self._db_api = db_api
        self._db_hostname = db_hostname
        self._db_username = db_username
        self._db_password = db_password
        self._db_schema = db_schema
        self._echo = echo
        self._engine = None
        self._session_factory = None

    def init_connection(self) -> None:
        """
        Initializes the database engine and session factory if they haven't been
        initialized yet.
        """
        if self._engine is None:
            # Create the database engine using a string in the format:
            # dialect+api://username:password@hostname/schema
            self._engine = create_engine(
                get_db_url(
                    db_dialect=self._db_dialect,
                    db_api=self._db_api,
                    db_hostname=self._db_hostname,
                    db_username=self._db_username,
                    db_password=self._db_password,
                    db_schema=self._db_schema,
                ),
                echo=self._echo,
            )
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self._engine)

    def close_connection(self) -> None:
        """
        Closes the database connection by disposing the engine.
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    def generate_schema(self) -> None:
        """
        Generates the database schema based on the models defined in models.py.
        """
        models.Base.metadata.create_all(self._engine)

    def drop_all_tables(self) -> None:
        """
        Drops all tables in the database.

        WARNING: This will delete all data in the database. Use with caution.
        """
        models.Base.metadata.drop_all(self._engine)

    def commit_all(self, *models: list[models.Base]) -> None:
        """
        Commits all provided models to the database in a single transaction.

        @param models: Variable number of lists of model instances to commit.
        """
        with self._session_factory() as session:
            for model_list in models:
                session.add_all(model_list)
            session.commit()


def get_db_url(
    db_dialect: str,
    db_api: str,
    db_hostname: str,
    db_username: str,
    db_password: str,
    db_schema: str,
) -> str:
    """
    Constructs a database URL string from the provided components.

    @param db_dialect: Database dialect (e.g., "mysql").
    @param db_api: Database API (e.g., "mysqlconnector").
    @param db_hostname: Database hostname (e.g., "localhost:3306").
    @param db_username: Database username.
    @param db_password: Database password.
    @param db_schema: Database schema name.
    @return: Database URL string in the format: \
        dialect+api://username:password@hostname/schema
    """
    return (
        f"{db_dialect}+{db_api}://"
        f"{db_username}:{db_password}"
        f"@{db_hostname}/{db_schema}"
    )


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
    """
    Processes the data for a single term and appends model instances to the provided
    lists.

    @param term_data: Dictionary containing the term's course data.
    @param year: Year of the term (e.g., 2024).
    @param semester: Semester name (e.g., "FALL").
    @param course_models: List to append Course model instances to.
    @param course_attribute_models: List to append Course_Attribute model instances to.
    @param course_relationship_models: List to append Course_Relationship model instances
        to.
    @param course_restriction_models: List to append Course_Restriction model instances
        to.
    @param course_offering_models: List to append Course_Offering model instances to.
    @param course_faculty_models: List to append Course_Faculty model instances to.
    @param processed_courses: Set of course codes that have already been processed in
        later semesters. This is used to prioritize the most recent
        data for courses that appear in multiple semesters.
    """
    for subject_code, subject_data in term_data.items():
        for course_num, course_sections in subject_data["courses"].items():
            course_code = f"{subject_code} {course_num}"
            # Skip if this course has already been processed in a later semester
            if course_code in processed_courses:
                continue
            processed_courses.add(course_code)
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
                    credit_max=main_section["creditMax"] or main_section["creditMin"],
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
                        rel_subj=crosslist.split(" ")[0],
                        rel_code_num=crosslist.split(" ")[1],
                    )
                    # Some classes may have duplicate crosslist entries
                    for crosslist in set(main_section["crosslists"])
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
    """
    Runs the JSON to SQL conversion process: initializes the database connection,
    processes each term's data, and commits the resulting models to the database.

    @param processed_data_dir: Directory containing the processed JSON files.
    @param db_dialect: Database dialect (e.g., "mysql").
    @param db_api: Database API (e.g., "mysqlconnector").
    @param db_hostname: Database hostname (e.g., "localhost:3306").
    @param db_username: Database username.
    @param db_password: Database password.
    @param db_schema: Database schema name.
    @param attribute_code_name_map_path: Path to JSON file mapping attribute codes to
        names.
    @param instructor_rcsid_name_map_path: Path to JSON file mapping instructor RCSIDs to
        names.
    @param generated_instructor_rcsid_name_map_path: Path to JSON file mapping generated
        nstructor RCSIDs to names.
    @param restriction_code_name_map_path: Path to JSON file mapping restriction codes to
        names.
    @param subject_code_name_map_path: Path to JSON file mapping subject codes to names.
    """
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
    db_manager.init_connection()
    logger.info(
        "Connected to database with URL "
        + get_db_url(db_dialect, db_api, db_hostname, db_username, "****", db_schema)
    )

    # Reset database schema
    db_manager.drop_all_tables()
    logger.info("Dropped all existing tables in the database.")
    db_manager.generate_schema()
    logger.info("Generated database schema based on models.")

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
    logger.info(f"Committed {len(restriction_models)} restrictions")
    logger.info(f"Committed {len(attribute_models)} attributes")
    logger.info(f"Committed {len(subject_models)} subjects")
    logger.info(f"Committed {len(faculty_models)} faculty members")
    logger.info(f"Committed {len(generated_faculty_models)} generated faculty members")

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
        course_attribute, course_relationship, course_restriction, course_offering
    )
    db_manager.commit_all(course_faculty)
    logger.info(f"Committed {len(course)} courses")
    logger.info(f"Committed {len(course_attribute)} course attributes")
    logger.info(f"Committed {len(course_relationship)} course relationships")
    logger.info(f"Committed {len(course_restriction)} course restrictions")
    logger.info(f"Committed {len(course_offering)} course offerings")
    logger.info(f"Committed {len(course_faculty)} course faculty assignments")

    db_manager.close_connection()
