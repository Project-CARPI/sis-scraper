import carpi_data_model.models as models
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

_engine: Engine = None
_session_factory: sessionmaker = None


def init_engine_and_sessionmaker(
    db_dialect: str,
    db_api: str,
    db_hostname: str,
    db_username: str,
    db_password: str,
    db_schema: str,
) -> tuple[Engine, sessionmaker]:
    global _engine, _session_factory
    if _engine is None:
        _engine = create_engine(
            f"{db_dialect}+{db_api}://{db_username}:{db_password}"
            f"@{db_hostname}/{db_schema}",
            echo=True,
        )
        _session_factory = sessionmaker(bind=_engine)
    return _engine, _session_factory


def generate_schema(engine: Engine) -> None:
    models.Base.metadata.create_all(engine)


def main():
    load_dotenv()
    db_dialect = os.getenv("DB_DIALECT")
    db_api = os.getenv("DB_API")
    db_hostname = os.getenv("DB_HOSTNAME")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_schema = os.getenv("DB_SCHEMA")
    engine, session_factory = init_engine_and_sessionmaker(
        db_dialect, db_api, db_hostname, db_username, db_password, db_schema
    )
    generate_schema(engine)
    engine.dispose()


if __name__ == "__main__":
    main()
