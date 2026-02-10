from __future__ import annotations

import os
import uuid
from typing import Generator

import pytest
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from orm_app.base import Base


@pytest.fixture(scope="session")
def db_engine() -> Generator[Engine, None, None]:
    database_url = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL") or "postgresql+psycopg://app_user:app_pass@localhost:5432/app_db"
    engine = create_engine(database_url, future=True)

    try:
        with engine.connect():
            pass
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL недоступен для интеграционных тестов: {exc}")

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def db_schema(db_engine: Engine) -> Generator[str, None, None]:
    schema_name = f"pytest_{uuid.uuid4().hex[:8]}"
    with db_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
        conn.execute(text(f'SET search_path TO "{schema_name}"'))
        Base.metadata.create_all(bind=conn)

    try:
        yield schema_name
    finally:
        with db_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@pytest.fixture(scope="function")
def db_session(db_engine: Engine, db_schema: str) -> Generator[Session, None, None]:
    with Session(db_engine) as session:
        session.execute(text(f'SET search_path TO "{db_schema}"'))
        yield session

        session.rollback()
        session.execute(text("TRUNCATE TABLE posts, users RESTART IDENTITY CASCADE"))
        session.commit()


@pytest.fixture(scope="function")
def faker() -> Faker:
    return Faker("ru_RU")
