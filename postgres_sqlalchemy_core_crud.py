from __future__ import annotations

from typing import Any

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, delete, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


DATABASE_URL = "postgresql+psycopg://app_user:app_pass@localhost:5432/app_db"

metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(120), nullable=False),
    Column("email", String(255), nullable=False, unique=True),
    Column("age", Integer, nullable=False),
)


def get_engine(database_url: str = DATABASE_URL) -> Engine:
    return create_engine(database_url, future=True)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


def create_user(engine: Engine, name: str, email: str, age: int) -> int:
    conn = engine.connect()
    transaction = conn.begin()
    try:
        stmt = insert(users).values(name=name, email=email, age=age).returning(users.c.id)
        user_id = conn.execute(stmt).scalar_one()
        transaction.commit()
        return user_id
    except SQLAlchemyError as exc:
        transaction.rollback()
        raise RuntimeError(f"Ошибка создания пользователя: {exc}") from exc
    finally:
        conn.close()


def get_user_by_id(engine: Engine, user_id: int) -> dict[str, Any] | None:
    with engine.connect() as conn:
        stmt = select(users).where(users.c.id == user_id)
        row = conn.execute(stmt).mappings().first()
        return dict(row) if row else None


def get_all_users(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        stmt = select(users).order_by(users.c.id)
        rows = conn.execute(stmt).mappings().all()
        return [dict(row) for row in rows]


def update_user(
    engine: Engine,
    user_id: int,
    name: str | None = None,
    email: str | None = None,
    age: int | None = None,
) -> bool:
    values: dict[str, Any] = {}
    if name is not None:
        values["name"] = name
    if email is not None:
        values["email"] = email
    if age is not None:
        values["age"] = age

    if not values:
        return False

    conn = engine.connect()
    transaction = conn.begin()
    try:
        stmt = update(users).where(users.c.id == user_id).values(**values)
        result = conn.execute(stmt)
        transaction.commit()
        return result.rowcount > 0
    except SQLAlchemyError as exc:
        transaction.rollback()
        raise RuntimeError(f"Ошибка обновления пользователя: {exc}") from exc
    finally:
        conn.close()


def delete_user(engine: Engine, user_id: int) -> bool:
    conn = engine.connect()
    transaction = conn.begin()
    try:
        stmt = delete(users).where(users.c.id == user_id)
        result = conn.execute(stmt)
        transaction.commit()
        return result.rowcount > 0
    except SQLAlchemyError as exc:
        transaction.rollback()
        raise RuntimeError(f"Ошибка удаления пользователя: {exc}") from exc
    finally:
        conn.close()


if __name__ == "__main__":
    engine = get_engine()
    init_db(engine)

    print("Таблица users создана (если отсутствовала).")

    created_user_id = create_user(engine, name="Alice", email="alice@example.com", age=30)
    print(f"Создан пользователь с id={created_user_id}")

    print("Пользователь по id:", get_user_by_id(engine, created_user_id))
    print("Все пользователи:", get_all_users(engine))

    updated = update_user(engine, created_user_id, age=31)
    print("Обновление выполнено:", updated)
    print("После обновления:", get_user_by_id(engine, created_user_id))

    deleted = delete_user(engine, created_user_id)
    print("Удаление выполнено:", deleted)
