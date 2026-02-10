from __future__ import annotations

import pytest
from faker import Faker
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from orm_app import crud
from orm_app.models import Post


def test_create_user_positive(db_session: Session, faker: Faker) -> None:
    user = crud.create_user(
        db_session,
        name=faker.name(),
        email=faker.email(),
        is_active=True,
    )

    assert user.id is not None
    assert user.name
    assert user.email
    assert user.is_active is True


def test_create_user_duplicate_email_negative(db_session: Session, faker: Faker) -> None:
    email = faker.email()
    crud.create_user(db_session, name=faker.name(), email=email, is_active=True)

    with pytest.raises(IntegrityError):
        crud.create_user(db_session, name=faker.name(), email=email, is_active=True)


def test_get_user_with_posts_positive(db_session: Session, faker: Faker) -> None:
    user = crud.create_user(db_session, name=faker.name(), email=faker.email())
    first_post = crud.create_post(db_session, user_id=user.id, title=faker.sentence(nb_words=4), body=faker.text())
    second_post = crud.create_post(db_session, user_id=user.id, title=faker.sentence(nb_words=4), body=faker.text())

    fetched = crud.get_user_with_posts(db_session, user.id)

    assert fetched is not None
    assert fetched.id == user.id
    assert {post.id for post in fetched.posts} == {first_post.id, second_post.id}


def test_get_user_with_posts_negative_not_found(db_session: Session) -> None:
    fetched = crud.get_user_with_posts(db_session, user_id=999_999)

    assert fetched is None


def test_create_post_negative_user_not_found(db_session: Session, faker: Faker) -> None:
    with pytest.raises(IntegrityError):
        crud.create_post(
            db_session,
            user_id=999_999,
            title=faker.sentence(nb_words=4),
            body=faker.text(),
        )


def test_get_all_users_positive(db_session: Session, faker: Faker) -> None:
    u1 = crud.create_user(db_session, name=faker.name(), email=faker.email())
    u2 = crud.create_user(db_session, name=faker.name(), email=faker.email(), is_active=False)

    users = crud.get_all_users(db_session)

    assert [user.id for user in users] == [u1.id, u2.id]


def test_update_post_title_positive(db_session: Session, faker: Faker) -> None:
    user = crud.create_user(db_session, name=faker.name(), email=faker.email())
    post = crud.create_post(db_session, user.id, title=faker.sentence(nb_words=3), body=faker.text())
    new_title = faker.sentence(nb_words=5)

    updated = crud.update_post_title(db_session, post.id, new_title)

    assert updated is not None
    assert updated.title == new_title


def test_update_post_title_negative_not_found(db_session: Session, faker: Faker) -> None:
    updated = crud.update_post_title(db_session, post_id=999_999, new_title=faker.sentence(nb_words=4))

    assert updated is None


def test_update_user_status_positive(db_session: Session, faker: Faker) -> None:
    user = crud.create_user(db_session, name=faker.name(), email=faker.email(), is_active=True)

    updated = crud.update_user_status(db_session, user_id=user.id, is_active=False)

    assert updated is not None
    assert updated.is_active is False


def test_update_user_status_negative_not_found(db_session: Session) -> None:
    updated = crud.update_user_status(db_session, user_id=999_999, is_active=False)

    assert updated is None


def test_delete_user_positive_with_cascade(db_session: Session, faker: Faker) -> None:
    user = crud.create_user(db_session, name=faker.name(), email=faker.email())
    post = crud.create_post(db_session, user_id=user.id, title=faker.sentence(nb_words=4), body=faker.text())
    post_id = post.id

    deleted = crud.delete_user(db_session, user_id=user.id)

    assert deleted is True
    assert crud.get_user_with_posts(db_session, user.id) is None
    assert db_session.scalar(select(Post).where(Post.id == post_id)) is None


def test_delete_user_negative_not_found(db_session: Session) -> None:
    deleted = crud.delete_user(db_session, user_id=999_999)

    assert deleted is False
