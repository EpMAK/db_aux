from __future__ import annotations

from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from orm_app.models import Post, User


def create_user(session: Session, name: str, email: str, is_active: bool = True) -> User:
    user = User(name=name, email=email, is_active=is_active)
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def create_post(session: Session, user_id: int, title: str, body: str) -> Post:
    post = Post(user_id=user_id, title=title, body=body)
    session.add(post)
    session.commit()
    session.refresh(post)
    return post


def get_user_with_posts(session: Session, user_id: int) -> User | None:
    stmt = select(User).options(selectinload(User.posts)).where(User.id == user_id)
    return session.execute(stmt).scalar_one_or_none()


def get_all_users(session: Session) -> Sequence[User]:
    stmt = select(User).options(selectinload(User.posts)).order_by(User.id)
    return session.execute(stmt).scalars().all()


def update_post_title(session: Session, post_id: int, new_title: str) -> Post | None:
    post = session.get(Post, post_id)
    if post is None:
        return None
    post.title = new_title
    session.commit()
    session.refresh(post)
    return post


def update_user_status(session: Session, user_id: int, is_active: bool) -> User | None:
    user = session.get(User, user_id)
    if user is None:
        return None
    user.is_active = is_active
    session.commit()
    session.refresh(user)
    return user


def delete_user(session: Session, user_id: int) -> bool:
    user = session.get(User, user_id)
    if user is None:
        return False
    session.delete(user)
    session.commit()
    return True
