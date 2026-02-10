from __future__ import annotations

from orm_app.crud import (
    create_post,
    create_user,
    delete_user,
    get_all_users,
    get_user_with_posts,
    update_post_title,
    update_user_status,
)
from orm_app.database import SessionLocal
from orm_app.models import Post


def main() -> None:
    with SessionLocal() as session:
        user = create_user(session, name="Alice", email="alice@example.com", is_active=True)
        post_1 = create_post(session, user_id=user.id, title="First", body="Hello")
        _post_2 = create_post(session, user_id=user.id, title="Second", body="World")

        loaded_user = get_user_with_posts(session, user.id)
        print("User with posts:", loaded_user.id, [p.title for p in loaded_user.posts])

        updated = update_post_title(session, post_1.id, "First (edited)")
        print("Updated post:", updated.id, updated.title)
        status_updated = update_user_status(session, user.id, False)
        print("Updated user status:", status_updated.id, status_updated.is_active)

        print("All users:", [(u.id, u.email, u.is_active, len(u.posts)) for u in get_all_users(session)])

        delete_user(session, user.id)

        remained_posts = session.query(Post).filter(Post.user_id == user.id).all()
        print("Posts after user delete (should be empty):", remained_posts)


if __name__ == "__main__":
    main()
