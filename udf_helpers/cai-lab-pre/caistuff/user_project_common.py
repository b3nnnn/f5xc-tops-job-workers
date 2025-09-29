"""Shared helpers for CalypsoAI user/project examples."""
from __future__ import annotations

from collections.abc import Callable, Iterable

from calypsoai import CalypsoAI, RequestError, datatypes as dt


def find_user(api: CalypsoAI, email: str) -> dt.User | None:
    target = email.lower()
    for user in api.users.iterate(search=email, batchSize=50):
        if user.email and user.email.lower() == target:
            return user
    return None


def find_role(api: CalypsoAI, name: str) -> dt.Role | None:
    target = name.lower()
    for role in api.roles.iterate(batchSize=50):
        if role.name and role.name.lower() == target:
            return role
    return None


def _user_is_admin(api: CalypsoAI, project: dt.Project, email: str) -> bool:
    target = email.lower()
    try:
        for member in api.projects.members.iterate(project, admin=True, batchSize=50):
            if member.email and member.email.lower() == target:
                return True
    except RequestError as exc:
        if exc.statusCode in {403, 404}:
            return False
        raise
    return False


def clean_projects(api: CalypsoAI, project_name: str, admin_email: str) -> list[str]:
    deleted: list[str] = []
    for project in api.projects.iterate(search=project_name, batchSize=50):
        if project.name != project_name:
            continue
        try:
            is_admin = _user_is_admin(api, project, admin_email)
        except RequestError as exc:
            if exc.statusCode in {403, 404}:
                continue
            raise
        if not is_admin:
            continue
        identifier = str(project.id or project.friendlyId or project.name)
        try:
            api.client.projects.deleteProject(identifier)
        except RequestError as exc:
            if exc.statusCode in {403, 404}:
                print(
                    f"Skipping deletion for project '{identifier}' during cleanup (status {exc.statusCode}).",
                )
                continue
            raise
        deleted.append(identifier)
    return deleted


def ensure_only_admin(
    api: CalypsoAI,
    project: dt.Project,
    admin_email: str,
    *,
    log: Callable[[str], None] | None = print,
) -> None:
    """Remove all project admins except the requested user."""

    def emit(message: str) -> None:
        if log:
            log(message)

    target = admin_email.lower()
    try:
        admins: Iterable[dt.User] = api.projects.members.iterate(project, admin=True, batchSize=50)
    except RequestError as exc:
        if exc.statusCode in {403, 404}:
            emit(
                "Project admin cleanup skipped due to API response "
                f"{exc.statusCode}; continuing.",
            )
            return
        raise

    for admin in admins:
        email = (admin.email or "").lower()
        if not email or email == target:
            continue
        try:
            api.projects.members.remove(project, user=admin)
        except RequestError as exc:
            if exc.statusCode in {403, 404}:
                emit(
                    f"Project admin cleanup: could not remove {admin.email} "
                    f"(status {exc.statusCode}); continuing.",
                )
                continue
            raise
        emit(
            f"Project admin cleanup: removed {admin.email or 'unknown user'} "
            f"from project '{project.name}'.",
        )
