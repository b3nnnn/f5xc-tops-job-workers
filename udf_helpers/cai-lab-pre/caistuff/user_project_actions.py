"""High-level helpers for CalypsoAI user/project provisioning workflows."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

from calypsoai import CalypsoAI, RequestError, datatypes as dt

from caistuff.user_project_common import clean_projects, ensure_only_admin, find_role, find_user

DEFAULT_CALYPSOAI_URL = "https://www.us1.calypsoai.app"

LogFn = Callable[[str], None]


@dataclass(slots=True)
class CreateUserProjectResult:
    """Result of the create_user_project workflow."""

    user: dt.User
    project: dt.Project
    verification_email_sent: bool
    verification_message: str | None


@dataclass(slots=True)
class CleanupUserProjectResult:
    """Result of the cleanup_user_project workflow."""

    deleted_projects: list[str]
    user_deleted: bool
    user: dt.User | None


def _emit(log: LogFn | None, message: str) -> None:
    if log:
        log(message)


def _coerce_project_type(project_type: dt.ProjectType | str) -> dt.ProjectType:
    if isinstance(project_type, dt.ProjectType):
        return project_type
    if isinstance(project_type, str):
        try:
            return dt.ProjectType[project_type.upper()]
        except KeyError as exc:  # pragma: no cover - defensive branch
            valid = ", ".join(pt.name.lower() for pt in dt.ProjectType)
            raise ValueError(
                f"Invalid project_type '{project_type}'. Valid options: {valid}.",
            ) from exc
    raise TypeError("project_type must be a ProjectType or string name")


def _ensure_api(
    api: CalypsoAI | None,
    *,
    url: str | None,
    token: str | None,
    insecure: bool,
) -> CalypsoAI:
    if api is not None:
        return api

    resolved_url = url or os.environ.get("CALYPSOAI_URL") or DEFAULT_CALYPSOAI_URL
    resolved_token = token or os.environ.get("CALYPSOAI_TOKEN")

    if not resolved_url:
        raise ValueError(
            "CalypsoAI URL must be provided via parameter or CALYPSOAI_URL environment variable.",
        )
    if not resolved_token:
        raise ValueError(
            "CalypsoAI API token must be provided via parameter or CALYPSOAI_TOKEN environment variable.",
        )

    return CalypsoAI(url=resolved_url, token=resolved_token, verify_tls=not insecure)


def _project_exists(api: CalypsoAI, project_name: str) -> bool:
    try:
        for project in api.projects.iterate(search=project_name, batchSize=50):
            if project.name == project_name:
                return True
    except RequestError as exc:
        if exc.statusCode in {403, 404}:
            return False
        raise
    return False


def create_user_project(
    email: str,
    *,
    name: str | None = None,
    role: str = "UDF-user",
    project_type: dt.ProjectType | str = dt.ProjectType.CHAT,
    send_invite: bool = False,
    api: CalypsoAI | None = None,
    url: str | None = None,
    token: str | None = None,
    insecure: bool = False,
    ensure_admin_cleanup: bool = True,
    log: LogFn | None = None,
) -> CreateUserProjectResult:
    """Provision a CalypsoAI user and project for the supplied email."""

    calypso = _ensure_api(api, url=url, token=token, insecure=insecure)
    project_name = f"{email}-project"

    if find_user(calypso, email):
        raise ValueError(
            f"User {email} already exists. Run cleanup before creating a fresh user.",
        )

    if _project_exists(calypso, project_name):
        raise ValueError(
            f"Project '{project_name}' already exists. Run cleanup before creating a new project.",
        )

    udf_role = find_role(calypso, role)
    if udf_role is None:
        raise ValueError(f"Role '{role}' not found. Create it before re-running this workflow.")

    new_user = calypso.users.create(
        email=email,
        name=name or email,
        sendInvite=send_invite,
    )
    calypso.users.assignRole(new_user, udf_role)
    _emit(log, f"Created user {new_user.email} and assigned role '{udf_role.name}'.")

    verification_email_sent = False
    verification_message = None

    user_id = getattr(new_user, "id", None)
    if user_id:
        try:
            calypso.client.users.verification.resend.post(str(user_id))
        except RequestError as exc:
            if exc.statusCode not in {403, 404}:
                raise
            verification_message = (
                "Resend verification email skipped due to API response "
                f"{exc.statusCode}; continuing."
            )
            _emit(log, verification_message)
        else:
            verification_email_sent = True
            verification_message = f"Triggered verification email resend for {new_user.email}."
            _emit(log, verification_message)
    else:
        verification_message = "Skipping verification email resend because the user ID was not returned."
        _emit(log, verification_message)

    project_type_enum = _coerce_project_type(project_type)
    project = calypso.projects.create(
        name=project_name,
        projectType=project_type_enum,
        admins=[new_user],
    )
    _emit(log, f"Created project '{project.name}' ({project.id}) with {new_user.email} as admin.")

    if ensure_admin_cleanup:
        ensure_only_admin(calypso, project, new_user.email or email, log=log)

    return CreateUserProjectResult(
        user=new_user,
        project=project,
        verification_email_sent=verification_email_sent,
        verification_message=verification_message,
    )


def cleanup_user_project(
    email: str,
    *,
    api: CalypsoAI | None = None,
    url: str | None = None,
    token: str | None = None,
    insecure: bool = False,
    log: LogFn | None = None,
) -> CleanupUserProjectResult:
    """Delete the project and user associated with the supplied email."""

    calypso = _ensure_api(api, url=url, token=token, insecure=insecure)
    project_name = f"{email}-project"

    try:
        deleted_projects = clean_projects(calypso, project_name, email)
    except RequestError as exc:
        if exc.statusCode not in {403, 404}:
            raise
        deleted_projects = []
        _emit(log, f"Project cleanup skipped due to API response {exc.statusCode}; continuing.")
    else:
        if deleted_projects:
            _emit(log, f"Deleted projects: {', '.join(deleted_projects)}")
        else:
            _emit(log, "No existing projects needed removal.")

    existing = find_user(calypso, email)
    user_deleted = False

    if existing:
        try:
            calypso.users.delete(existing)
        except RequestError as exc:
            if exc.statusCode not in {403, 404}:
                raise
            _emit(
                log,
                f"User {existing.email} could not be deleted during cleanup (status {exc.statusCode}); continuing.",
            )
        else:
            user_deleted = True
            _emit(log, f"Deleted existing user {existing.email}.")
    else:
        _emit(log, "No existing user matched the email; skipping delete.")

    return CleanupUserProjectResult(
        deleted_projects=deleted_projects,
        user_deleted=user_deleted,
        user=existing if existing else None,
    )
