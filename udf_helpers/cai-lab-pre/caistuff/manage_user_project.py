#!/usr/bin/env python3
"""
Example CalypsoAI automation.

Deletes any project named '<email>-project' where <email> is an admin, removes
the user if present, then recreates the user with the requested role and
provisions a fresh project with that user as an administrator.

Prerequisites:
  • Set CALYPSOAI_TOKEN (and optionally CALYPSOAI_URL), or pass them on the CLI.
"""
from __future__ import annotations

import argparse
import os
import sys

from calypsoai import CalypsoAI, RequestError, datatypes as dt
from user_project_common import clean_projects, ensure_only_admin, find_role, find_user

CALYPSOAI_URL = "https://www.us1.calypsoai.app"

def run() -> None:
    parser = argparse.ArgumentParser(description="Provision CalypsoAI user + project.")
    parser.add_argument("email", help="Target user email.")
    parser.add_argument("--name", help="Display name (defaults to the email).")
    parser.add_argument("--role", default="UDF-user", help="Org role to assign after creation.")
    parser.add_argument(
        "--project-type",
        choices=["app", "bot", "chat"],
        default="chat",
        help="Project type to create.",
    )
    parser.add_argument("--url", default=os.environ.get("CALYPSOAI_URL"), help="CalypsoAI base URL.")
    parser.add_argument("--token", default=os.environ.get("CALYPSOAI_TOKEN"), help="CalypsoAI API token.")
    parser.add_argument(
        "--send-invite",
        action="store_true",
        help="Send the CalypsoAI invitation email when creating the user.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification (only for trusted dev environments).",
    )
    args = parser.parse_args()

    api = CalypsoAI(url=args.url, token=args.token, verify_tls=not args.insecure)
    project_name = f"{args.email}-project"

    try:
        deleted_projects = clean_projects(api, project_name, args.email)
    except RequestError as exc:
        if exc.statusCode not in {403, 404}:
            raise
        print(f"Project cleanup skipped due to API response {exc.statusCode}; continuing.")
        deleted_projects = []
    if deleted_projects:
        print(f"Deleted projects: {', '.join(deleted_projects)}")
    else:
        print("No existing projects needed removal.")

    if existing := find_user(api, args.email):
        try:
            api.users.delete(existing)
        except RequestError as exc:
            if exc.statusCode not in {403, 404}:
                raise
            print(f"User {existing.email} could not be deleted during cleanup (status {exc.statusCode}); continuing.")
        else:
            print(f"Deleted existing user {existing.email}.")
    else:
        print("No existing user matched the email; skipping delete.")

    udf_role = find_role(api, args.role)
    if udf_role is None:
        raise SystemExit(f"Role '{args.role}' not found. Create it before re-running this script.")

    new_user = api.users.create(
        email=args.email,
        name=args.name or args.email,
        sendInvite=args.send_invite,
    )
    api.users.assignRole(new_user, udf_role)
    print(f"Created user {new_user.email} and assigned role '{udf_role.name}'.")

    project_type = dt.ProjectType[args.project_type.upper()]
    project = api.projects.create(
        name=project_name,
        projectType=project_type,
        admins=[new_user],
    )
    print(f"Created project '{project.name}' ({project.id}) with {new_user.email} as admin.")

    ensure_only_admin(api, project, new_user.email)


if __name__ == "__main__":
    try:
        run()
    except RequestError as exc:
        sys.exit(f"CalypsoAI request failed: {exc}")
