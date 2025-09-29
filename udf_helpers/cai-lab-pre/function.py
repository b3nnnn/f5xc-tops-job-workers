"""
Reverse engineering Kevin's stuff is always fun

This will pave/nuke a CAI account/project for a given email
"""
from __future__ import annotations
import json
import os
import time
from typing import Callable

import boto3

from calypsoai import RequestError
from caistuff.user_project_actions import cleanup_user_project, create_user_project

def provision(
    email: str,
    *,
    token: str,
    url: str | None = None,
    insecure: bool = False,
    log: Callable[[str], None] | None = None,
) -> None:
    """Create a user/project pair for the supplied email."""

    log_fn = log or print

    try:
        result = create_user_project(
            email,
            name="UDF User " + email,
            role="UDF-user",
            project_type="chat",
            send_invite=True,
            token=token,
            url=url,
            insecure=insecure,
            log=lambda msg: log_fn(f"[create] {msg}"),
        )
        log_fn(f"Project ID: {result.project.id}")
    except RequestError as exc:
        log_fn(f"Failed to create: {exc}")


def tidy(
    email: str,
    *,
    token: str,
    url: str | None = None,
    insecure: bool = False,
    log: Callable[[str], None] | None = None,
) -> None:
    """Clean up any project/user that matches the supplied email."""

    log_fn = log or print

    try:
        cleanup = cleanup_user_project(
            email,
            token=token,
            url=url,
            insecure=insecure,
            log=lambda msg: log_fn(f"[cleanup] {msg}"),
        )
        log_fn(f"Removed projects: {cleanup.deleted_projects}")
    except RequestError as exc:
        log_fn(f"Failed to clean up: {exc}")

def get_parameters(parameters: list, region_name: str = "us-east-1") -> dict:
    """
    Fetch parameters from AWS Parameter Store.
    """
    try:
        aws = boto3.session.Session()
        ssm = aws.client("ssm", region_name=region_name)
        response = ssm.get_parameters(Names=parameters, WithDecryption=True)
        result = {param["Name"].split("/")[-1]: param["Value"] for param in response["Parameters"]}
        return result
    except Exception as e:
        raise RuntimeError(f"Failed to fetch parameters: {e}") from e


def validate_payload(payload: dict):
    """
    Validate the payload for required fields.
    """
    required_fields = ["ssm_base_path", "first_name", "last_name", "email"]
    missing_fields = [field for field in required_fields if field not in payload]

    if missing_fields:
        raise RuntimeError(f"Missing required fields in payload: {', '.join(missing_fields)}")


def cai_nukepave(payload, token, log: Callable[[str], None] | None = None):
    """
    Nuke/Pave CAI user and project setup
    """
    log_fn = log or print
    email = payload['email']

    log_fn(f"Starting cleanup for {email}")
    tidy(email=email, token=token, log=log_fn)
    log_fn("Waiting for cleanup to settle")
    time.sleep(5)  # Wait a bit to ensure cleanup is processed
    log_fn(f"Provisioning user/project for {email}")
    provision(email=email, token=token, log=log_fn)
    log_fn("Provisioning complete")

def main(payload: dict):
    """
    Main function to process the payload and pave/nuke the CAI account, project
    """
    try:
        log_messages: list[str] = []

        def log(message: str) -> None:
            log_messages.append(message)
            print(message)

        validate_payload(payload)

        env = os.getenv("ENV")
        if not env:
            raise RuntimeError("Missing required environment variable: ENV")

        ssm_base_path = payload["ssm_base_path"]
        email = payload["email"]
        first_name = payload["first_name"]
        last_name = payload["last_name"]

        #region = boto3.session.Session().region_name
        region = "us-east-1"
        log(f"Processing payload for {first_name} {last_name} <{email}>")
        log(f"Fetching parameters from {ssm_base_path} in {region}")
        params = get_parameters(
            [
                f"{ssm_base_path}/cai-token"
            ],
            region_name=region,
        )
        log("Successfully retrieved CAI parameters")
        try:
            cai_nukepave(payload=payload, token=params['cai-token'], log=log)
        except Exception as e: 
            raise RuntimeError(f"Failed to pave/nuke CAI account/project: {e}") from e

        res = {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "email": email,
                    "messages": log_messages,
                }
            ),
        }


    except Exception as e:
        err = {
            "statusCode": 500,
            "body": f"Error: {e}"
        }
        print(err)
        raise RuntimeError(err) from e

    return res


def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    """
    return main(event)


if __name__ == "__main__":
    # Simulated direct payload for local testing
    test_payload = {
        "ssm_base_path": "/tenantOps-dev/cai-lab",
        "email": "",
        "first_name": "Test",
        "last_name": "User"
    }
    main(test_payload)
