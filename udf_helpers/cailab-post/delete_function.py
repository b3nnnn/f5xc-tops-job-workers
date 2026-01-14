"""
Delete a CAI demo org by looking up the org ID from DynamoDB.
"""
from __future__ import annotations

import json
import os
from typing import Callable

import boto3
from boto3.dynamodb.conditions import Attr
import requests

from function import add_udflab_tag, get_parameters, validate_payload
from function import DEFAULT_CALYPSOAI_URL, STATE_TABLE_NAME, _resolve_cai_url


def _lookup_org_id(
    *,
    email: str,
    petname: str,
    region_name: str = "us-east-1",
) -> str | None:
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(STATE_TABLE_NAME)
    matches: list[dict] = []
    start_key: dict | None = None

    while True:
        scan_kwargs = {
            "FilterExpression": Attr("email").eq(email) & Attr("petname").eq(petname),
            "Limit": 100,
        }
        if start_key:
            scan_kwargs["ExclusiveStartKey"] = start_key
        response = table.scan(**scan_kwargs)
        matches.extend(response.get("Items", []))
        if len(matches) > 1:
            break
        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            break

    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(f"Multiple orgs found for {email}/{petname}.")
    return matches[0].get("org_id")


def _delete_org(
    org_id: str,
    *,
    token: str,
    url: str | None = None,
    insecure: bool = False,
    log: Callable[[str], None] | None = None,
) -> None:
    log_fn = log or print
    base_url = _resolve_cai_url(url)
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.delete(
        f"{base_url}/backend/v1/orgs/{org_id}",
        headers=headers,
        json={"confirmOrgId": org_id},
        verify=not insecure,
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Org delete failed ({response.status_code}): {response.text}")
    log_fn(f"Deleted org {org_id}")


def main(payload: dict):
    """
    Main function to delete a demo org from the supplied email + petname.
    """
    petname = payload.get("petname")
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
        email = add_udflab_tag(payload["email"])

        region = "us-east-1"
        log(f"Processing delete payload for <{email}>")
        log(f"Fetching parameters from {ssm_base_path} in {region}")
        params = get_parameters(
            [f"{ssm_base_path}/cai-token"],
            region_name=region,
        )
        log("Successfully retrieved CAI parameters")

        org_id = _lookup_org_id(email=email, petname=petname, region_name=region)
        if not org_id:
            return {
                "statusCode": 404,
                "body": json.dumps(
                    {
                        "email": email,
                        "petname": petname,
                        "messages": log_messages + ["No org id found for the email/petname pair."],
                    }
                ),
            }

        _delete_org(
            org_id,
            token=params["cai-token"],
            url=DEFAULT_CALYPSOAI_URL,
            log=log,
        )

        res = {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "email": email,
                    "petname": petname,
                    "org_id": org_id,
                    "messages": log_messages,
                }
            ),
        }
        if petname:
            log(petname)

    except Exception as e:
        err = {"statusCode": 500, "body": f"Error: {e}"}
        print(err)
        if petname:
            print(petname)
        return err

    return res


def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    """
    return main(event)
