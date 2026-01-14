"""
Create a CAI demo org and return its ID for UDF workflows.
"""
from __future__ import annotations
import json
import os
import time
from typing import Callable

import boto3
import requests


DEFAULT_CALYPSOAI_URL = "https://aisec.f5se.com"
STATE_TABLE_NAME = "tops-udf-lab-deployment-cai-state-dev"


def add_udflab_tag(email: str) -> str:
    """Ensure the email local-part includes the +udflab tag."""
    if "@" not in email:
        raise ValueError(f"Invalid email address: {email}")

    local_part, domain = email.split("@", 1)
    if "+udflab" in local_part:
        return email

    return f"{local_part}+udflab@{domain}"

def _resolve_cai_url(url: str | None) -> str:
    resolved = url or os.environ.get("CALYPSOAI_URL") or DEFAULT_CALYPSOAI_URL
    return resolved.rstrip("/")

def _create_demo_org_and_validate_org(
    email: str,
    *,
    token: str,
    url: str | None = None,
    insecure: bool = False,
    log: Callable[[str], None] | None = None,
) -> str:
    log_fn = log or print
    base_url = _resolve_cai_url(url)
    headers = {"Authorization": f"Bearer {token}"}
    org_payload = {
        "name": f"{email}'s org",
        "ownerName": email,
        "ownerEmail": email,
        "demo": {"withData": True},
    }

    response = requests.post(
        f"{base_url}/backend/v1/orgs",
        headers=headers,
        json=org_payload,
        verify=not insecure,
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Org creation failed ({response.status_code}): {response.text}")

    data = response.json()
    org_id = data.get("id")
    org_token = (data.get("token") or {}).get("value")
    if not org_token:
        raise RuntimeError("Org creation response missing token value.")

    log_fn(f"Created demo org {org_id or '(unknown id)'} for {email}")

    _run_org_post_creation_requests(
        org_token,
        base_url=base_url,
        insecure=insecure,
        log=log_fn,
    )
    return org_id

def _run_org_post_creation_requests(
    org_token: str,
    *,
    base_url: str,
    insecure: bool,
    log: Callable[[str], None] | None,
    requests_fns: list[Callable[..., requests.Response]] | None = None,
) -> None:
    requests_to_run = requests_fns or [_fetch_org_details]
    for request_fn in requests_to_run:
        _run_with_auth_retry(
            request_fn,
            org_token=org_token,
            base_url=base_url,
            insecure=insecure,
            log=log,
        )

def _run_with_auth_retry(
    request_fn: Callable[..., requests.Response],
    *,
    org_token: str,
    base_url: str,
    insecure: bool,
    log: Callable[[str], None] | None,
    max_attempts: int = 5,
    delay_seconds: float = 1.0,
    success_statuses: set[int] | None = None,
) -> None:
    log_fn = log or print
    allowed_successes = success_statuses or {200}
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = request_fn(
                org_token=org_token,
                base_url=base_url,
                insecure=insecure,
                log=log,
            )
            if response.status_code in allowed_successes:
                return
            if response.status_code not in {401, 403}:
                raise RuntimeError(
                    f"Org post-creation request failed ({response.status_code}): {response.text}"
                )
            last_error = RuntimeError(
                f"Auth not ready ({response.status_code}): {response.text}"
            )
        except requests.RequestException as exc:
            last_error = exc
        if attempt < max_attempts:
            log_fn(f"Auth not ready yet; retrying org request ({attempt}/{max_attempts})")
            time.sleep(delay_seconds)
    raise RuntimeError(f"Org post-creation request failed after {max_attempts} attempts: {last_error}")

def _fetch_org_details(
    *,
    org_token: str,
    base_url: str,
    insecure: bool,
    log: Callable[[str], None] | None,
) -> requests.Response:
    log_fn = log or print
    response = requests.get(
        f"{base_url}/backend/v1/org",
        headers={"Authorization": f"Bearer {org_token}"},
        verify=not insecure,
        timeout=20,
    )
    if response.status_code < 400:
        log_fn("Fetched org details")
    return response

def provision(
    email: str,
    *,
    token: str,
    url: str | None = None,
    insecure: bool = False,
    log: Callable[[str], None] | None = None,
) -> str | None:
    """Create the demo org for the supplied email and return its id."""

    log_fn = log or print

    try:
        org_id = _create_demo_org_and_validate_org(
            email,
            token=token,
            url=url,
            insecure=insecure,
            log=log_fn,
        )
        if org_id:
            log_fn(f"Demo org id: {org_id}")
        return org_id
    except Exception as exc:
        log_fn(f"Failed to create: {exc}")
        return None



def get_parameters(parameters: list, region_name: str = "us-east-1") -> dict:
    """
    Fetch parameters from AWS Parameter Store.
    """
    try:
        aws = boto3.session.Session()
        ssm = aws.client("ssm", region_name=region_name)
        response = ssm.get_parameters(Names=parameters, WithDecryption=True)
        invalid = response.get("InvalidParameters") or []
        if invalid:
            raise RuntimeError(f"Missing parameters in SSM: {', '.join(invalid)}")
        result = {param["Name"].split("/")[-1]: param["Value"] for param in response["Parameters"]}
        return result
    except Exception as e:
        raise RuntimeError(f"Failed to fetch parameters: {e}") from e


def record_provisioned_org(
    org_id: str,
    *,
    email: str,
    petname: str,
    region_name: str = "us-east-1",
) -> None:
    """Persist org state for later cleanup."""
    try:
        dynamodb = boto3.resource("dynamodb", region_name=region_name)
        table = dynamodb.Table(STATE_TABLE_NAME)
        table.put_item(
            Item={
                "org_id": org_id,
                "email": email,
                "petname": petname,
            }
        )
    except Exception as e:
        raise RuntimeError(f"Failed to record org state: {e}") from e


def validate_payload(payload: dict):
    """
    Validate the payload for required fields.
    """
    required_fields = ["ssm_base_path", "email", "petname"]
    missing_fields = [field for field in required_fields if field not in payload]

    if missing_fields:
        raise RuntimeError(f"Missing required fields in payload: {', '.join(missing_fields)}")
    empty_fields = [field for field in required_fields if not str(payload.get(field, "")).strip()]
    if empty_fields:
        raise RuntimeError(f"Empty required fields in payload: {', '.join(empty_fields)}")




def main(payload: dict):
    """
    Main function to process the payload and create a demo org.
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
        log(f"Processing payload for <{email}>")
        log(f"Fetching parameters from {ssm_base_path} in {region}")
        params = get_parameters(
            [
                f"{ssm_base_path}/cai-token"
            ],
            region_name=region,
        )
        log("Successfully retrieved CAI parameters")
        try:
            org_id = provision(email=email, token=params['cai-token'], url=DEFAULT_CALYPSOAI_URL, log=log)
        except Exception as e:
            raise RuntimeError(f"Failed to create CAI demo org: {e}") from e

        if org_id:
            log(f"Recording org {org_id} in DynamoDB table {STATE_TABLE_NAME}")
            record_provisioned_org(
                org_id,
                email=email,
                petname=petname,
                region_name=region,
            )

        res = {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "email": email,
                    "org_id": org_id,
                    "messages": log_messages,
                }
            ),
        }
        if petname:
            log(petname)


    except Exception as e:
        err = {
            "statusCode": 500,
            "body": f"Error: {e}"
        }
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


if __name__ == "__main__":
    # Simulated direct payload for local testing
    test_payload = {
        "ssm_base_path": "/tenantOps-dev/cai-lab",
        "email": "test@test.com",
        "petname": "cow"
    }
    main(test_payload)
