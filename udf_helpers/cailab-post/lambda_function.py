"""Lambda entry point wrapper."""

from __future__ import annotations

from delete_function import lambda_handler as _lambda_handler


def lambda_handler(event, context):
    """Delegate to the shared handler implementation in delete_function.py."""
    return _lambda_handler(event, context)
