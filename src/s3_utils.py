"""Thin S3 JSON helpers, kept apart from business logic so they're easy to mock."""

import json
from typing import Any


def read_json(s3_client, bucket: str, key: str) -> Any:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    # utf-8-sig: resource files edited on Windows sometimes carry a BOM.
    return json.loads(body.decode("utf-8-sig"))


def write_json(s3_client, bucket: str, key: str, payload: Any) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
