"""Thin S3 helpers, kept apart from business logic so they're easy to mock."""

import json
import logging
from typing import Any, Iterable

logger = logging.getLogger(__name__)

JSON_SUFFIX = ".json"
DELETE_BATCH_SIZE = 1000  # the S3 delete_objects limit


def list_json_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    """Every key under `prefix` whose name ends in .json, across however many
    pages the listing needs."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(JSON_SUFFIX):
                keys.append(key)
    return keys


def read_json(s3_client, bucket: str, key: str) -> Any:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    # utf-8-sig: resource files edited on Windows sometimes carry a BOM.
    return json.loads(body.decode("utf-8-sig"))


def read_json_files(s3_client, bucket: str, keys: Iterable[str]) -> tuple[list[Any], list[str], list[str]]:
    """Reads every key as JSON, tolerating individual bad files (empty objects,
    corrupted uploads) instead of failing the whole batch.

    Returns (records, successful_keys, skipped_keys). Skipped keys are logged
    and left untouched in S3 so they can be investigated, rather than deleted
    alongside the good ones.
    """
    records: list[Any] = []
    successful_keys: list[str] = []
    skipped_keys: list[str] = []

    for key in keys:
        try:
            records.append(read_json(s3_client, bucket, key))
            successful_keys.append(key)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Skipping unreadable source file s3://%s/%s: %s", bucket, key, exc)
            skipped_keys.append(key)

    return records, successful_keys, skipped_keys


def write_json(s3_client, bucket: str, key: str, payload: Any) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def delete_objects(s3_client, bucket: str, keys: Iterable[str]) -> list[str]:
    """Deletes keys in batches of DELETE_BATCH_SIZE. Returns the keys actually
    deleted; per-key failures are logged."""
    keys = list(keys)
    deleted: list[str] = []
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[start : start + DELETE_BATCH_SIZE]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": False},
        )
        deleted.extend(obj["Key"] for obj in response.get("Deleted", []))
        for error in response.get("Errors", []):
            logger.error("Failed to delete s3://%s/%s: %s", bucket, error["Key"], error.get("Message"))
    return deleted
