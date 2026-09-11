"""Writes rows to S3 as Hive-partitioned parquet, via Polars.

Output layout is:

    <prefix>/<col_1>=<value_1>/<col_2>=<value_2>/.../
        year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet

e.g. .../cliente_prefijo=BDO/operacion_prefijo=SAC/year=2026/month=08/day=18/
transcripciones_20260818T153045Z.parquet. `partition_cols` (from the
contract) and year/month/day are all real Hive-style key=value segments.
year/month/day are based on the processing date (UTC, when this function
runs), not any field in the data -- constant for the whole batch, not
contract-configurable.

Partition columns are excluded from each file's own schema (they're already
encoded in the S3 key path) -- `DataFrame.partition_by(..., include_key=False)`
groups the rows by distinct partition-column combinations and drops those
columns from each group in one step. Dedup is NOT done here -- it happens
once on the business rows in transform.py, before they're split into
output_core/output_atts, since output_atts rows don't carry the dedup key
column at all.
"""

import os
import tempfile
from datetime import datetime, timezone
from typing import Optional

import polars as pl

# Maps the contract's column "type" values to Polars dtypes. Every declared
# column gets cast explicitly rather than trusting Polars' row-based type
# inference -- e.g. an all-NULL column (a genuine gap in some contracts,
# like gestion_tipo/gestion_canal) infers as Polars' Null dtype instead of
# the contract's declared type unless cast explicitly. "timestamp" isn't
# listed here -- it needs Series.str.to_datetime(), not a plain cast.
_POLARS_TYPE_BY_CONTRACT_TYPE = {
    "string": pl.Utf8,
    "integer": pl.Int64,
    "json": pl.Utf8,
}


def _cast_columns(df: pl.DataFrame, column_types: dict[str, str]) -> pl.DataFrame:
    exprs = []
    for name, contract_type in column_types.items():
        if contract_type == "timestamp":
            exprs.append(pl.col(name).str.to_datetime(time_zone="UTC", strict=False))
            continue
        target_dtype = _POLARS_TYPE_BY_CONTRACT_TYPE.get(contract_type)
        if target_dtype is not None:
            exprs.append(pl.col(name).cast(target_dtype, strict=False))
    return df.with_columns(exprs) if exprs else df


def write_hive_parquet(
    s3_client,
    rows: list[dict],
    bucket: str,
    prefix: str,
    partition_cols: Optional[list[str]] = None,
    column_types: Optional[dict[str, str]] = None,
    filename_prefix: str = "part",
) -> list[str]:
    """Returns the list of object keys written."""
    if not rows:
        return []

    partition_cols = [c for c in (partition_cols or []) if c in rows[0]]
    # Callers may pass a broader column_types mapping than what these
    # specific rows actually carry (e.g. the contract's full set of
    # technical column types, reused across two different output shapes)
    # -- only cast columns that actually exist here.
    column_types = {k: v for k, v in (column_types or {}).items() if k in rows[0]}
    prefix = prefix.rstrip("/")

    now = datetime.now(timezone.utc)
    date_path = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
    filename = f"{filename_prefix}_{now.strftime('%Y%m%dT%H%M%SZ')}.parquet"

    df = _cast_columns(pl.DataFrame(rows, infer_schema_length=None), column_types)

    if partition_cols:
        groups = df.partition_by(partition_cols, as_dict=True, include_key=False)
    else:
        groups = {(): df}

    written_keys = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for i, (combo, group_df) in enumerate(groups.items()):
            partition_path = "/".join(f"{col}={value}" for col, value in zip(partition_cols, combo))
            key_prefix = (
                f"{prefix}/{partition_path}/{date_path}" if partition_path else f"{prefix}/{date_path}"
            )

            local_path = os.path.join(tmp_dir, f"part_{i}.parquet")
            group_df.write_parquet(local_path)

            key = f"{key_prefix}/{filename}"
            with open(local_path, "rb") as fh:
                s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
            written_keys.append(key)

    return written_keys
