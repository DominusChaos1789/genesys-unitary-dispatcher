"""Loads the data contracts (transcripcion.json) that drive the contracts
process: which bucket/prefix to read from, how to rename columns, how to
compute technical/audit columns, where the two parquet outputs (output_core,
output_atts) are written, and which Genesys organization the provider's
conversations belong to."""

from dataclasses import dataclass

import src.s3_utils as s3_utils
from src.config import Settings

# technical_columns entries in the contract JSON only carry calculus_type/
# description/columns/delimiter -- no "type" field. This is the Lambda's own
# fixed knowledge of what type each one is cast to on write; anything not
# listed here defaults to "string".
_TECHNICAL_COLUMN_TYPES = {
    "ejecucion_id": "string",
    "registro_id": "string",
    "registro_hash": "string",
    "cliente_prefijo": "string",
    "operacion_prefijo": "string",
    "operacion_segmento": "string",
    "operacion_proceso": "string",
    "base_id": "string",
    "base_grupo": "string",
    "base_fecha_id": "string",
    "base_proceso": "string",
    "base_nombre": "string",
    "cargue_id": "string",
    "cargue_fecha": "timestamp",
    "cargue_fecha_id": "string",
    "archivo_fecha": "timestamp",
    "archivo_fecha_id": "string",
    "parquet_nombre": "string",
    "canal_id": "string",
    "canal_proveedor": "string",
    "atributo_nombre": "string",
    "atributo_tipo": "string",
    "atributo_posicion": "integer",
    "atributo_valor": "string",
}


@dataclass(frozen=True)
class Contract:
    raw: dict

    @property
    def source_bucket_logical(self) -> str:
        return self.raw["source"]["bucket_name"]

    @property
    def source_prefix(self) -> str:
        return self.raw["source"]["prefix_pattern"]

    @property
    def columns(self) -> list[dict]:
        return self.raw["columns"]

    @property
    def transformations(self) -> list[dict]:
        return self.raw.get("transformations", [])

    @property
    def partition_by(self) -> list[str]:
        return self.raw.get("output", {}).get("iceberg", {}).get("partition_by", [])

    @property
    def timestamp_columns(self) -> list[str]:
        return [c["name"] for c in self.columns if c["type"] == "timestamp"]

    @property
    def column_types(self) -> dict[str, str]:
        return {c["name"]: c["type"] for c in self.columns}

    @property
    def deduplication(self) -> dict:
        return self.raw.get("deduplication", {})

    @property
    def technical_columns(self) -> dict:
        return self.raw.get("technical_columns", {})

    @property
    def technical_column_types(self) -> dict[str, str]:
        # Union of every technical column name referenced by either output, not
        # just the ones with a calculus_type defined -- a name can appear in
        # technical_col_keep without a matching technical_columns entry (a gap
        # in the contract itself), and it still needs a real declared type
        # instead of whatever the parquet writer infers for an all-NULL column.
        names = (
            set(self.technical_columns)
            | set(self.output_core_technical_columns)
            | set(self.output_atts_technical_columns)
        )
        return {name: _TECHNICAL_COLUMN_TYPES.get(name, "string") for name in names}

    def _output(self, key: str) -> dict:
        return self.raw[key]

    @property
    def output_core_bucket_logical(self) -> str:
        return self._output("output_core")["bucket_name"]

    @property
    def output_core_prefix(self) -> str:
        return self._output("output_core")["prefix_pattern"]

    @property
    def output_core_filename(self) -> str:
        return self._output("output_core")["output_name_file"]

    @property
    def output_core_technical_columns(self) -> list[str]:
        return self._output("output_core").get("technical_col_keep", [])

    @property
    def output_atts_bucket_logical(self) -> str:
        return self._output("output_atts")["bucket_name"]

    @property
    def output_atts_prefix(self) -> str:
        return self._output("output_atts")["prefix_pattern"]

    @property
    def output_atts_filename(self) -> str:
        return self._output("output_atts")["output_name_file"]

    @property
    def output_atts_technical_columns(self) -> list[str]:
        return self._output("output_atts").get("technical_col_keep", [])

    @property
    def gne_organization_id(self) -> str:
        """Which Genesys organization this provider's conversations belong to
        ("org-1".."org-4"). Each contract carries its own, so conversations
        from different contracts can need different OAuth tokens."""
        return self.raw["genesys_cloud_organization"]

    @property
    def server_key(self) -> str:
        """The `servers` key in the SSM params for this contract's
        organization -- the params use "org_3" where the contract says "org-3"."""
        return self.gne_organization_id.replace("-", "_")


def list_contract_keys(s3_client, settings: Settings) -> list[str]:
    """Every contract this run should process.

    CONTRACT_KEY pins the run to one contract; otherwise every .json under
    CONTRACTS_PREFIX is a contract, one per provider/operation pair -- e.g.
    .../transcripciones/bdo/sac/transcripcion.json, .../pel/sac/..., .../bpp/cob/...
    Sorted so the processing order is stable run to run.
    """
    if settings.contract_key:
        return [settings.contract_key]
    return sorted(s3_utils.list_json_keys(s3_client, settings.resources_bucket, settings.contracts_prefix))


def load_contract(s3_client, settings: Settings, contract_key: str = "") -> Contract:
    key = contract_key or settings.contract_key
    return Contract(raw=s3_utils.read_json(s3_client, settings.resources_bucket, key))
