# -*- coding: utf-8 -*-
"""AWS Systems Manager (SSM) Parameter Store loader.

Retrieves the Genesys API configuration stored under a hierarchical path
(e.g. /augusta-nexa-dev/genesys/api), handling pagination, SecureString
decryption and JSON parsing, plus the matching Secrets Manager entries that
hold the per-organization OAuth credentials.

Author(s): Felipe Segundo Abril Bermúdez
"""

import json
from typing import Any, Dict

import boto3
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AWSConfigLoader:
    """Loads every parameter under a given SSM path, plus the secrets that
    share that path prefix, returning them keyed by their name relative to
    the path (so "/augusta-nexa-dev/genesys/api/servers" -> "servers")."""

    def __init__(
        self,
        path: str,
        region_name: str = "us-east-2",
        profile_name: str | None = None,
        verify: bool | str = True,
    ) -> None:
        self.path = path.rstrip("/")  # Avoid slashes at the end of the path
        self.ssm = boto3.client("ssm", region_name=region_name, verify=verify)
        self.secrets = boto3.client("secretsmanager", region_name=region_name, verify=verify)

        # Enables local testing environment
        if not verify and profile_name is not None:
            session = boto3.Session(profile_name=profile_name)
            self.ssm = session.client("ssm", region_name=region_name, verify=verify)
            self.secrets = session.client("secretsmanager", region_name=region_name, verify=verify)

    def _parse_value(self, value: str) -> Any:
        """JSON-decode a parameter value, falling back to the raw string."""
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    def _normalize_key(self, name: str) -> str:
        """/env/service/config/client_a -> config/client_a"""
        return name.replace(self.path + "/", "")

    def get_all_parameters(self) -> Dict[str, Any]:
        paginator = self.ssm.get_paginator("get_parameters_by_path")
        params: Dict[str, Any] = {}

        for page in paginator.paginate(Path=self.path, Recursive=True, WithDecryption=True):
            for param in page.get("Parameters", []):
                key = self._normalize_key(param["Name"])
                params[key] = self._parse_value(param["Value"])

        return params

    def get_all_secrets(self) -> Dict[str, Any]:
        paginator = self.secrets.get_paginator("list_secrets")
        secrets: Dict[str, Any] = {}

        for page in paginator.paginate():
            for secret in page.get("SecretList", []):
                name = secret["Name"]
                if not name.startswith(self.path):
                    continue
                key = self._normalize_key(name)

                try:
                    response = self.secrets.get_secret_value(SecretId=name)
                    secrets[key] = {
                        "value": self._parse_value(response.get("SecretString")),
                        "arn": response.get("ARN"),
                        "created_date": str(secret.get("CreatedDate")),
                    }
                except Exception as exc:  # noqa: BLE001 -- reported per secret
                    secrets[key] = {"error": str(exc)}

        return secrets
