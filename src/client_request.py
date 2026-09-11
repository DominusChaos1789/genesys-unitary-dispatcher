"""Minimal HTTP client for the Genesys Cloud API.

Request Unitary only uses it to exchange OAuth client credentials for a
bearer token -- the flow's own calls are executed downstream by Unitary
Status and Unitary Download.
"""

import logging

import requests

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


class GenesysClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def call(self, endpoint_config):
        url = self.base_url + endpoint_config["url"]
        method = endpoint_config["method"]
        headers = endpoint_config["headers"]

        params = endpoint_config.get("params_template") or None
        json_body = endpoint_config.get("body_template") or None

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=json_body,
            )

            LOGGER.info("Se consume el metodo %s, con url %s -> %s", method, url, response.status_code)

            if response.status_code == 404:
                LOGGER.info("URL no encontrada, lo ignoro: %s ", url)
            else:
                response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError:
            LOGGER.error("Error en metodo %s %s", method, url)
            raise
