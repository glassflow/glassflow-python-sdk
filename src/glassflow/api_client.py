from __future__ import annotations

import sys
import requests as requests_http
from .config import GlassFlowConfig
from pathlib import PurePosixPath


class APIClient:
    glassflow_config = GlassFlowConfig()

    def __init__(self):
        super().__init__()
        self.client = requests_http.Session()

    def _get_core_headers(self) -> dict:
        headers = {"Accept": "application/json",
                   "Gf-Client": self.glassflow_config.glassflow_client,
                   "User-Agent": self.glassflow_config.user_agent, "Gf-Python-Version": (
                f"{sys.version_info.major}."
                f"{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            )}
        return headers

    def _request(
        self,
        method,
        endpoint,
        request_headers=None,
        json=None,
        request_query_params=None,
        files=None,
        data=None
    ):
        # updated request method that knows the request details and does not use utils
        # Do the https request. check for errors. if no errors, return the raw response http object that the caller can
        # map to a pydantic object
        headers = self._get_core_headers()
        if request_headers:
            headers.update(request_headers)

        url = (
            f"{self.glassflow_config.server_url.rstrip('/')}/{PurePosixPath(endpoint)}"
        )

        http_res = self.client.request(
            method, url=url, params=request_query_params, headers=headers, json=json, files=files, data=data
        )
        http_res.raise_for_status()
        return http_res
