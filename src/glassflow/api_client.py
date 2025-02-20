from __future__ import annotations

import sys
from typing import Any

import requests as requests_http

from .config import GlassFlowConfig
from .models import errors


class APIClient:
    glassflow_config = GlassFlowConfig()

    def __init__(self):
        """API client constructor"""
        super().__init__()
        self.client = requests_http.Session()

    def _get_core_headers(self) -> dict:
        headers = {
            "Accept": "application/json",
            "Gf-Client": self.glassflow_config.glassflow_client,
            "User-Agent": self.glassflow_config.user_agent,
            "Gf-Python-Version": (
                f"{sys.version_info.major}."
                f"{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            ),
        }
        return headers

    def _request(
        self,
        method,
        endpoint,
        request_headers=None,
        json=None,
        request_query_params=None,
        files=None,
        data=None,
    ):
        headers = self._get_core_headers()
        if request_headers:
            headers.update(request_headers)

        url = self.glassflow_config.server_url + endpoint

        try:
            http_res = self.client.request(
                method,
                url=url,
                params=request_query_params,
                headers=headers,
                json=json,
                files=files,
                data=data,
            )
            http_res.raise_for_status()
        except requests_http.HTTPError as http_err:
            raise errors.UnknownError(http_err.response) from http_err
        return http_res

    def __eq__(self, other: Any) -> bool:
        vars_self = {k: v for k, v in vars(self).items() if k != "client"}
        vars_other = {k: v for k, v in vars(other).items() if k != "client"}
        return (type(self), vars_self) == (type(other), vars_other)
