import sys
from dataclasses import dataclass
from typing import Optional

import requests as requests_http

from .config import GlassFlowConfig
from .utils import get_req_specific_headers


class APIClient:
    glassflow_config = GlassFlowConfig()

    def __init__(self):
        self.client = requests_http.Session()

    def _get_headers(
            self, request: dataclass, req_content_type: Optional[str] = None
    ) -> dict:
        headers = get_req_specific_headers(request)
        headers["Accept"] = "application/json"
        headers["Gf-Client"] = self.glassflow_config.glassflow_client
        headers["User-Agent"] = self.glassflow_config.user_agent
        headers["Gf-Python-Version"] = (
            f"{sys.version_info.major}."
            f"{sys.version_info.minor}."
            f"{sys.version_info.micro}"
        )

        if req_content_type and req_content_type not in (
                "multipart/form-data",
                "multipart/mixed",
        ):
            headers["content-type"] = req_content_type

        return headers
