from __future__ import annotations

import sys
import requests as requests_http
from .config import GlassFlowConfig


class APIClient:
    glassflow_config = GlassFlowConfig()

    def __init__(self):
        super().__init__()
        self.client = requests_http.Session()

    def _get_headers2(self) -> dict:
        headers = {"Accept": "application/json",
                   "Gf-Client": self.glassflow_config.glassflow_client,
                   "User-Agent": self.glassflow_config.user_agent, "Gf-Python-Version": (
                f"{sys.version_info.major}."
                f"{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            )}
        return headers
