"""GlassFlow Python Client to interact with GlassFlow API"""

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


class GlassFlowClient(APIClient):
    """GlassFlow Client to interact with GlassFlow API and manage pipelines and other resources

    Attributes:
        client: requests.Session object to make HTTP requests to GlassFlow API
        glassflow_config: GlassFlowConfig object to store configuration
        organization_id: Organization ID of the user. If not provided, the default organization will be used

    """

    def __init__(self, personal_access_token: str = None, organization_id: str = None) -> None:
        """Create a new GlassFlowClient object

        Args:
            personal_access_token: GlassFlow Personal Access Token
            organization_id: Organization ID of the user. If not provided, the default organization will be used
        """
        super().__init__()
        self.personal_access_token = personal_access_token
        self.organization_id = organization_id
