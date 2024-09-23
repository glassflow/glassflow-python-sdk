import sys
from abc import ABC
from typing import Optional

import requests as requests_http

from .config import GlassFlowConfig
from .models import errors
from .models.operations.base import BaseRequest, BaseResponse
from .utils import utils as utils


class APIClient(ABC):
    glassflow_config = GlassFlowConfig()

    def __init__(self):
        super().__init__()
        self.client = requests_http.Session()

    def _get_headers(
            self, request: BaseRequest, req_content_type: Optional[str] = None
    ) -> dict:
        headers = utils.get_req_specific_headers(request)
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

    def request(self, method: str, endpoint: str, request: BaseRequest) -> BaseResponse:
        request_type = type(request)

        url = utils.generate_url(
            request_type,
            self.glassflow_config.server_url,
            endpoint,
            request,
        )

        req_content_type, data, form = utils.serialize_request_body(
            request, request_type, "request_body", False, True, "json"
        )
        if method == "GET":
            data = None

        headers = self._get_headers(request, req_content_type)
        query_params = utils.get_query_params(request_type, request)

        # make the request
        http_res = self.client.request(
            method, url=url, params=query_params, headers=headers, data=data, files=form)
        content_type = http_res.headers.get("Content-Type")

        res = BaseResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )

        if http_res.status_code in [400, 500]:
            out = utils.unmarshal_json(http_res.text, errors.Error)
            out.raw_response = http_res
            raise out
        elif http_res.status_code == 429:
            pass
        elif 400 < http_res.status_code < 600:
            raise errors.ClientError(
                "API error occurred",
                http_res.status_code,
                http_res.text,
                http_res
            )

        return res
