from __future__ import annotations

import dataclasses

from .base import BasePipelineDataRequest, BasePipelineManagementRequest


@dataclasses.dataclass
class StatusAccessTokenRequest(BasePipelineDataRequest):
    """Request check the status of an access token

    Attributes:
        pipeline_id: The id of the pipeline
        organization_id: The id of the organization
        x_pipeline_access_token: The access token of the pipeline

    """

    pass


@dataclasses.dataclass
class ListAccessTokensRequest(BasePipelineManagementRequest):
    page_size: int = 50
    page: int = 1
