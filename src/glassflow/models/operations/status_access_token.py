from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class StatusAccessTokenRequest:
    """Request check the status of an access token

    Attributes:
        pipeline_id: The id of the pipeline
        x_pipeline_access_token: The access token of the pipeline

    """

    pipeline_id: str = dataclasses.field(
        metadata={
            "path_param": {
                "field_name": "pipeline_id",
                "style": "simple",
                "explode": False,
            }
        }
    )
    x_pipeline_access_token: str = dataclasses.field(
        default=None,
        metadata={
            "header": {
                "field_name": "X-PIPELINE-ACCESS-TOKEN",
                "style": "simple",
                "explode": False,
            }
        },
    )
