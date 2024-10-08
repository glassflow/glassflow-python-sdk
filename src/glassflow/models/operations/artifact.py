from __future__ import annotations

import dataclasses

from .base import BasePipelineManagementRequest


@dataclasses.dataclass
class GetArtifactRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class PostArtifactRequest(BasePipelineManagementRequest):
    file: str | None = dataclasses.field(
        default=None,
        metadata={
            "multipart_form": {
                "field_name": "file",
            }
        },
    )
    requirementsTxt: str | None = dataclasses.field(default=None)
