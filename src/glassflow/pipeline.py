from typing import List, Optional

from .client import APIClient
from .models import api, operations, errors


class Pipeline(APIClient):
    def __init__(
            self,
            personal_access_token: str,
            name: Optional[str] = None,
            space_id: Optional[str] = None,
            id: Optional[str] = None,
            source_kind: Optional[str] = None,
            source_config: Optional[str] = None,
            sink_kind: Optional[str] = None,
            sink_config: Optional[str] = None,
            requirements: Optional[str] = None,
            transformation_code: Optional[str] = None,
            transformation_file: Optional[str] = None,
            env_vars: Optional[List[str]] = None,
            state: api.PipelineState = "running",
            organization_id: Optional[str] = None,
            metadata: Optional[dict] = None,
    ):
        super().__init__()
        self.id = id
        self.name = name
        self.space_id = space_id
        self.personal_access_token = personal_access_token
        self.source_kind = source_kind
        self.source_config = source_config
        self.sink_kind = sink_kind
        self.sink_config = sink_config
        self.requirements = requirements
        self.transformation_code = transformation_code
        self.transformation_file = transformation_file
        self.env_vars = env_vars
        self.state = state
        self.organization_id = organization_id
        self.metadata = metadata if metadata is not None else {}
        self.created_at = None
        self.access_tokens = []

        if self.transformation_code is None and self.transformation_file is not None:
            try:
                self.transformation_code = open(self.transformation_file, "r").read()
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Transformation file was not found in "
                    f"{self.transformation_file}")

        if source_kind is not None and self.source_config is not None:
            self.source_connector = api.SourceConnector(
                kind=self.source_kind,
                config=self.source_config,
            )
        elif self.source_kind is None and self.source_config is None:
            self.source_connector = None
        else:
            raise ValueError(
                "Both source_kind and source_config must be provided")

        if self.sink_kind is not None and self.sink_config is not None:
            self.sink_connector = api.SinkConnector(
                kind=sink_kind,
                config=sink_config,
            )
        elif self.sink_kind is None and self.sink_config is None:
            self.sink_connector = None
        else:
            raise ValueError("Both sink_kind and sink_config must be provided")

    def fetch(self):
        if self.id is None:
            raise ValueError(
                "Pipeline id must be provided in order to fetch it's details")

        request = operations.GetPipelineRequest(
            pipeline_id=self.id,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )

        try:
            res = self.request(
                method="GET",
                endpoint=f"/pipelines/{self.id}",
                request=request,
            )
            res_json = res.raw_response.json()
        except errors.ClientError as e:
            if e.status_code == 404:
                raise errors.PipelineNotFoundError(self.id, e.raw_response)
            elif e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response)
            else:
                raise e

        self.name = res_json["name"]
        self.space_id = res_json["space_id"]
        if res_json["source_connector"]:
            self.source_kind = res_json["source_connector"]["kind"]
            self.source_config = res_json["source_connector"]["config"]
        if res_json["sink_connector"]:
            self.sink_kind = res_json["sink_connector"]["kind"]
            self.sink_config = res_json["sink_connector"]["config"]
        self.created_at = res_json["created_at"]
        self.env_vars = res_json["environments"]
        return self

    def create(self):
        create_pipeline = api.CreatePipeline(
            name=self.name,
            space_id=self.space_id,
            transformation_function=self.transformation_code,
            requirements_txt=self.requirements,
            source_connector=self.source_connector,
            sink_connector=self.sink_connector,
            environments=self.env_vars,
            state=self.state,
            metadata=self.metadata,
        )
        if self.name is None:
            raise ValueError(
                "Name must be provided in order to create the pipeline")
        if self.space_id is None:
            raise ValueError(
                "Space_id must be provided in order to create the pipeline")
        if self.transformation_code is None and self.transformation_file is None:
            raise ValueError(
                "Either transformation_code or transformation_file must "
                "be provided")

        request = operations.CreatePipelineRequest(
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
            **create_pipeline.__dict__,
        )

        try:
            base_res = self.request(
                method="POST",
                endpoint=f"/pipelines",
                request=request
            )
            res = operations.CreatePipelineResponse(
                status_code=base_res.status_code,
                content_type=base_res.content_type,
                raw_response=base_res.raw_response,
                **base_res.raw_response.json())
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response)
            else:
                raise e

        self.id = res.id
        self.created_at = res.created_at
        self.access_tokens.append({
            "name": "default", "token": res.access_token
        })
        return self
