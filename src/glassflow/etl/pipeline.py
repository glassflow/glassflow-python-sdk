from __future__ import annotations

import json
from typing import Any

import yaml
from httpx._models import Response
from pydantic import ValidationError

from . import errors, models
from .api_client import APIClient
from .dlq import DLQ


class Pipeline(APIClient):
    """
    Main class for managing Kafka to ClickHouse pipelines.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(
        self,
        host: str | None = None,
        pipeline_id: str | None = None,
        config: models.PipelineConfig | dict[str, Any] | None = None,
    ):
        """Initialize the Pipeline class.

        Args:
            host: GlassFlow API host
            pipeline_id: ID of the pipeline to create
            config: Pipeline configuration
        """
        super().__init__(host=host)

        if not config and not pipeline_id:
            raise ValueError("Either config or pipeline_id must be provided")
        elif config and pipeline_id:
            raise ValueError("Only one of config or pipeline_id can be provided")

        if pipeline_id is not None:
            self.pipeline_id = pipeline_id

        if config is not None:
            if isinstance(config, dict):
                self.config = models.PipelineConfig.model_validate(config)
            else:
                self.config = config
            self.pipeline_id = self.config.pipeline_id
        else:
            self.config = None

        self._dlq = DLQ(pipeline_id=self.pipeline_id, host=host)

    def get(self) -> Pipeline:
        """Fetch a pipeline by its ID.

        Returns:
            Pipeline: A Pipeline instance for the given ID

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        response = self._request(
            "GET", f"{self.ENDPOINT}/{self.pipeline_id}", event_name="PipelineGet"
        )
        self.config = models.PipelineConfig.model_validate(response.json())
        self._dlq = DLQ(pipeline_id=self.pipeline_id, host=self.host)
        return self

    def create(self) -> Pipeline:
        """Creates a new pipeline with the given config.

        Returns:
            Pipeline: A Pipeline instance for the created pipeline

        Raises:
            PipelineAlreadyExistsError: If pipeline already exists
            PipelineInvalidConfigurationError: If configuration is invalid
            APIError: If the API request fails
        """
        if self.config is None:
            raise ValueError("Pipeline configuration must be provided in constructor")
        try:
            self._request(
                "POST",
                self.ENDPOINT,
                json=self.config.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
                event_name="PipelineCreated",
            )
            return self

        except errors.ForbiddenError as e:
            self._track_event("PipelineCreated", error_type="PipelineAlreadyExists")
            raise errors.PipelineAlreadyExistsError(
                status_code=e.status_code,
                message=f"Pipeline with ID {self.config.pipeline_id} already exists;"
                "delete it first before creating new pipeline or use a"
                "different pipeline ID",
                response=e.response,
            ) from e

    def rename(self, name: str) -> Pipeline:
        """Renames the pipeline with the given name.

        Returns:
            Pipeline: A Pipeline instance for the renamed pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        self._request(
            "PATCH",
            f"{self.ENDPOINT}/{self.pipeline_id}",
            json={"name": name},
            event_name="PipelineRenamed",
        )
        self.config.name = name
        return self

    def update(
        self, config_patch: models.PipelineConfigPatch | dict[str, Any]
    ) -> Pipeline:
        """Updates the pipeline with the given config.

        Args:
            config_patch: Pipeline configuration patch

        Returns:
            Pipeline: A Pipeline instance for the updated pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        raise NotImplementedError("Updating is not implemented")

    def delete(self, terminate: bool = True) -> None:
        """Deletes the pipeline with the given ID.

        Args:
            terminate: Whether to terminate the pipeline (i.e. delete all the pipeline
                components and potentially all the events in the pipeline)

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        if not terminate:
            raise NotImplementedError("Graceful deletion is not implemented")

        if self.config is None:
            self.get()
        endpoint = f"{self.ENDPOINT}/{self.pipeline_id}/terminate"
        self._request("DELETE", endpoint, event_name="PipelineDeleted")

    def pause(self) -> Pipeline:
        """Pauses the pipeline with the given ID.

        Returns:
            Pipeline: A Pipeline instance for the paused pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        raise NotImplementedError("Pausing is not implemented")

    def resume(self) -> Pipeline:
        """Resumes the pipeline with the given ID.

        Returns:
            Pipeline: A Pipeline instance for the resumed pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        raise NotImplementedError("Resuming is not implemented")

    def health(self) -> dict[str, Any]:
        """Get the health of the pipeline.

        Returns:
            dict: Pipeline health
        """
        return self._request(
            "GET",
            f"{self.ENDPOINT}/{self.pipeline_id}/health",
            event_name="PipelineHealth",
        ).json()

    def to_dict(self) -> dict[str, Any]:
        """Convert the pipeline configuration to a dictionary.

        Returns:
            dict: Pipeline configuration as a dictionary
        """
        if not hasattr(self, "config") or self.config is None:
            return {"pipeline_id": self.pipeline_id}

        return self.config.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )

    def to_yaml(self, yaml_path: str) -> None:
        """Save the pipeline configuration to a YAML file.

        Args:
            yaml_path: Path to the YAML file
        """
        with open(yaml_path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False)

    def to_json(self, json_path: str) -> None:
        """Save the pipeline configuration to a JSON file.

        Args:
            json_path: Path to the JSON file
        """
        with open(json_path, "w") as f:
            json.dump(self.to_dict(), f, indent=4)

    @classmethod
    def from_yaml(cls, yaml_path: str, host: str | None = None) -> Pipeline:
        """Create a pipeline from a YAML file.

        Args:
            yaml_path: Path to the YAML file
            host: GlassFlow API host

        Returns:
            Pipeline: A Pipeline instance for the created pipeline
        """
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)
        return cls(config=config, host=host)

    @classmethod
    def from_json(cls, json_path: str, host: str | None = None) -> Pipeline:
        """Create a pipeline from a JSON file.

        Args:
            json_path: Path to the JSON file
            host: GlassFlow API host

        Returns:
            Pipeline: A Pipeline instance for the created pipeline
        """
        with open(json_path, "r") as f:
            config = json.load(f)
        return cls(config=config, host=host)

    @staticmethod
    def validate_config(config: dict[str, Any]) -> bool:
        """
        Validate a pipeline configuration.

        Args:
            config: Dictionary containing the pipeline configuration

        Returns:
            True if the configuration is valid

        Raises:
            ValueError: If the configuration is invalid
            ValidationError: If the configuration fails Pydantic validation
        """
        try:
            models.PipelineConfig.model_validate(config)
            return True
        except ValidationError as e:
            raise e
        except ValueError as e:
            raise e

    @property
    def dlq(self) -> DLQ:
        """Get the DLQ (Dead Letter Queue) client for this pipeline.

        Returns:
            DLQ: The DLQ client instance
        """
        return self._dlq

    @dlq.setter
    def dlq(self, dlq: DLQ) -> None:
        self._dlq = dlq

    def _tracking_info(self) -> dict[str, Any]:
        """Get information about the active pipeline."""
        # If config is not set, return minimal info
        if self.config is None:
            return {"pipeline_id": self.pipeline_id}

        # Extract join info
        join_enabled = getattr(self.config.join, "enabled", False)

        # Extract deduplication info
        deduplication_enabled = any(
            t.deduplication and t.deduplication.enabled
            for t in self.config.source.topics
        )

        # Extract connection params
        conn_params = self.config.source.connection_params

        root_ca_provided = conn_params.root_ca is not None
        skip_auth = conn_params.skip_auth
        protocol = str(conn_params.protocol)
        mechanism = str(conn_params.mechanism)

        return {
            "pipeline_id": self.config.pipeline_id,
            "join_enabled": join_enabled,
            "deduplication_enabled": deduplication_enabled,
            "source_auth_method": mechanism,
            "source_security_protocol": protocol,
            "source_root_ca_provided": root_ca_provided,
            "source_skip_auth": skip_auth,
        }

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        pipeline_properties = self._tracking_info()
        properties = {**pipeline_properties, **kwargs}
        super()._track_event(event_name, **properties)

    def _request(
        self, method: str, endpoint: str, event_name: str, **kwargs: Any
    ) -> Response:
        try:
            response = super()._request(method, endpoint, **kwargs)
            self._track_event(event_name)
            return response
        except errors.NotFoundError as e:
            self._track_event(event_name, error_type="PipelineNotFound")
            raise errors.PipelineNotFoundError(
                status_code=e.status_code,
                message=f"Pipeline with id '{self.pipeline_id}' not found",
                response=e.response,
            ) from e
        except errors.UnprocessableContentError as e:
            self._track_event(event_name, error_type="InvalidPipelineConfig")
            raise errors.PipelineInvalidConfigurationError(
                status_code=e.status_code,
                message=e.message or "Invalid pipeline configuration",
            ) from e
        except errors.APIError as e:
            self._track_event(event_name, error_type="InternalServerError")
            raise e
