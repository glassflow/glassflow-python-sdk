from __future__ import annotations

from typing import Any, List

from . import errors, models
from .api_client import APIClient
from .pipeline import Pipeline


class Client(APIClient):
    """
    Manager class for handling multiple Pipeline instances.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(self, host: str | None = None) -> None:
        """Initialize the PipelineManager class.

        Args:
            host: GlassFlow API host
        """
        super().__init__(host=host)

    def get_pipeline(self, pipeline_id: str):
        """Fetch a pipeline by its ID.

        Args:
            pipeline_id: The ID of the pipeline to fetch

        Returns:
            Pipeline: A Pipeline instance for the given ID

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        return Pipeline(host=self.host, pipeline_id=pipeline_id).get()

    def list_pipelines(self) -> List[dict]:
        """Returns a list of available pipelines.

        Returns:
            List[dict]: List of pipeline items with details as dictionaries

        Raises:
            APIError: If the API request fails
        """
        try:
            response = self._request("GET", self.ENDPOINT)
            data = response.json()

            # API always returns a list of pipelines
            return data if isinstance(data, list) else []

        except errors.NotFoundError:
            # No pipelines found, return empty list
            return []
        except errors.APIError as e:
            self._track_event("PipelineListError", error_type="InternalServerError")
            raise e

    def create_pipeline(
        self,
        pipeline_config: dict[str, Any] | models.PipelineConfig | None = None,
        pipeline_config_yaml_path: str | None = None,
        pipeline_config_json_path: str | None = None,
    ):
        """Creates a new pipeline with the given config.

        Args:
            pipeline_config: Dictionary or PipelineConfig object containing
                the pipeline configuration
            pipeline_config_yaml_path: Path to the YAML file containing
                the pipeline configuration
            pipeline_config_json_path: Path to the JSON file containing
                the pipeline configuration

        Returns:
            Pipeline: A Pipeline instance for the created pipeline

        Raises:
            PipelineAlreadyExistsError: If pipeline already exists
            PipelineInvalidConfigurationError: If configuration is invalid
            APIError: If the API request fails
        """
        if pipeline_config is None:
            if pipeline_config_yaml_path is None and pipeline_config_json_path is None:
                raise ValueError(
                    "Either pipeline_config or pipeline_config_yaml_path or "
                    "pipeline_config_json_path must be provided"
                )
            if pipeline_config_yaml_path is not None:
                pipeline = Pipeline.from_yaml(pipeline_config_yaml_path, host=self.host)
            elif pipeline_config_json_path is not None:
                pipeline = Pipeline.from_json(pipeline_config_json_path, host=self.host)
        else:
            if (
                pipeline_config_yaml_path is not None
                or pipeline_config_json_path is not None
            ):
                raise ValueError(
                    "Either pipeline_config or pipeline_config_yaml_path or "
                    "pipeline_config_json_path must be provided"
                )
            pipeline = Pipeline(config=pipeline_config, host=self.host)

        return pipeline.create()

    def delete_pipeline(self, pipeline_id: str, terminate: bool = True) -> None:
        """Deletes the pipeline with the given ID.

        Args:
            pipeline_id: The ID of the pipeline to delete
            terminate: Whether to terminate the pipeline (i.e. delete all the pipeline
                components and potentially all the events in the pipeline)
        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        Pipeline(host=self.host, pipeline_id=pipeline_id).delete(terminate=terminate)

    def disable_tracking(self) -> None:
        """Disable tracking of pipeline events."""
        self._tracking.enabled = False
