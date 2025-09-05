from unittest.mock import patch

import pytest

from glassflow.etl import errors
from glassflow.etl.client import Client
from glassflow.etl.models import PipelineConfig
from glassflow.etl.pipeline import Pipeline
from tests.data import mock_responses


class TestClient:
    """Tests for the Client class."""

    def test_client_init(self):
        """Test Client initialization."""
        client = Client(host="https://example.com")
        assert client.host == "https://example.com"
        assert client.http_client.base_url == "https://example.com"

    def test_client_get_pipeline_success(self, valid_config, mock_success_response):
        """Test successful pipeline retrieval by ID."""
        client = Client()
        pipeline_id = "test-pipeline-id"

        mock_success_response.json.return_value = valid_config

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline = client.get_pipeline(pipeline_id)
            mock_request.assert_called_once_with(
                "GET", f"{client.ENDPOINT}/{pipeline_id}"
            )
            assert isinstance(pipeline, Pipeline)
            assert pipeline.pipeline_id == pipeline_id

    def test_client_get_pipeline_not_found(self, mock_not_found_response):
        """Test pipeline retrieval when pipeline is not found."""
        client = Client()
        pipeline_id = "non-existent-pipeline"

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError) as exc_info:
                client.get_pipeline(pipeline_id)
            assert "not found" in str(exc_info.value)

    def test_client_list_pipelines_success_list_format(self):
        """Test successful pipeline listing with list format response."""
        client = Client()
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=200,
            json_data=[
                {
                    "pipeline_id": "loadtest",
                    "name": "loadtest",
                    "transformation_type": "Deduplication",
                    "created_at": "2025-07-28T11:50:05.478766129Z",
                    "state": "",
                },
                {
                    "pipeline_id": "loadtest-4",
                    "name": "loadtest-4",
                    "transformation_type": "Ingest Only",
                    "created_at": "2025-07-28T11:52:53.210108151Z",
                    "state": "",
                },
                {
                    "pipeline_id": "loadtest-5",
                    "name": "loadtest-5",
                    "transformation_type": "Join",
                    "created_at": "2025-07-28T11:54:46.270842895Z",
                    "state": "",
                },
            ],
        )

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipelines = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert len(pipelines) == 3
            assert pipelines[0]["pipeline_id"] == "loadtest"
            assert pipelines[0]["name"] == "loadtest"
            assert pipelines[0]["transformation_type"] == "Deduplication"
            assert pipelines[1]["pipeline_id"] == "loadtest-4"
            assert pipelines[1]["transformation_type"] == "Ingest Only"
            assert pipelines[2]["pipeline_id"] == "loadtest-5"
            assert pipelines[2]["transformation_type"] == "Join"

    def test_client_list_pipeline_success_single_item(self):
        """Test successful pipeline listing with single pipeline in list response."""
        client = Client()
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=200,
            json_data=[
                {
                    "pipeline_id": "single-pipeline",
                    "name": "single-pipeline",
                    "transformation_type": "Ingest Only",
                    "created_at": "2025-07-28T11:50:05.478766129Z",
                    "state": "",
                }
            ],
        )

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipelines = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert len(pipelines) == 1
            assert pipelines[0]["pipeline_id"] == "single-pipeline"
            assert pipelines[0]["name"] == "single-pipeline"
            assert pipelines[0]["transformation_type"] == "Ingest Only"

    def test_client_list_pipelines_empty(self):
        """Test pipeline listing when no pipelines exist."""
        client = Client()
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=404,
            json_data=[],
        )

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipelines = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert pipelines == []

    def test_client_create_pipeline_success(self, valid_config, mock_success_response):
        """Test successful pipeline creation."""
        client = Client()

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline = client.create_pipeline(valid_config)
            mock_request.assert_called_once_with(
                "POST", client.ENDPOINT, json=mock_request.call_args[1]["json"]
            )
            assert isinstance(pipeline, Pipeline)
            assert pipeline.pipeline_id == valid_config["pipeline_id"]

    def test_client_create_pipeline_already_exists(
        self, valid_config, mock_forbidden_response
    ):
        """Test pipeline creation when pipeline already exists."""
        client = Client()

        with patch("httpx.Client.request", return_value=mock_forbidden_response):
            with pytest.raises(errors.PipelineAlreadyExistsError):
                client.create_pipeline(valid_config)

    def test_client_create_pipeline_from_yaml_success(self, mock_success_response):
        """Test pipeline creation from YAML file."""
        client = Client()
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            client.create_pipeline(
                pipeline_config_yaml_path="tests/data/valid_pipeline.yaml"
            )
            mock_request.assert_called_once_with(
                "POST", client.ENDPOINT, json=mock_request.call_args[1]["json"]
            )

    def test_client_create_pipeline_from_json_success(self, mock_success_response):
        """Test pipeline creation from JSON file."""
        client = Client()
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            client.create_pipeline(
                pipeline_config_json_path="tests/data/valid_pipeline.json"
            )
            mock_request.assert_called_once_with(
                "POST", client.ENDPOINT, json=mock_request.call_args[1]["json"]
            )

    def test_client_create_pipeline_value_error(self, valid_config):
        """Test pipeline creation with invalid configuration."""
        client = Client()
        with pytest.raises(ValueError):
            client.create_pipeline(
                pipeline_config=valid_config,
                pipeline_config_yaml_path="tests/data/valid_pipeline.yaml",
                pipeline_config_json_path="tests/data/valid_pipeline.json",
            )
        with pytest.raises(ValueError):
            client.create_pipeline()

    def test_client_delete_pipeline_success(
        self, mock_success_response, mock_success_get_pipeline
    ):
        """Test successful pipeline deletion."""
        client = Client()
        pipeline_id = "test-pipeline-id"

        with patch("glassflow.etl.pipeline.Pipeline.get") as pipeline_get:
            with patch(
                "httpx.Client.request", return_value=mock_success_response
            ) as mock_delete_request:
                client.delete_pipeline(pipeline_id, terminate=True)
                pipeline_get.assert_called_once_with()
                mock_delete_request.assert_called_once_with(
                    "DELETE", f"{client.ENDPOINT}/{pipeline_id}/terminate"
                )

    def test_client_delete_pipeline_not_found(self, mock_not_found_response):
        """Test pipeline deletion when pipeline is not found."""
        client = Client()
        pipeline_id = "non-existent-pipeline"

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError) as exc_info:
                client.delete_pipeline(pipeline_id)
            assert "not found" in str(exc_info.value)

    def test_pipeline_to_dict(self, valid_config):
        """Test Pipeline to_dict method."""
        config = PipelineConfig(**valid_config)
        pipeline = Pipeline(config=config)

        pipeline_dict = pipeline.to_dict()
        assert isinstance(pipeline_dict, dict)
        assert pipeline_dict["pipeline_id"] == valid_config["pipeline_id"]

    def test_pipeline_delete(self, pipeline_from_id, mock_success_response):
        """Test Pipeline delete with explicit pipeline_id."""
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline_from_id.delete(terminate=True)
            mock_request.assert_called_once_with(
                "DELETE",
                f"{pipeline_from_id.ENDPOINT}/{pipeline_from_id.pipeline_id}/terminate",
            )
