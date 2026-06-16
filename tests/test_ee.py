"""Tests for the Enterprise (ee) client and pipeline scaffold.

DLQ-specific Enterprise capabilities are covered in a follow-up PR.
"""

from unittest.mock import patch

import pytest

from glassflow import ee
from glassflow.etl import errors
from glassflow.etl.client import Client as OSSClient
from glassflow.etl.pipeline import Pipeline as OSSPipeline
from tests.data import mock_responses


@pytest.fixture
def ee_pipeline(valid_config):
    """Fixture for an Enterprise Pipeline with a valid config."""
    config = ee.PipelineConfig(**valid_config)
    return ee.Pipeline(host="http://localhost:8080", config=config)


class TestEEInheritance:
    """The ee classes extend, not replace, the OSS ones."""

    def test_ee_client_subclasses_oss(self):
        assert issubclass(ee.Client, OSSClient)

    def test_ee_pipeline_subclasses_oss(self):
        assert issubclass(ee.Pipeline, OSSPipeline)


class TestEEWiring:
    """Edition propagates from Client to the Pipeline it returns."""

    def test_client_constructs_ee_pipeline(self):
        assert ee.Client._pipeline_class is ee.Pipeline

    def test_get_pipeline_returns_ee_pipeline(
        self, mock_success, get_pipeline_response, get_health_payload
    ):
        client = ee.Client(host="http://localhost:8080")
        with mock_success(
            [get_pipeline_response, get_health_payload("test-pipeline-id")]
        ):
            pipeline = client.get_pipeline("test-pipeline-id")

        assert isinstance(pipeline, ee.Pipeline)


class TestGetStreams:
    @pytest.fixture
    def ee_pipeline_by_id(self):
        return ee.Pipeline(host="http://localhost:8080", pipeline_id="p1")

    def test_get_streams_success(self, ee_pipeline_by_id, mock_success, mock_track):
        payload = {
            "pipeline_id": "p1",
            "streams": [
                {"stream_name": "gfm-abc-DLQ", "component": "dlq"},
                {"stream_name": "gfm-abc-ingestor", "component": "ingestor"},
            ],
        }
        with mock_success(json_payloads=[payload]) as mock_get:
            streams = ee_pipeline_by_id.get_streams()

            mock_get.assert_called_once_with("GET", "/api/v1/pipeline/p1/streams")
            assert streams == payload["streams"]
            assert streams[0]["component"] == "dlq"

    def test_get_streams_empty_on_204(self, ee_pipeline_by_id, mock_track):
        resp = mock_responses.create_mock_response_factory()(
            status_code=204, json_data=None
        )
        with patch("httpx.Client.request", return_value=resp):
            assert ee_pipeline_by_id.get_streams() == []

    def test_get_streams_not_found(self, ee_pipeline_by_id, mock_track):
        resp = mock_responses.create_mock_response_factory()(
            status_code=404, json_data={"message": "not found"}
        )
        with patch(
            "httpx.Client.request", side_effect=resp.raise_for_status.side_effect
        ):
            with pytest.raises(errors.PipelineNotFoundError):
                ee_pipeline_by_id.get_streams()

    def test_get_streams_forbidden_maps_to_feature_not_licensed(
        self, ee_pipeline_by_id, mock_track
    ):
        resp = mock_responses.create_mock_response_factory()(
            status_code=403, json_data={"message": "Forbidden"}
        )
        with patch(
            "httpx.Client.request", side_effect=resp.raise_for_status.side_effect
        ):
            with pytest.raises(errors.FeatureNotLicensedError) as exc:
                ee_pipeline_by_id.get_streams()
            assert isinstance(exc.value, errors.ForbiddenError)
