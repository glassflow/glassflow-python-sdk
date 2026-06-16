"""Tests for the Enterprise (ee) client and pipeline scaffold.

DLQ-specific Enterprise capabilities are covered in a follow-up PR.
"""

import pytest

from glassflow import ee
from glassflow.etl.client import Client as OSSClient
from glassflow.etl.pipeline import Pipeline as OSSPipeline


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
