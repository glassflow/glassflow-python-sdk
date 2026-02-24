"""Tests for DLQ (Dead Letter Queue) functionality."""

from unittest.mock import patch

import pytest

from glassflow.etl import DLQ, Pipeline, errors
from tests.data import error_scenarios, mock_responses


class TestDLQ:
    """Test cases for DLQ class."""

    def test_dlq_initialization(self, dlq):
        """Test DLQ initialization."""
        assert dlq.http_client.base_url == "http://localhost:8080"
        assert dlq.endpoint == "/api/v1/pipeline/test-pipeline/dlq"

    def test_consume_success(self, dlq, mock_success):
        """Test successful DLQ consume operation."""
        payload = [
            {"id": "msg1", "content": "test message 1"},
            {"id": "msg2", "content": "test message 2"},
        ]
        with mock_success(json_payloads=[payload]) as mock_get:
            result = dlq.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "GET", f"{dlq.endpoint}/consume", params={"batch_size": 50}
            )
            assert result == [
                {"id": "msg1", "content": "test message 1"},
                {"id": "msg2", "content": "test message 2"},
            ]

    def test_consume_returns_empty_list_when_no_messages(self, dlq, mock_success):
        """Test DLQ consume returns empty list when there are no messages to consume."""
        with mock_success(json_payloads=[[]]) as mock_get:
            result = dlq.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "GET", f"{dlq.endpoint}/consume", params={"batch_size": 50}
            )
            assert result == []

    def test_consume_returns_empty_list_on_204_no_content(self, dlq):
        """Test DLQ consume returns empty list when API responds with 204 No Content."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=204,
            json_data=None,
        )
        with patch("httpx.Client.request", return_value=mock_response) as mock_get:
            result = dlq.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "GET", f"{dlq.endpoint}/consume", params={"batch_size": 50}
            )
            assert result == []

    def test_consume_returns_empty_list_on_200_empty_body(self, dlq):
        """Test DLQ consume returns empty list when API returns 200 with empty body."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=200,
            json_data=None,
        )
        mock_response.content = b""
        with patch("httpx.Client.request", return_value=mock_response) as mock_get:
            result = dlq.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "GET", f"{dlq.endpoint}/consume", params={"batch_size": 50}
            )
            assert result == []

    @pytest.mark.parametrize(
        "scenario",
        [
            s
            for s in error_scenarios.get_dlq_error_scenarios()
            if s["name"].startswith("invalid_batch_size")
        ],
        ids=lambda s: s["name"],
    )
    def test_consume_invalid_batch_size_scenarios(self, dlq, scenario):
        """Test DLQ consume with various invalid batch size scenarios."""
        with pytest.raises(scenario["expected_error"]) as exc_info:
            dlq.consume(batch_size=scenario["batch_size"])

        assert scenario["error_message"] in str(exc_info.value)

    @pytest.mark.parametrize(
        "scenario",
        [
            s
            for s in error_scenarios.get_dlq_error_scenarios()
            if s["name"].startswith("http_error")
        ],
        ids=lambda s: s["name"],
    )
    def test_consume_http_error_scenarios(self, dlq, scenario):
        """Test DLQ consume with HTTP error scenarios."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=scenario["status_code"],
            json_data={"message": scenario["text"]},
            text=scenario["text"],
        )

        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(scenario["expected_error"]) as exc_info:
                dlq.consume(batch_size=50)

            assert scenario["error_message"] in str(exc_info.value)

    def test_state_success(self, dlq, mock_success):
        """Test successful DLQ state operation."""
        state_payload = {
            "total_messages": 42,
            "pending_messages": 5,
            "last_updated": "2023-01-01T00:00:00Z",
        }
        with mock_success(state_payload) as mock_get:
            result = dlq.state()

            mock_get.assert_called_once_with("GET", f"{dlq.endpoint}/state")
            assert result == {
                "total_messages": 42,
                "pending_messages": 5,
                "last_updated": "2023-01-01T00:00:00Z",
            }

    def test_state_server_error(self, dlq):
        """Test DLQ state with server error."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=500,
            json_data={"message": "Internal server error"},
        )

        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(errors.ServerError) as exc_info:
                dlq.state()

            assert "Internal server error" in str(exc_info.value)

    def test_purge_success(self, dlq, mock_success):
        """Test successful DLQ purge operation."""
        with mock_success() as mock_post:
            dlq.purge()

            mock_post.assert_called_once_with("POST", f"{dlq.endpoint}/purge")

    def test_purge_pipeline_not_found(self, dlq):
        """Test DLQ purge with pipeline not found error."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=404,
            json_data={"message": "Pipeline not found"},
        )

        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(errors.PipelineNotFoundError) as exc_info:
                dlq.purge()

            assert "test-pipeline" in str(exc_info.value)

    def test_purge_server_error(self, dlq):
        """Test DLQ purge with server error."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=500,
            json_data={"message": "Internal server error"},
        )

        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(errors.ServerError) as exc_info:
                dlq.purge()

            assert "Internal server error" in str(exc_info.value)


class TestPipelineDLQIntegration:
    """Test cases for Pipeline-DLQ integration."""

    def test_pipeline_dlq_property(self, pipeline):
        """Test that Pipeline has a DLQ property."""
        assert hasattr(pipeline, "dlq")
        assert isinstance(pipeline.dlq, DLQ)

    def test_pipeline_dlq_property_same_url(self):
        """Test that Pipeline DLQ uses the same base URL."""
        custom_url = "http://custom-url:9000"
        pipeline = Pipeline(host=custom_url, pipeline_id="test-pipeline-id")

        assert pipeline.http_client.base_url == custom_url
        assert pipeline.dlq.http_client.base_url == custom_url

    def test_pipeline_dlq_consume_integration(self, pipeline, mock_success):
        """Test Pipeline DLQ consume functionality."""
        with mock_success(json_payloads=[[{"id": "msg1", "content": "test"}]]):
            result = pipeline.dlq.consume(batch_size=10)

            assert result == [{"id": "msg1", "content": "test"}]

    def test_pipeline_dlq_state_integration(self, pipeline, mock_success):
        """Test Pipeline DLQ state functionality."""
        with mock_success({"total_messages": 10}):
            result = pipeline.dlq.state()

            assert result == {"total_messages": 10}

    def test_pipeline_dlq_purge_integration(self, pipeline, mock_success):
        """Test Pipeline DLQ purge functionality."""
        with mock_success():
            pipeline.dlq.purge()
            # If no exception is raised, the test passes
