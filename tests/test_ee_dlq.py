"""Tests for the Enterprise DLQ: list / reprocess / discard."""

from unittest.mock import patch

import pytest

from glassflow import ee
from glassflow.etl import errors
from glassflow.etl.dlq import DLQ as OSSDLQ
from tests.data import mock_responses


@pytest.fixture
def ee_dlq():
    return ee.DLQ(host="http://localhost:8080", pipeline_id="test-pipeline")


@pytest.fixture
def ee_pipeline(valid_config):
    config = ee.PipelineConfig(**valid_config)
    return ee.Pipeline(host="http://localhost:8080", config=config)


class TestEEDLQWiring:
    def test_ee_dlq_subclasses_oss(self):
        assert issubclass(ee.DLQ, OSSDLQ)

    def test_pipeline_exposes_ee_dlq(self, ee_pipeline):
        assert isinstance(ee_pipeline.dlq, ee.DLQ)

    def test_dlq_setter_still_works(self, ee_pipeline, ee_dlq):
        ee_pipeline.dlq = ee_dlq
        assert ee_pipeline.dlq is ee_dlq

    def test_get_pipeline_dlq_is_ee(
        self, mock_success, get_pipeline_response, get_health_payload
    ):
        client = ee.Client(host="http://localhost:8080")
        with mock_success(
            [get_pipeline_response, get_health_payload("test-pipeline-id")]
        ):
            pipeline = client.get_pipeline("test-pipeline-id")
        assert isinstance(pipeline.dlq, ee.DLQ)


class TestList:
    def test_list_success(self, ee_dlq, mock_success):
        payload = [
            {
                "message_id": "seq_101",
                "source": "source-0",
                "component": "sink",
                "error": "connection refused",
                "original_message": "{}",
                "received_at": "2026-05-29T14:00:00Z",
            }
        ]
        with mock_success(json_payloads=[payload]) as mock_get:
            result = ee_dlq.list(batch_size=50)

            mock_get.assert_called_once_with(
                "GET", f"{ee_dlq.endpoint}/list", params={"batch_size": 50}
            )
            assert result == payload

    def test_list_with_cursor(self, ee_dlq, mock_success):
        with mock_success(json_payloads=[[]]) as mock_get:
            ee_dlq.list(batch_size=10, cursor="seq_200")
            mock_get.assert_called_once_with(
                "GET",
                f"{ee_dlq.endpoint}/list",
                params={"batch_size": 10, "cursor": "seq_200"},
            )

    def test_list_empty_on_204(self, ee_dlq):
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=204, json_data=None
        )
        with patch("httpx.Client.request", return_value=mock_response):
            assert ee_dlq.list() == []

    @pytest.mark.parametrize("bad", [0, 101, -1, "10"])
    def test_list_invalid_batch_size(self, ee_dlq, bad):
        with pytest.raises(ValueError, match="batch_size must be an integer"):
            ee_dlq.list(batch_size=bad)


class TestReprocess:
    def test_reprocess_selected(self, ee_dlq, mock_success):
        with mock_success(
            json_payloads=[{"request_id": "rep_1", "status": "accepted"}]
        ) as mock_post:
            result = ee_dlq.reprocess(["seq_101", "seq_102"])

            mock_post.assert_called_once_with(
                "POST",
                f"{ee_dlq.endpoint}/reprocess",
                json={"mode": "selected", "message_ids": ["seq_101", "seq_102"]},
            )
            assert result == {"request_id": "rep_1", "status": "accepted"}

    def test_reprocess_all(self, ee_dlq, mock_success):
        with mock_success(
            json_payloads=[{"request_id": "rep_2", "status": "accepted"}]
        ) as mock_post:
            ee_dlq.reprocess_all()
            mock_post.assert_called_once_with(
                "POST", f"{ee_dlq.endpoint}/reprocess", json={"mode": "all"}
            )

    def test_reprocess_empty_ids_raises(self, ee_dlq):
        with pytest.raises(ValueError, match="must be non-empty"):
            ee_dlq.reprocess([])

    def test_reprocess_too_many_ids_raises(self, ee_dlq):
        with pytest.raises(ValueError, match="cannot exceed 1000"):
            ee_dlq.reprocess([str(i) for i in range(1001)])


class TestDiscard:
    def test_discard_selected(self, ee_dlq, mock_success):
        with mock_success(
            json_payloads=[{"request_id": "dis_1", "discarded_count": 2}]
        ) as mock_post:
            result = ee_dlq.discard(["seq_1", "seq_2"])

            mock_post.assert_called_once_with(
                "POST",
                f"{ee_dlq.endpoint}/discard",
                json={"mode": "selected", "message_ids": ["seq_1", "seq_2"]},
            )
            assert result == {"request_id": "dis_1", "discarded_count": 2}

    def test_discard_all(self, ee_dlq, mock_success):
        with mock_success(
            json_payloads=[{"request_id": "dis_2", "discarded_count": 9}]
        ) as mock_post:
            ee_dlq.discard_all()
            mock_post.assert_called_once_with(
                "POST", f"{ee_dlq.endpoint}/discard", json={"mode": "all"}
            )

    def test_discard_empty_ids_raises(self, ee_dlq):
        with pytest.raises(ValueError, match="must be non-empty"):
            ee_dlq.discard([])


class TestDeprecatedInherited:
    def test_consume_warns_and_delegates_to_list(self, ee_dlq, mock_success):
        with mock_success(json_payloads=[[{"message_id": "seq_1"}]]) as mock_get:
            with pytest.warns(DeprecationWarning, match="use DLQ.list"):
                result = ee_dlq.consume(batch_size=25)

            # Hits /dlq/list, not /dlq/consume.
            mock_get.assert_called_once_with(
                "GET", f"{ee_dlq.endpoint}/list", params={"batch_size": 25}
            )
            assert result == [{"message_id": "seq_1"}]

    def test_purge_warns_and_hits_purge_endpoint(self, ee_dlq, mock_success):
        with mock_success() as mock_post:
            with pytest.warns(DeprecationWarning, match="use DLQ.discard_all"):
                ee_dlq.purge()

            mock_post.assert_called_once_with("POST", f"{ee_dlq.endpoint}/purge")


class TestEntitlement:
    def test_forbidden_maps_to_feature_not_licensed(self, ee_dlq):
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=403, json_data={"message": "Forbidden"}
        )
        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(errors.FeatureNotLicensedError) as exc_info:
                ee_dlq.reprocess_all()

            assert "Enterprise" in str(exc_info.value)
            # Still catchable as a ForbiddenError by existing 403 handling.
            assert isinstance(exc_info.value, errors.ForbiddenError)


class TestPipelineState:
    def _conflict_patch(self):
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=409, json_data={"message": "pipeline is not running"}
        )
        return patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        )

    def test_reprocess_on_non_running_raises_pipeline_not_running(self, ee_dlq):
        with self._conflict_patch():
            with pytest.raises(errors.PipelineNotRunningError) as exc_info:
                ee_dlq.reprocess(["seq_1"])

            assert "Running" in str(exc_info.value)
            # Still catchable as the generic 409 ConflictError.
            assert isinstance(exc_info.value, errors.ConflictError)

    def test_reprocess_all_on_non_running_raises_pipeline_not_running(self, ee_dlq):
        with self._conflict_patch():
            with pytest.raises(errors.PipelineNotRunningError):
                ee_dlq.reprocess_all()

    def test_discard_409_stays_conflict_error(self, ee_dlq):
        # Discard has no Running-state constraint, so a 409 is not remapped.
        with self._conflict_patch():
            with pytest.raises(errors.ConflictError) as exc_info:
                ee_dlq.discard_all()

            assert not isinstance(exc_info.value, errors.PipelineNotRunningError)
