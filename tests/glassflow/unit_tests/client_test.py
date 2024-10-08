import pytest

from glassflow.models import errors


@pytest.fixture
def list_pipelines_response():
    return {
        "total_amount": 1,
        "pipelines": [
            {
                "name": "test-name",
                "space_id": "test-space-id",
                "metadata": {"additionalProp1": {}},
                "id": "test-id",
                "created_at": "2024-09-25T13:52:17.910Z",
                "state": "running",
                "space_name": "test-space-name",
            }
        ],
    }


def test_list_pipelines_ok(requests_mock, list_pipelines_response, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines",
        json=list_pipelines_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    res = client.list_pipelines()

    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.total_amount == list_pipelines_response["total_amount"]
    assert res.pipelines == list_pipelines_response["pipelines"]


def test_list_pipelines_fail_with_401(requests_mock, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines",
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.UnauthorizedError):
        client.list_pipelines()
