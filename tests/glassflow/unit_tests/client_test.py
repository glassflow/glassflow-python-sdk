import pytest

from glassflow import Pipeline, Secret, Space, errors


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

    assert res.total_amount == list_pipelines_response["total_amount"]
    assert res.pipelines[0].name == list_pipelines_response["pipelines"][0]["name"]


def test_list_pipelines_fail_with_401(requests_mock, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines",
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.UnauthorizedError):
        client.list_pipelines()


def test_create_pipeline_ok(requests_mock, client, create_pipeline_response):
    requests_mock.post(
        client.glassflow_config.server_url + "/pipelines",
        json=create_pipeline_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    pipeline_args = dict(
        name=create_pipeline_response["name"],
        space_id=create_pipeline_response["space_id"],
        transformation_file="tests/data/transformation.py",
        source_kind="google_pubsub",
        source_config={
            "project_id": "test-project-id",
            "subscription_id": "test-subscription-id",
            "credentials_json": Secret(
                key="testCredentials",
                personal_access_token=client.personal_access_token,
            ),
        },
    )
    p1 = client.create_pipeline(**pipeline_args)
    p2 = Pipeline(
        personal_access_token=client.personal_access_token, **pipeline_args
    ).create()

    assert p1 == p2


def test_create_space_ok(requests_mock, client, create_space_response):
    requests_mock.post(
        client.glassflow_config.server_url + "/spaces",
        json=create_space_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    s1 = client.create_space(name=create_space_response["name"])
    s2 = Space(
        personal_access_token=client.personal_access_token,
        name=create_space_response["name"],
    ).create()

    assert s1 == s2


def test_create_secret_ok(requests_mock, client):
    requests_mock.post(
        client.glassflow_config.server_url + "/secrets",
        status_code=201,
        headers={"Content-Type": "application/json"},
    )

    secret_args = {
        "key": "testKey",
        "value": "test-value",
    }
    s1 = client.create_secret(**secret_args)
    s2 = Secret(
        personal_access_token=client.personal_access_token, **secret_args
    ).create()

    assert s1 == s2
