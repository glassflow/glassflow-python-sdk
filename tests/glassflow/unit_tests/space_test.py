import pytest

from glassflow import Space


def test_create_space_ok(requests_mock, create_space_response, client):
    requests_mock.post(
        client.glassflow_config.server_url + "/spaces",
        json=create_space_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    space = Space(
        name=create_space_response["name"],
        personal_access_token="test-token"
    ).create()

    assert space.name == create_space_response["name"]
    assert space.id == create_space_response["id"]
    assert space.created_at == create_space_response["created_at"]


def test_create_space_fail_with_missing_name(client):
    with pytest.raises(ValueError) as e:
        Space(personal_access_token="test-token").create()

    assert str(e.value) == (
        "Name must be provided in order to create the space"
    )


def test_delete_space_ok(requests_mock, client):
    requests_mock.delete(
        client.glassflow_config.server_url + "/spaces/test-space-id",
        status_code=204,
        headers={"Content-Type": "application/json"},
    )
    space = Space(
        id="test-space-id",
        personal_access_token="test-token",
    )
    space.delete()
