from datetime import datetime

import pytest

from glassflow import Space, errors


def test_create_space_ok(requests_mock, create_space_response, client):
    requests_mock.post(
        client.glassflow_config.server_url + "/spaces",
        json=create_space_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    space = Space(
        name=create_space_response["name"], personal_access_token="test-token"
    ).create()

    assert space.name == create_space_response["name"]
    assert space.id == create_space_response["id"]

    parsed_response_space_created_at = datetime.strptime(
        create_space_response["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    assert space.created_at.replace(tzinfo=None) == parsed_response_space_created_at


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


def test_delete_space_fail_with_missing_id(client):
    with pytest.raises(ValueError) as e:
        Space(personal_access_token="test-token").delete()

    assert str(e.value) == "Space id must be provided in the constructor"


def test_delete_space_file_with_409(requests_mock, client):
    requests_mock.delete(
        client.glassflow_config.server_url + "/spaces/test-space-id",
        status_code=409,
        json={"msg": "", "existed_pipeline_id": ""},
        headers={"Content-Type": "application/json"},
    )
    with pytest.raises(errors.SpaceIsNotEmptyError):
        Space(
            id="test-space-id",
            personal_access_token="test-token",
        ).delete()
