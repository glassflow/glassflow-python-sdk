import pytest

from glassflow import Secret, errors


def test_create_secret_ok(requests_mock, client):
    requests_mock.post(
        client.glassflow_config.server_url + "/secrets",
        status_code=201,
        headers={"Content-Type": "application/json"},
    )
    Secret(
        key="SecretKey", value="SecretValue", personal_access_token="test-token"
    ).create()


def test_create_secret_fail_with_invalid_key_error(client):
    with pytest.raises(errors.SecretInvalidKeyError):
        Secret(
            key="secret-key", value="secret-value", personal_access_token="test-token"
        )


def test_create_secret_fail_with_value_error(client):
    with pytest.raises(ValueError):
        Secret(personal_access_token="test-token").create()


def test_delete_secret_ok(requests_mock, client):
    secret_key = "SecretKey"
    requests_mock.delete(
        client.glassflow_config.server_url + f"/secrets/{secret_key}",
        status_code=204,
        headers={"Content-Type": "application/json"},
    )
    Secret(key=secret_key, personal_access_token="test-token").delete()


def test_delete_secret_fail_with_value_error(client):
    with pytest.raises(ValueError):
        Secret(personal_access_token="test-token").delete()
