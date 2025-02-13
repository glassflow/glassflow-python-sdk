import pytest

from glassflow import errors


def test_create_secret_ok(creating_secret):
    assert creating_secret.key == "SecretKey"
    assert creating_secret.value == "SecretValue"


def test_delete_secret_fail_with_401(secret_with_invalid_key_and_token):
    with pytest.raises(errors.SecretUnauthorizedError):
        secret_with_invalid_key_and_token.delete()


def test_delete_secret_fail_with_404(secret_with_invalid_key):
    with pytest.raises(errors.SecretNotFoundError):
        secret_with_invalid_key.delete()
