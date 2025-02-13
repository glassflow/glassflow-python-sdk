import pytest

from glassflow import errors


def test_create_space_ok(creating_space):
    assert creating_space.name == "integration-tests"
    assert creating_space.id is not None


def test_delete_space_fail_with_404(space_with_random_id):
    with pytest.raises(errors.SpaceNotFoundError):
        space_with_random_id.delete()


def test_delete_space_fail_with_401(space_with_random_id_and_invalid_token):
    with pytest.raises(errors.SpaceUnauthorizedError):
        space_with_random_id_and_invalid_token.delete()
