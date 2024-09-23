import pytest

from glassflow.pipeline import Pipeline


def test_pipeline_transformation_file_not_found():
    with pytest.raises(FileNotFoundError):
        Pipeline(transformation_file="fake_file.py", personal_access_token="test-token")
