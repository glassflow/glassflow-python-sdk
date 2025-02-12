import os
import uuid

import pytest

from glassflow import (
    GlassFlowClient,
    Pipeline,
    PipelineDataSink,
    PipelineDataSource,
    Secret,
    Space,
)


@pytest.fixture(scope="session")
def client():
    return GlassFlowClient(os.getenv("PERSONAL_ACCESS_TOKEN"))


@pytest.fixture
def space(client):
    return Space(
        name="integration-tests", personal_access_token=client.personal_access_token
    )


@pytest.fixture
def creating_space(space):
    space.create()
    yield space
    space.delete()


@pytest.fixture
def space_with_random_id(client):
    return Space(
        id=str(uuid.uuid4()),
        personal_access_token=client.personal_access_token,
    )


@pytest.fixture
def space_with_random_id_and_invalid_token():
    return Space(
        id=str(uuid.uuid4()),
        personal_access_token="invalid-token",
    )


@pytest.fixture
def pipeline(client, creating_space):
    return Pipeline(
        name="test_pipeline",
        space_id=creating_space.id,
        transformation_file="tests/data/transformation.py",
        personal_access_token=client.personal_access_token,
        metadata={"view_only": True},
        source_kind="google_pubsub",
        source_config={
            "project_id": "my-project-id",
            "subscription_id": "my-subscription-id",
            "credentials_json": "my-credentials.json",
        },
    )


@pytest.fixture
def pipeline_with_secrets(client, creating_space, creating_secret):
    return Pipeline(
        name="test_pipeline",
        space_id=creating_space.id,
        transformation_file="tests/data/transformation.py",
        personal_access_token=client.personal_access_token,
        metadata={"view_only": True},
        source_kind="google_pubsub",
        source_config_secret_refs={
            "project_id": {
                "value": "my-project-id",
            },
            "subscription_id": {
                "value": "my-subscription-id",
            },
            "credentials_json": {
                "secret_ref": {
                    "type": "organization",
                    "key": creating_secret.key
                },
            },
        },
    )

@pytest.fixture
def creating_pipeline_with_secret(pipeline_with_secrets):
    pipeline_with_secrets.create()
    yield pipeline_with_secrets
    pipeline_with_secrets.delete()


@pytest.fixture
def pipeline_with_random_id(client):
    return Pipeline(
        id=str(uuid.uuid4()),
        personal_access_token=client.personal_access_token,
    )


@pytest.fixture
def pipeline_with_random_id_and_invalid_token():
    return Pipeline(
        id=str(uuid.uuid4()),
        personal_access_token="invalid-token",
    )


@pytest.fixture
def creating_pipeline(pipeline):
    pipeline.create()
    yield pipeline
    pipeline.delete()


@pytest.fixture
def source(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=creating_pipeline.id,
        pipeline_access_token=creating_pipeline.access_tokens[0].token,
    )


@pytest.fixture
def source_with_invalid_access_token(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=creating_pipeline.id, pipeline_access_token="invalid-access-token"
    )


@pytest.fixture
def source_with_non_existing_id(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=str(uuid.uuid4()),
        pipeline_access_token=creating_pipeline.access_tokens[0].token,
    )


@pytest.fixture
def source_with_published_events(source):
    source.publish({"test_field": "test_value"})
    yield source


@pytest.fixture
def sink(source_with_published_events):
    return PipelineDataSink(
        pipeline_id=source_with_published_events.pipeline_id,
        pipeline_access_token=source_with_published_events.pipeline_access_token,
    )


@pytest.fixture
def secret(client):
    return Secret(
        key="SecretKey",
        value="SecretValue",
        personal_access_token=client.personal_access_token,
    )


@pytest.fixture
def creating_secret(secret):
    secret.create()
    yield secret
    secret.delete()


@pytest.fixture
def secret_with_invalid_key_and_token():
    return Secret(
        key="InvalidSecretKey",
        personal_access_token="invalid-token",
    )


@pytest.fixture
def secret_with_invalid_key(client):
    return Secret(
        key="InvalidSecretKey", personal_access_token=client.personal_access_token
    )
