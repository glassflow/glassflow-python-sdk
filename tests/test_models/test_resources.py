import pytest

from glassflow.etl.errors import ImmutableResourceError
from glassflow.etl.models.resources import (
    JetStreamResources,
    JoinResources,
    NATSResources,
    PipelineResourcesConfig,
    Resources,
    StorageResources,
    TransformResources,
)


class TestImmutableResourceErrors:
    """
    Tests for ImmutableResourceError when updating frozen pipeline resource fields.
    """

    def test_jetstream_resources_update_immutable_max_age_raises(self):
        """
        Updating nats.stream max_age raises ImmutableResourceError with clear message.
        """
        current = JetStreamResources(max_age="24h", max_bytes="512Mi")
        patch = JetStreamResources(max_age="72h", max_bytes=None)
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "max_age" in str(exc_info.value)
        assert "max_bytes" in str(exc_info.value)
        assert "nats.stream" in str(exc_info.value)
        assert "immutable" in str(exc_info.value).lower()

    def test_jetstream_resources_update_immutable_max_bytes_raises(self):
        """Updating nats.stream max_bytes raises ImmutableResourceError."""
        current = JetStreamResources(max_age="24h", max_bytes="512Mi")
        patch = JetStreamResources(max_age=None, max_bytes="1GB")
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "immutable" in str(exc_info.value).lower()

    def test_jetstream_resources_update_empty_patch_succeeds(self):
        """Updating with no frozen fields changed does not raise."""
        current = JetStreamResources(max_age="24h", max_bytes="512Mi")
        patch = JetStreamResources()
        result = current.update(patch)
        assert result.max_age == "24h"
        assert result.max_bytes == "512Mi"

    def test_storage_resources_update_immutable_size_raises(self):
        """
        Updating transform.storage size raises ImmutableResourceError.
        """
        current = StorageResources(size="5Gi")
        patch = StorageResources(size="10Gi")
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "size" in str(exc_info.value)
        assert "transform.storage" in str(exc_info.value)
        assert "immutable" in str(exc_info.value).lower()

    def test_storage_resources_update_empty_patch_succeeds(self):
        """Updating storage with no size change does not raise."""
        current = StorageResources(size="5Gi")
        patch = StorageResources()
        result = current.update(patch)
        assert result.size == "5Gi"

    def test_join_resources_update_immutable_replicas_raises(self):
        """Updating join replicas raises ImmutableResourceError with clear message."""
        current = JoinResources(replicas=1, limits=Resources(memory="64Mi"))
        patch = JoinResources(replicas=2, limits=None, requests=None)
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "replicas" in str(exc_info.value)
        assert "join" in str(exc_info.value)
        assert "immutable" in str(exc_info.value).lower()

    def test_join_resources_update_limits_and_requests_succeeds(self):
        """Updating join limits/requests (mutable) does not raise."""
        current = JoinResources(
            replicas=1,
            limits=Resources(memory="64Mi"),
            requests=Resources(memory="32Mi"),
        )
        patch = JoinResources(
            replicas=None,
            limits=Resources(memory="128Mi", cpu="100m"),
            requests=Resources(cpu="50m"),
        )
        result = current.update(patch)
        assert result.replicas == 1
        assert result.limits is not None
        assert result.limits.memory == "128Mi"
        assert result.limits.cpu == "100m"
        assert result.requests is not None
        assert result.requests.cpu == "50m"

    def test_pipeline_resources_config_update_nats_immutable_raises(self):
        """Updating pipeline_resources with nats.stream immutable fields raises."""
        current = PipelineResourcesConfig(
            nats=NATSResources(
                stream=JetStreamResources(max_age="24h", max_bytes="512Mi")
            )
        )
        patch = PipelineResourcesConfig(
            nats=NATSResources(stream=JetStreamResources(max_age="72h", max_bytes=None))
        )
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "nats.stream" in str(exc_info.value)

    def test_pipeline_resources_config_update_transform_storage_immutable_raises(self):
        """Updating pipeline_resources with transform.storage size raises."""
        current = PipelineResourcesConfig(
            transform=TransformResources(storage=StorageResources(size="5Gi"))
        )
        patch = PipelineResourcesConfig(
            transform=TransformResources(storage=StorageResources(size="10Gi"))
        )
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "transform.storage" in str(exc_info.value)
