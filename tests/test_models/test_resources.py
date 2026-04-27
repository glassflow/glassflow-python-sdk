import pytest

from glassflow.etl.errors import ImmutableResourceError
from glassflow.etl.models.resources import (
    JetStreamResources,
    NATSResources,
    PipelineResourcesConfig,
    Resources,
    SinkResources,
    StorageResources,
)


class TestImmutableResourceErrors:
    """
    Tests for ImmutableResourceError when updating frozen pipeline resource fields.
    """

    def test_jetstream_resources_update_immutable_max_age_raises(self):
        """
        Updating nats.stream max_age raises ImmutableResourceError with clear message.
        """
        current = JetStreamResources(maxAge="24h", maxBytes="512Mi")
        patch = JetStreamResources(maxAge="72h", maxBytes=None)
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "maxAge" in str(exc_info.value)
        assert "maxBytes" in str(exc_info.value)
        assert "nats.stream" in str(exc_info.value)
        assert "immutable" in str(exc_info.value).lower()

    def test_jetstream_resources_update_immutable_max_bytes_raises(self):
        """Updating nats.stream max_bytes raises ImmutableResourceError."""
        current = JetStreamResources(maxAge="24h", maxBytes="512Mi")
        patch = JetStreamResources(maxAge=None, maxBytes="1GB")
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "immutable" in str(exc_info.value).lower()

    def test_jetstream_resources_update_empty_patch_succeeds(self):
        """Updating with no frozen fields changed does not raise."""
        current = JetStreamResources(maxAge="24h", maxBytes="512Mi")
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

    def test_pipeline_resources_config_update_nats_immutable_raises(self):
        """Updating resources with nats.stream immutable fields raises."""
        current = PipelineResourcesConfig(
            nats=NATSResources(
                stream=JetStreamResources(maxAge="24h", maxBytes="512Mi")
            )
        )
        patch = PipelineResourcesConfig(
            nats=NATSResources(stream=JetStreamResources(maxAge="72h", maxBytes=None))
        )
        with pytest.raises(ImmutableResourceError) as exc_info:
            current.update(patch)
        assert "nats.stream" in str(exc_info.value)

    def test_pipeline_resources_config_update_sink_succeeds(self):
        """Updating resources with sink replicas succeeds."""
        current = PipelineResourcesConfig(
            sink=SinkResources(
                replicas=1,
                requests=Resources(memory="64Mi"),
                limits=Resources(memory="128Mi"),
            )
        )
        patch = PipelineResourcesConfig(sink=SinkResources(replicas=2))
        result = current.update(patch)
        assert result.sink.replicas == 2
