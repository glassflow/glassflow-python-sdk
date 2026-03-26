"""Tests for source models (Kafka and OTLP)."""

import pytest

from glassflow.etl.models.sources import (
    DeduplicationConfig,
    OTLPLogsSource,
    OTLPMetricsSource,
    OTLPSource,
    OTLPTracesSource,
)


class TestOTLPSource:
    """Tests for the OTLPSource model."""

    def test_otlp_source_logs(self):
        src = OTLPLogsSource(id="logs-src")
        assert src.type == "otlp.logs"
        assert src.id == "logs-src"
        assert src.deduplication is None

    def test_otlp_source_metrics(self):
        src = OTLPMetricsSource(id="metrics-src")
        assert src.type == "otlp.metrics"

    def test_otlp_source_traces(self):
        src = OTLPTracesSource(id="traces-src")
        assert src.type == "otlp.traces"

    def test_otlp_source_with_deduplication(self):
        src = OTLPLogsSource(
            id="logs-src",
            deduplication=DeduplicationConfig(
                enabled=True,
                key="trace_id",
                time_window="1h",
            ),
        )
        assert src.deduplication.enabled is True
        assert src.deduplication.key == "trace_id"

    def test_otlp_source_invalid_type(self):
        """Concrete OTLP classes reject a mismatched Literal type value."""
        with pytest.raises(ValueError):
            OTLPLogsSource(type="kafka", id="src")

    def test_otlp_source_missing_id(self):
        with pytest.raises(ValueError):
            OTLPLogsSource()

    def test_otlp_source_from_dict(self):
        data = {"type": "otlp.traces", "id": "t-src"}
        src = OTLPTracesSource.model_validate(data)
        assert src.id == "t-src"

    def test_otlp_source_isinstance_base(self):
        """All concrete OTLP types are instances of OTLPSource."""
        assert isinstance(OTLPLogsSource(id="x"), OTLPSource)
        assert isinstance(OTLPMetricsSource(id="x"), OTLPSource)
        assert isinstance(OTLPTracesSource(id="x"), OTLPSource)


class TestOTLPDeduplication:
    """Tests for DeduplicationConfig used with OTLP sources."""

    def test_otlp_deduplication_enabled(self):
        d = DeduplicationConfig(enabled=True, key="trace_id", time_window="1h")
        assert d.enabled is True
        assert d.key == "trace_id"
        assert d.time_window == "1h"

    def test_otlp_deduplication_disabled(self):
        d = DeduplicationConfig(enabled=False)
        assert d.enabled is False
        assert d.key is None
        assert d.time_window is None

    def test_otlp_deduplication_defaults(self):
        d = DeduplicationConfig()
        assert d.enabled is False
