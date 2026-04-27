"""Tests for source models (Kafka and OTLP)."""

import pytest

from glassflow.etl.models.sources import (
    OTLPLogsSource,
    OTLPMetricsSource,
    OTLPSource,
    OTLPTracesSource,
)


class TestOTLPSource:
    """Tests for the OTLPSource model."""

    def test_otlp_source_logs(self):
        src = OTLPLogsSource(source_id="logs-src")
        assert src.type == "otlp.logs"
        assert src.source_id == "logs-src"

    def test_otlp_source_metrics(self):
        src = OTLPMetricsSource(source_id="metrics-src")
        assert src.type == "otlp.metrics"

    def test_otlp_source_traces(self):
        src = OTLPTracesSource(source_id="traces-src")
        assert src.type == "otlp.traces"

    def test_otlp_source_invalid_type(self):
        """Concrete OTLP classes reject a mismatched Literal type value."""
        with pytest.raises(ValueError):
            OTLPLogsSource(type="kafka", source_id="src")

    def test_otlp_source_missing_source_id(self):
        with pytest.raises(ValueError):
            OTLPLogsSource()

    def test_otlp_source_from_dict(self):
        data = {"type": "otlp.traces", "source_id": "t-src"}
        src = OTLPTracesSource.model_validate(data)
        assert src.source_id == "t-src"

    def test_otlp_source_isinstance_base(self):
        """All concrete OTLP types are instances of OTLPSource."""
        assert isinstance(OTLPLogsSource(source_id="x"), OTLPSource)
        assert isinstance(OTLPMetricsSource(source_id="x"), OTLPSource)
        assert isinstance(OTLPTracesSource(source_id="x"), OTLPSource)
