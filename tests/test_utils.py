"""Tests for pipeline migration utilities."""

from glassflow.etl import models
from glassflow.etl.models.pipeline import PipelineVersion
from glassflow.etl.utils import migrate_pipeline_v2_to_v3


def _v2_dedup_pipeline() -> dict:
    return {
        "version": "v2",
        "pipeline_id": "dedup-pipeline",
        "source": {
            "type": "kafka",
            "provider": "confluent",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [
                {
                    "name": "users",
                    "consumer_group_initial_offset": "earliest",
                    "deduplication": {
                        "enabled": True,
                        "id_field": "event_id",
                        "id_field_type": "string",
                        "time_window": "1h",
                    },
                }
            ],
        },
        "schema": {
            "fields": [
                {
                    "source_id": "users",
                    "name": "event_id",
                    "type": "string",
                    "column_name": "event_id",
                    "column_type": "String",
                },
                {
                    "source_id": "users",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
            ]
        },
        "sink": {
            "type": "clickhouse",
            "host": "clickhouse",
            "port": "9000",
            "http_port": "8123",
            "database": "default",
            "username": "default",
            "password": "secret",
            "secure": False,
            "table": "users_dedup",
            "max_batch_size": 1000,
        },
        "join": {"enabled": False},
        "filter": {"enabled": False},
    }


def _v2_join_pipeline() -> dict:
    return {
        "version": "v2",
        "pipeline_id": "join-pipeline",
        "source": {
            "type": "kafka",
            "provider": "aiven",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [
                {
                    "name": "user-logins",
                    "consumer_group_initial_offset": "earliest",
                    "deduplication": {
                        "enabled": True,
                        "id_field": "session_id",
                        "id_field_type": "string",
                        "time_window": "12h",
                    },
                },
                {
                    "name": "orders",
                    "consumer_group_initial_offset": "earliest",
                    "deduplication": {
                        "enabled": True,
                        "id_field": "order_id",
                        "id_field_type": "string",
                        "time_window": "12h",
                    },
                },
            ],
        },
        "schema": {
            "fields": [
                {
                    "source_id": "user-logins",
                    "name": "session_id",
                    "type": "string",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {
                    "source_id": "user-logins",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
                {
                    "source_id": "orders",
                    "name": "order_id",
                    "type": "string",
                    "column_name": "order_id",
                    "column_type": "String",
                },
                {
                    "source_id": "orders",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "order_user_id",
                    "column_type": "String",
                },
            ]
        },
        "join": {
            "enabled": True,
            "type": "temporal",
            "sources": [
                {
                    "source_id": "user-logins",
                    "join_key": "user_id",
                    "join_key_type": "string",
                    "time_window": "1h",
                    "orientation": "left",
                },
                {
                    "source_id": "orders",
                    "join_key": "user_id",
                    "join_key_type": "string",
                    "time_window": "1h",
                    "orientation": "right",
                },
            ],
        },
        "sink": {
            "type": "clickhouse",
            "host": "clickhouse",
            "port": "9000",
            "database": "default",
            "username": "default",
            "password": "secret",
            "secure": False,
            "table": "user_orders",
        },
    }


def _v2_transform_pipeline() -> dict:
    return {
        "version": "v2",
        "pipeline_id": "transform-pipeline",
        "source": {
            "type": "kafka",
            "provider": "confluent",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [
                {
                    "name": "users",
                    "consumer_group_initial_offset": "earliest",
                    "deduplication": {"enabled": False},
                }
            ],
        },
        "stateless_transformation": {
            "enabled": True,
            "id": "upper_transform",
            "type": "expr_lang_transform",
            "source_id": "users",
            "config": {
                "transform": [
                    {
                        "expression": "upper(name)",
                        "output_name": "upper_name",
                        "output_type": "string",
                    },
                    {
                        "expression": "user_id",
                        "output_name": "user_id",
                        "output_type": "string",
                    },
                ]
            },
        },
        "schema": {
            "fields": [
                {
                    "source_id": "upper_transform",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
                {
                    "source_id": "upper_transform",
                    "name": "upper_name",
                    "type": "string",
                    "column_name": "upper_name",
                    "column_type": "String",
                },
            ]
        },
        "join": {"enabled": False},
        "sink": {
            "type": "clickhouse",
            "host": "clickhouse",
            "port": "9000",
            "database": "default",
            "username": "default",
            "password": "secret",
            "secure": False,
            "table": "users_transformed",
        },
    }


def _v2_filter_pipeline() -> dict:
    return {
        "version": "v2",
        "pipeline_id": "filter-pipeline",
        "source": {
            "type": "kafka",
            "provider": "confluent",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [
                {
                    "name": "events",
                    "consumer_group_initial_offset": "earliest",
                    "deduplication": {"enabled": False},
                }
            ],
        },
        "schema": {
            "fields": [
                {
                    "source_id": "events",
                    "name": "event_id",
                    "type": "string",
                    "column_name": "event_id",
                    "column_type": "String",
                },
                {
                    "source_id": "events",
                    "name": "status",
                    "type": "string",
                    "column_name": "status",
                    "column_type": "String",
                },
            ]
        },
        "filter": {
            "enabled": True,
            "expression": "status == 'active'",
        },
        "join": {"enabled": False},
        "sink": {
            "type": "clickhouse",
            "host": "clickhouse",
            "port": "9000",
            "database": "default",
            "username": "default",
            "password": "secret",
            "secure": False,
            "table": "events_filtered",
        },
    }


class TestMigratePipelineV2ToV3:
    def test_returns_pipeline_config(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert isinstance(result, models.PipelineConfig)

    def test_version_set_to_v3(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.version == PipelineVersion.V3

    def test_pipeline_id_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.pipeline_id == "dedup-pipeline"

    # --- sources -----------------------------------------------------------

    def test_sources_flattened(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert len(result.sources) == 1
        assert result.sources[0].source_id == "users"
        assert result.sources[0].topic == "users"
        assert result.sources[0].type == "kafka"

    def test_sources_have_connection_params(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.sources[0].connection_params.brokers == ["kafka:9092"]

    # --- dedup as transforms ------------------------------------------------

    def test_dedup_migrated_to_transform(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        dedup_transforms = [
            t for t in (result.transforms or []) if t.type == models.TransformType.DEDUP
        ]
        assert len(dedup_transforms) == 1
        assert dedup_transforms[0].config.key == "event_id"
        assert dedup_transforms[0].config.time_window == "1h"

    # --- source schema_fields from top-level schema -------------------------

    def test_source_schema_fields_migrated(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        src = result.sources[0]
        assert src.schema_fields is not None
        names = [f.name for f in src.schema_fields]
        assert "event_id" in names
        assert "user_id" in names

    # --- sink connection_params ---------------------------------------------

    def test_sink_connection_params_nested(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        cp = result.sink.connection_params
        assert cp.host == "clickhouse"
        assert cp.port == "9000"
        assert cp.database == "default"

    def test_sink_flat_fields_removed(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        sink_dict = result.sink.model_dump()
        assert "host" not in sink_dict
        assert "port" not in sink_dict

    # --- sink mapping from top-level schema ---------------------------------

    def test_sink_mapping_created(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.sink.mapping is not None
        assert len(result.sink.mapping) == 2

    def test_sink_mapping_names(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        names = [m.name for m in result.sink.mapping]
        assert "event_id" in names
        assert "user_id" in names

    def test_sink_source_id_derived_from_schema(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.sink.source_id == "users"

    # --- join ---------------------------------------------------------------

    def test_join_migrated_to_left_right(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        assert result.join.enabled is True
        assert result.join.left_source is not None
        assert result.join.right_source is not None
        assert result.join.left_source.source_id == "user-logins"
        assert result.join.left_source.key == "user_id"
        assert result.join.right_source.source_id == "orders"
        assert result.join.right_source.key == "user_id"

    def test_join_output_fields_populated_from_schema(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        assert result.join.output_fields is not None
        field_pairs = {(f.source_id, f.name) for f in result.join.output_fields}
        assert ("user-logins", "session_id") in field_pairs
        assert ("user-logins", "user_id") in field_pairs
        assert ("orders", "order_id") in field_pairs
        assert ("orders", "user_id") in field_pairs

    def test_multiple_sources_flattened(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        source_ids = [s.source_id for s in result.sources]
        assert "user-logins" in source_ids
        assert "orders" in source_ids
        for src in result.sources:
            assert src.schema_fields is not None

    # --- filter migration -----------------------------------------------------

    def test_filter_migrated_to_transform(self):
        result = migrate_pipeline_v2_to_v3(_v2_filter_pipeline())
        filter_transforms = [
            t
            for t in (result.transforms or [])
            if t.type == models.TransformType.FILTER
        ]
        assert len(filter_transforms) == 1
        assert filter_transforms[0].config.expression == "status == 'active'"
        assert filter_transforms[0].source_id == "events"

    def test_filter_pipeline_returns_valid_config(self):
        result = migrate_pipeline_v2_to_v3(_v2_filter_pipeline())
        assert isinstance(result, models.PipelineConfig)
        assert result.pipeline_id == "filter-pipeline"

    # --- input is not mutated -----------------------------------------------

    def test_original_dict_not_mutated(self):
        original = _v2_dedup_pipeline()
        migrate_pipeline_v2_to_v3(original)
        assert original["version"] == "v2"
        assert "id_field" in original["source"]["topics"][0]["deduplication"]
        assert "schema" in original
        assert "host" in original["sink"]


class TestMigratePipelineV2ToV3WithTransformation:
    def test_returns_pipeline_config(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        assert isinstance(result, models.PipelineConfig)

    def test_transformation_migrated_to_stateless_transform(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        stateless = [
            t
            for t in (result.transforms or [])
            if t.type == models.TransformType.STATELESS
        ]
        assert len(stateless) == 1
        assert stateless[0].source_id == "users"

    def test_transformation_config_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        stateless = [
            t
            for t in (result.transforms or [])
            if t.type == models.TransformType.STATELESS
        ]
        transform = stateless[0].config.transforms[0]
        assert transform.expression == "upper(name)"
        assert transform.output_name == "upper_name"

    def test_sink_source_id_derived_from_transformation(self):
        """sink.source_id should point to the transformation, not the topic."""
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        assert result.sink.source_id == "upper_transform"

    def test_sink_mapping_includes_transformation_output(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        names = [m.name for m in result.sink.mapping]
        assert "upper_name" in names

    def test_sink_mapping_includes_source_fields(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        names = [m.name for m in result.sink.mapping]
        assert "user_id" in names

    def test_sink_connection_params_nested(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        assert result.sink.connection_params.host == "clickhouse"
        assert result.sink.connection_params.database == "default"
