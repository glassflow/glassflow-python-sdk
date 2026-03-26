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

    # --- deduplication ---------------------------------------------------

    def test_dedup_id_field_migrated_to_key(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.source.topics[0].deduplication.key == "event_id"

    def test_dedup_id_field_type_removed(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        dedup = result.source.topics[0].deduplication
        assert not hasattr(dedup, "id_field_type")

    def test_dedup_time_window_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert result.source.topics[0].deduplication.time_window == "1h"

    # --- topic schema_fields from top-level schema -----------------------

    def test_topic_schema_fields_migrated(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        topic = result.source.topics[0]
        assert topic.schema_fields is not None
        names = [f.name for f in topic.schema_fields]
        assert "event_id" in names
        assert "user_id" in names

    def test_top_level_schema_removed(self):
        result = migrate_pipeline_v2_to_v3(_v2_dedup_pipeline())
        assert "schema" not in result.model_fields_set

    # --- sink connection_params ------------------------------------------

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

    # --- sink mapping from top-level schema ------------------------------

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

    # --- join ------------------------------------------------------------

    def test_join_key_migrated_from_join_key(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        for src in result.join.sources:
            assert src.key == "user_id"

    def test_join_key_type_removed(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        for src in result.join.sources:
            assert not hasattr(src, "join_key_type")

    def test_join_fields_populated_from_schema(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        assert result.join.fields is not None
        field_pairs = {(f.source_id, f.name) for f in result.join.fields}
        assert ("user-logins", "session_id") in field_pairs
        assert ("user-logins", "user_id") in field_pairs
        assert ("orders", "order_id") in field_pairs
        assert ("orders", "user_id") in field_pairs

    def test_join_orientations_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        orientations = {s.source_id: s.orientation for s in result.join.sources}
        assert orientations["user-logins"] == models.JoinOrientation.LEFT
        assert orientations["orders"] == models.JoinOrientation.RIGHT

    def test_multiple_topic_schema_fields(self):
        result = migrate_pipeline_v2_to_v3(_v2_join_pipeline())
        topic_names = [t.name for t in result.source.topics]
        assert "user-logins" in topic_names
        assert "orders" in topic_names
        for topic in result.source.topics:
            assert topic.schema_fields is not None

    # --- input is not mutated --------------------------------------------

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

    def test_transformation_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        st = result.stateless_transformation
        assert st.enabled is True
        assert st.id == "upper_transform"
        assert st.source_id == "users"

    def test_transformation_config_preserved(self):
        result = migrate_pipeline_v2_to_v3(_v2_transform_pipeline())
        transform = result.stateless_transformation.config.transform[0]
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
