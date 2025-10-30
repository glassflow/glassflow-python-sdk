"""Tests for config update methods."""

from glassflow.etl import models


class TestPipelineConfigUpdate:
    """Tests for PipelineConfig.update() method."""

    def test_update_name(self, valid_config):
        """Test updating pipeline name."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(name="Updated Name")

        updated = config.update(patch)

        assert updated.name == "Updated Name"
        assert updated.pipeline_id == config.pipeline_id
        assert updated.source == config.source
        assert updated.sink == config.sink
        # Original config should be unchanged (immutable)
        assert config.name != "Updated Name"

    def test_update_source(self, valid_config):
        """Test updating source configuration."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(
            source=models.SourceConfigPatch(
                provider="new-provider",
                connection_params=models.KafkaConnectionParamsPatch(
                    brokers=["new-broker:9092"]
                ),
            )
        )

        updated = config.update(patch)

        assert updated.source.provider == "new-provider"
        assert updated.source.connection_params.brokers == ["new-broker:9092"]
        # Other source fields should remain unchanged
        assert updated.source.type == config.source.type
        assert updated.name == config.name

    def test_update_sink(self, valid_config):
        """Test updating sink configuration."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(
            sink=models.SinkConfigPatch(host="new-host", port="9000")
        )

        updated = config.update(patch)

        assert updated.sink.host == "new-host"
        assert updated.sink.port == "9000"
        # Other sink fields should remain unchanged
        assert updated.sink.database == config.sink.database
        assert updated.sink.username == config.sink.username

    def test_update_join(self, valid_config):
        """Test updating join configuration."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(join=models.JoinConfigPatch(enabled=False))

        updated = config.update(patch)

        assert updated.join.enabled is False
        # Original join should have been enabled
        assert config.join.enabled is True

    def test_update_join_when_none(self, valid_config_without_joins):
        """Test updating join when it's initially None."""
        config = models.PipelineConfig(**valid_config_without_joins)
        assert config.join is None or config.join.enabled is False

        patch = models.PipelineConfigPatch(
            join=models.JoinConfigPatch(
                enabled=True,
                type=models.JoinType.TEMPORAL,
                sources=[
                    models.JoinSourceConfig(
                        source_id="user_logins",
                        join_key="user_id",
                        time_window="1h",
                        orientation=models.JoinOrientation.LEFT,
                    ),
                    models.JoinSourceConfig(
                        source_id="orders",
                        join_key="user_id",
                        time_window="1h",
                        orientation=models.JoinOrientation.RIGHT,
                    ),
                ],
            )
        )

        updated = config.update(patch)

        assert updated.join.enabled is True
        assert updated.join.type == models.JoinType.TEMPORAL

    def test_update_multiple_fields(self, valid_config):
        """Test updating multiple fields at once."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(
            name="Multi Update",
            source=models.SourceConfigPatch(provider="updated-provider"),
            sink=models.SinkConfigPatch(host="updated-host"),
        )

        updated = config.update(patch)

        assert updated.name == "Multi Update"
        assert updated.source.provider == "updated-provider"
        assert updated.sink.host == "updated-host"

    def test_update_empty_patch(self, valid_config):
        """Test updating with an empty patch (all None)."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch()

        updated = config.update(patch)

        # Should return a copy with no changes
        assert updated.name == config.name
        assert updated.source == config.source
        assert updated.sink == config.sink

    def test_update_partial_nested(self, valid_config):
        """Test updating only part of a nested configuration."""
        config = models.PipelineConfig(**valid_config)
        original_brokers = config.source.connection_params.brokers
        original_protocol = config.source.connection_params.protocol

        patch = models.PipelineConfigPatch(
            source=models.SourceConfigPatch(
                connection_params=models.KafkaConnectionParamsPatch(
                    username="new-username"
                )
            )
        )

        updated = config.update(patch)

        # Only username should change
        assert updated.source.connection_params.username == "new-username"
        # Other connection params should remain unchanged
        assert updated.source.connection_params.brokers == original_brokers
        assert updated.source.connection_params.protocol == original_protocol


class TestSourceConfigUpdate:
    """Tests for SourceConfig.update() method."""

    def test_update_connection_params(self, valid_config):
        """Test updating Kafka connection parameters."""
        from glassflow.etl.models.source import KafkaProtocol

        source = models.SourceConfig(**valid_config["source"])
        patch = models.SourceConfigPatch(
            connection_params=models.KafkaConnectionParamsPatch(
                brokers=["updated-broker:9092"],
                protocol=KafkaProtocol.PLAINTEXT,
            )
        )

        updated = source.update(patch)

        assert updated.connection_params.brokers == ["updated-broker:9092"]
        from glassflow.etl.models.source import KafkaProtocol

        assert updated.connection_params.protocol == KafkaProtocol.PLAINTEXT
        # Other fields should remain unchanged
        assert updated.type == source.type
        assert updated.provider == source.provider

    def test_update_topics(self, valid_config):
        """Test updating topics with full TopicConfig objects (no partial patch)."""
        source = models.SourceConfig(**valid_config["source"])
        new_topic = models.TopicConfig(
            name="new-topic",
            schema=models.Schema(
                type=models.SchemaType.JSON,
                fields=[
                    models.SchemaField(name="id", type=models.KafkaDataType.STRING)
                ],
            ),
        )
        patch = models.SourceConfigPatch(topics=[new_topic])

        updated = source.update(patch)

        assert len(updated.topics) == 1
        assert updated.topics[0].name == "new-topic"
        # Topics list is replaced, not merged
        assert len(source.topics) > 1

    def test_update_provider(self, valid_config):
        """Test updating provider."""
        source = models.SourceConfig(**valid_config["source"])
        patch = models.SourceConfigPatch(provider="updated-provider")

        updated = source.update(patch)

        assert updated.provider == "updated-provider"
        assert updated.connection_params == source.connection_params


class TestSinkConfigUpdate:
    """Tests for SinkConfig.update() method."""

    def test_update_host_port(self, valid_config):
        """Test updating sink host and port."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(host="new-host", port="8080")

        updated = sink.update(patch)

        assert updated.host == "new-host"
        assert updated.port == "8080"
        # Other fields should remain unchanged
        assert updated.database == sink.database
        assert updated.username == sink.username

    def test_update_credentials(self, valid_config):
        """Test updating sink credentials."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(username="new-user", password="new-password")

        updated = sink.update(patch)

        assert updated.username == "new-user"
        assert updated.password == "new-password"
        assert updated.host == sink.host

    def test_update_table_mapping(self, valid_config):
        """Test updating table mapping."""
        sink = models.SinkConfig(**valid_config["sink"])
        new_mapping = [
            models.TableMapping(
                source_id="test",
                field_name="id",
                column_name="id",
                column_type=models.ClickhouseDataType.STRING,
            )
        ]
        patch = models.SinkConfigPatch(table_mapping=new_mapping)

        updated = sink.update(patch)

        assert len(updated.table_mapping) == 1
        assert updated.table_mapping[0].source_id == "test"

    def test_update_multiple_sink_fields(self, valid_config):
        """Test updating multiple sink fields at once."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(
            host="new-host",
            port="8080",
            database="new-db",
            table="new-table",
        )

        updated = sink.update(patch)

        assert updated.host == "new-host"
        assert updated.port == "8080"
        assert updated.database == "new-db"
        assert updated.table == "new-table"


class TestJoinConfigUpdate:
    """Tests for JoinConfig.update() method."""

    def test_update_enabled(self, valid_config):
        """Test updating join enabled status."""
        join = models.JoinConfig(**valid_config["join"])
        patch = models.JoinConfigPatch(enabled=False)

        updated = join.update(patch)

        assert updated.enabled is False
        assert updated.type == join.type
        assert updated.sources == join.sources

    def test_update_type(self, valid_config):
        """Test updating join type."""
        join = models.JoinConfig(**valid_config["join"])
        patch = models.JoinConfigPatch(type=models.JoinType.TEMPORAL)

        updated = join.update(patch)

        assert updated.type == models.JoinType.TEMPORAL
        assert updated.enabled == join.enabled

    def test_update_sources(self, valid_config):
        """Test updating join sources."""
        join = models.JoinConfig(**valid_config["join"])
        new_sources = [
            models.JoinSourceConfig(
                source_id="source1",
                join_key="key1",
                time_window="2h",
                orientation=models.JoinOrientation.LEFT,
            ),
            models.JoinSourceConfig(
                source_id="source2",
                join_key="key2",
                time_window="2h",
                orientation=models.JoinOrientation.RIGHT,
            ),
        ]
        patch = models.JoinConfigPatch(sources=new_sources)

        updated = join.update(patch)

        assert updated.sources == new_sources
        assert len(updated.sources) == 2


class TestKafkaConnectionParamsUpdate:
    """Tests for KafkaConnectionParams.update() method."""

    def test_update_brokers(self, valid_config):
        """Test updating brokers."""
        conn_params = models.KafkaConnectionParams(
            **valid_config["source"]["connection_params"]
        )
        patch = models.KafkaConnectionParamsPatch(brokers=["broker1:9092"])

        updated = conn_params.update(patch)

        assert updated.brokers == ["broker1:9092"]
        # Other fields should remain unchanged
        assert updated.protocol == conn_params.protocol
        assert updated.mechanism == conn_params.mechanism

    def test_update_auth_fields(self, valid_config):
        """Test updating authentication fields."""
        conn_params = models.KafkaConnectionParams(
            **valid_config["source"]["connection_params"]
        )
        patch = models.KafkaConnectionParamsPatch(
            username="new-user",
            password="new-pass",
            mechanism=models.KafkaMechanism.PLAIN,
        )

        updated = conn_params.update(patch)

        assert updated.username == "new-user"
        assert updated.password == "new-pass"
        assert updated.mechanism == models.KafkaMechanism.PLAIN


class TestDeduplicationConfigUpdate:
    """Tests for DeduplicationConfig.update() method."""

    def test_update_enabled(self, valid_config):
        """Test updating deduplication enabled status."""
        dedup = models.DeduplicationConfig(
            **valid_config["source"]["topics"][0]["deduplication"]
        )
        patch = models.DeduplicationConfigPatch(enabled=False)

        updated = dedup.update(patch)

        assert updated.enabled is False
        # Other fields should remain unchanged
        assert updated.id_field == dedup.id_field
        assert updated.time_window == dedup.time_window

    def test_update_id_field(self, valid_config):
        """Test updating deduplication id field."""
        dedup = models.DeduplicationConfig(
            **valid_config["source"]["topics"][0]["deduplication"]
        )
        patch = models.DeduplicationConfigPatch(
            id_field="new_id_field", id_field_type=models.KafkaDataType.INT
        )

        updated = dedup.update(patch)

        assert updated.id_field == "new_id_field"
        assert updated.id_field_type == models.KafkaDataType.INT
