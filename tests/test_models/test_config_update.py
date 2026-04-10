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
        assert updated.sources == config.sources
        assert updated.sink == config.sink
        assert updated.transforms == config.transforms
        assert updated.join == config.join
        assert updated.metadata == config.metadata
        # Original config should be unchanged (immutable)
        assert config.name != "Updated Name"

    def test_update_sink(self, valid_config):
        """Test updating sink configuration."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(
            sink=models.SinkConfigPatch(
                connection_params=models.ClickhouseConnectionParamsPatch(
                    host="new-host", port="9000"
                )
            )
        )

        updated = config.update(patch)

        assert updated.sink.connection_params.host == "new-host"
        assert updated.sink.connection_params.port == "9000"
        assert (
            updated.sink.connection_params.database
            == config.sink.connection_params.database
        )
        assert (
            updated.sink.connection_params.username
            == config.sink.connection_params.username
        )

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
                left_source=models.JoinSourceConfig(
                    source_id="user-logins",
                    key="user_id",
                    time_window="1h",
                ),
                right_source=models.JoinSourceConfig(
                    source_id="orders",
                    key="user_id",
                    time_window="1h",
                ),
                output_fields=[
                    models.JoinOutputField(source_id="user-logins", name="session_id"),
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
            sink=models.SinkConfigPatch(
                connection_params=models.ClickhouseConnectionParamsPatch(
                    host="updated-host"
                )
            ),
        )

        updated = config.update(patch)

        assert updated.name == "Multi Update"
        assert updated.sink.connection_params.host == "updated-host"

    def test_update_empty_patch(self, valid_config):
        """Test updating with an empty patch (all None)."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch()

        updated = config.update(patch)

        # Should return a copy with no changes
        assert updated.name == config.name
        assert updated.sources == config.sources
        assert updated.sink == config.sink


class TestKafkaSourceUpdate:
    """Tests for KafkaSource.update() method."""

    def test_update_connection_params(self, valid_config):
        """Test updating Kafka connection parameters."""
        source = models.KafkaSource(**valid_config["sources"][0])
        patch = models.KafkaSourcePatch(
            connection_params=models.KafkaConnectionParamsPatch(
                brokers=["updated-broker:9092"],
                protocol=models.KafkaProtocol.PLAINTEXT,
            )
        )

        updated = source.update(patch)

        assert updated.connection_params.brokers == ["updated-broker:9092"]
        assert updated.connection_params.protocol == models.KafkaProtocol.PLAINTEXT
        assert updated.type == source.type

    def test_update_topic(self, valid_config):
        """Test updating topic name."""
        source = models.KafkaSource(**valid_config["sources"][0])
        patch = models.KafkaSourcePatch(topic="new-topic")

        updated = source.update(patch)

        assert updated.topic == "new-topic"
        assert updated.connection_params == source.connection_params

    def test_update_schema_fields(self, valid_config):
        """Test updating schema_fields."""
        source = models.KafkaSource(**valid_config["sources"][0])
        new_fields = [
            models.KafkaField(name="new_field", type=models.KafkaDataType.STRING),
        ]
        patch = models.KafkaSourcePatch(schema_fields=new_fields)

        updated = source.update(patch)

        assert len(updated.schema_fields) == 1
        assert updated.schema_fields[0].name == "new_field"
        assert len(source.schema_fields) > 1  # Original unchanged


class TestSinkConfigUpdate:
    """Tests for SinkConfig.update() method."""

    def test_update_host_port(self, valid_config):
        """Test updating sink host and port."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(
            connection_params=models.ClickhouseConnectionParamsPatch(
                host="new-host", port="8080"
            )
        )

        updated = sink.update(patch)

        assert updated.connection_params.host == "new-host"
        assert updated.connection_params.port == "8080"
        assert updated.connection_params.database == sink.connection_params.database
        assert updated.connection_params.username == sink.connection_params.username

    def test_update_credentials(self, valid_config):
        """Test updating sink credentials."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(
            connection_params=models.ClickhouseConnectionParamsPatch(
                username="new-user", password="new-password"
            )
        )

        updated = sink.update(patch)

        assert updated.connection_params.username == "new-user"
        assert updated.connection_params.password == "new-password"
        assert updated.connection_params.host == sink.connection_params.host

    def test_update_multiple_sink_fields(self, valid_config):
        """Test updating multiple sink fields at once."""
        sink = models.SinkConfig(**valid_config["sink"])
        patch = models.SinkConfigPatch(
            connection_params=models.ClickhouseConnectionParamsPatch(
                host="new-host",
                port="8080",
                database="new-db",
            ),
            table="new-table",
        )

        updated = sink.update(patch)

        assert updated.connection_params.host == "new-host"
        assert updated.connection_params.port == "8080"
        assert updated.connection_params.database == "new-db"
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
        assert updated.left_source == join.left_source

    def test_update_type(self, valid_config):
        """Test updating join type."""
        join = models.JoinConfig(**valid_config["join"])
        patch = models.JoinConfigPatch(type=models.JoinType.TEMPORAL)

        updated = join.update(patch)

        assert updated.type == models.JoinType.TEMPORAL
        assert updated.enabled == join.enabled

    def test_update_left_source(self, valid_config):
        """Test updating join left_source."""
        join = models.JoinConfig(**valid_config["join"])
        new_left = models.JoinSourceConfig(
            source_id="new-source", key="new_key", time_window="2h"
        )
        patch = models.JoinConfigPatch(left_source=new_left)

        updated = join.update(patch)

        assert updated.left_source == new_left


class TestKafkaConnectionParamsUpdate:
    """Tests for KafkaConnectionParams.update() method."""

    def test_update_brokers(self, valid_config):
        """Test updating brokers."""
        conn_params = models.KafkaConnectionParams(
            **valid_config["sources"][0]["connection_params"]
        )
        patch = models.KafkaConnectionParamsPatch(brokers=["broker1:9092"])

        updated = conn_params.update(patch)

        assert updated.brokers == ["broker1:9092"]
        assert updated.protocol == conn_params.protocol
        assert updated.mechanism == conn_params.mechanism

    def test_update_auth_fields(self, valid_config):
        """Test updating authentication fields."""
        conn_params = models.KafkaConnectionParams(
            **valid_config["sources"][0]["connection_params"]
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
