# GlassFlow Python SDK

<p align="left">
  <a target="_blank" href="https://pypi.python.org/pypi/glassflow">
    <img src="https://img.shields.io/pypi/v/glassflow.svg?labelColor=&color=e69e3a">
  </a>
  <a target="_blank" href="https://github.com/glassflow/glassflow-python-sdk/blob/main/LICENSE.md">
    <img src="https://img.shields.io/pypi/l/glassflow.svg?labelColor=&color=e69e3a">
  </a>
  <a target="_blank" href="https://pypi.python.org/pypi/glassflow">
    <img src="https://img.shields.io/pypi/pyversions/glassflow.svg?labelColor=&color=e69e3a">
  </a>
  <br />
  <a target="_blank" href="(https://github.com/glassflow/glassflow-python-sdk/actions">
    <img src="https://github.com/glassflow/glassflow-python-sdk/workflows/Test/badge.svg?labelColor=&color=e69e3a">
  </a>
<!-- Pytest Coverage Comment:Begin -->
  <img src="https://img.shields.io/badge/coverage-90%25-brightgreen">
<!-- Pytest Coverage Comment:End -->
</p>

A Python SDK for creating and managing data pipelines between Kafka and ClickHouse.

## Features

- Create and manage data pipelines between Kafka and ClickHouse
- Ingest from Kafka sources or OTLP signals (logs, metrics, traces)
- Unified transforms pipeline: dedup, filter, and stateless transformations
- Temporal joins between sources based on a common key with a given time window
- Per-source Schema Registry integration
- Pipeline configuration via YAML or JSON
- Schema validation and configuration management
- Fine-grained resource control per pipeline component

## Installation

```bash
pip install glassflow
```

## Quick Start

### Initialize client

```python
from glassflow.etl import Client

client = Client(host="your-glassflow-etl-url")
```

### Create a pipeline

The example below uses pipeline version `v3`. See [Migrating from V2 to V3](#migrating-from-v2-to-v3) if you have existing `v2` configurations.

```python
pipeline_config = {
    "version": "v3",
    "pipeline_id": "my-pipeline-id",
    "sources": [
        {
            "type": "kafka",
            "source_id": "users",
            "connection_params": {
                "brokers": ["my.kafka.broker:9093"],
                "protocol": "PLAINTEXT",
            },
            "topic": "users",
            "consumer_group_initial_offset": "latest",
            "schema_fields": [
                {"name": "event_id",   "type": "string"},
                {"name": "user_id",    "type": "string"},
                {"name": "created_at", "type": "string"},
                {"name": "name",       "type": "string"},
                {"name": "email",      "type": "string"},
            ],
        }
    ],
    "transforms": [
        {
            "type": "dedup",
            "source_id": "users",
            "config": {
                "key": "event_id",
                "time_window": "1h",
            },
        }
    ],
    "sink": {
        "type": "clickhouse",
        "connection_params": {
            "host": "my.clickhouse.server",
            "port": "9000",
            "database": "default",
            "username": "default",
            "password": "mysecret",
            "secure": False,
        },
        "table": "users",
        "mapping": [
            {"name": "event_id",   "column_name": "event_id",   "column_type": "UUID"},
            {"name": "user_id",    "column_name": "user_id",    "column_type": "UUID"},
            {"name": "created_at", "column_name": "created_at", "column_type": "DateTime"},
            {"name": "name",       "column_name": "name",       "column_type": "String"},
            {"name": "email",      "column_name": "email",      "column_type": "String"},
        ],
    },
}

pipeline = client.create_pipeline(pipeline_config)
```

You can also load configurations from YAML or JSON files:

```python
pipeline = client.create_pipeline(
    pipeline_config_yaml_path="pipeline.yaml"
)
# or
pipeline = client.create_pipeline(
    pipeline_config_json_path="pipeline.json"
)
```

For full configuration reference — including Schema Registry, joins, OTLP sources, and resource controls — see the [GlassFlow docs](https://docs.glassflow.dev/configuration/pipeline-json-reference).

### Get pipeline

```python
pipeline = client.get_pipeline("my-pipeline-id")
```

### List pipelines

```python
pipelines = client.list_pipelines()
for pipeline in pipelines:
    print(f"Pipeline ID: {pipeline['pipeline_id']}, State: {pipeline['state']}")
```

### Stop / Terminate / Resume pipeline

```python
pipeline = client.get_pipeline("my-pipeline-id")

pipeline.stop()                                          # graceful stop → STOPPING
client.stop_pipeline("my-pipeline-id", terminate=True)  # ungraceful    → TERMINATING
pipeline.resume()                                        # restart       → RESUMING
```

### Delete pipeline

Only stopped or terminated pipelines can be deleted.

```python
client.delete_pipeline("my-pipeline-id")
# or
pipeline.delete()
```

## Migrating from V2 to V3

Pipeline version `v2` has been removed. Use `migrate_pipeline_v2_to_v3()` to convert an existing configuration automatically:

```python
from glassflow.etl import migrate_pipeline_v2_to_v3

v3_config = migrate_pipeline_v2_to_v3(v2_config)
pipeline = client.create_pipeline(v3_config)
```

If you prefer to migrate manually, the key changes are:

| Area | V2 | V3 |
|------|----|----|
| `version` | `"v2"` | `"v3"` |
| Sources | `source: {type, connection_params, topics: [...]}` | `sources: [{type, source_id, connection_params, topic, ...}]` flat list |
| Schema | top-level `schema.fields` block | `sources[].schema_fields` per source |
| Deduplication | per-topic `deduplication: {enabled, id_field, ...}` | `transforms: [{type: "dedup", source_id, config: {key, time_window}}]` |
| Filter | top-level `filter: {enabled, expression}` | `transforms: [{type: "filter", source_id, config: {expression}}]` |
| Transformation | top-level `stateless_transformation` | `transforms: [{type: "stateless", source_id, config: {transforms: [...]}}]` |
| Join | `join.sources: [{source_id, key, orientation}]` | `join: {left_source: {...}, right_source: {...}, output_fields: [...]}` |
| Sink connection | flat fields (`host`, `port`, ...) at top level | nested `sink.connection_params` object |
| Sink field mapping | top-level `schema.fields` with `source_id` | `sink.mapping` list of `{name, column_name, column_type}` |
| Resources | `pipeline_resources: {ingestor, transform, ...}` | `resources: {sources: [...], transform: [...], ...}` |
| Sink password | base64-encoded | plain text |

## Tracking

The SDK includes anonymous usage stats collection to help improve the product. It collects non-identifying information such as SDK version, Python version, and feature flags (e.g., whether joins or deduplication are enabled). No personally identifiable information is collected.

Usage states collection is enabled by default. To disable it:

```bash
export GF_USAGESTATS_ENABLED=false
```

```python
client.disable_usagestats()
```

## Development

### Setup

1. Clone the repository
2. Create a virtual environment
3. Install dependencies:

```bash
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
```

### Testing

```bash
pytest
```
