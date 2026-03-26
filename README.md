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
  <img src=https://img.shields.io/badge/coverage-92%25-brightgreen>
<!-- Pytest Coverage Comment:End -->
</p>

A Python SDK for creating and managing data pipelines between Kafka and ClickHouse.

## Features

- Create and manage data pipelines between Kafka and ClickHouse
- Ingest from Kafka topics or OTLP signals (logs, metrics, traces)
- Deduplication of events during a time window based on a key
- Temporal joins between topics based on a common key with a given time window
- Per-topic Schema Registry integration
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
    "source": {
        "type": "kafka",
        "connection_params": {
            "brokers": ["http://my.kafka.broker:9093"],
            "protocol": "PLAINTEXT",
            "mechanism": "NO_AUTH"
        },
        "topics": [
            {
                "id": "users",
                "name": "users",
                "consumer_group_initial_offset": "latest",
                "deduplication": {
                    "enabled": True,
                    "key": "event_id",
                    "time_window": "1h"
                },
                "schema_fields": [
                    {"name": "event_id",   "type": "string"},
                    {"name": "user_id",    "type": "string"},
                    {"name": "created_at", "type": "string"},
                    {"name": "name",       "type": "string"},
                    {"name": "email",      "type": "string"}
                ]
            }
        ]
    },
    "join": {"enabled": False},
    "sink": {
        "type": "clickhouse",
        "source_id": "users",
        "connection_params": {
            "host": "http://my.clickhouse.server",
            "port": "9000",
            "database": "default",
            "username": "default",
            "password": "mysecret",
            "secure": False
        },
        "mapping": [
            {"name": "event_id",   "column_name": "event_id",   "column_type": "UUID"},
            {"name": "user_id",    "column_name": "user_id",    "column_type": "UUID"},
            {"name": "created_at", "column_name": "created_at", "column_type": "DateTime"},
            {"name": "name",       "column_name": "name",       "column_type": "String"},
            {"name": "email",      "column_name": "email",      "column_type": "String"}
        ]
    }
}

pipeline = client.create_pipeline(pipeline_config)
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
| Topics | no `id` field | `id: "<topic-name>"` required |
| Schema | top-level `schema.fields` block | `source.topics[].schema_fields` per topic |
| Sink connection | flat fields (`host`, `port`, …) at top level | nested `sink.connection_params` object |
| Sink field mapping | top-level `schema.fields` with `source_id` | `sink.mapping` list of `{name, column_name, column_type}` |
| Deduplication key | `id_field` | `key` |
| Join key | `join_key` | `key` |
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
