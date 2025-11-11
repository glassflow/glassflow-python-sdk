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
  <img src=https://img.shields.io/badge/coverage-93%25-brightgreen>
<!-- Pytest Coverage Comment:End -->
</p>

A Python SDK for creating and managing data pipelines between Kafka and ClickHouse.

## Features

- Create and manage data pipelines between Kafka and ClickHouse
- Deduplication of events during a time window based on a key
- Temporal joins between topics based on a common key with a given time window
- Schema validation and configuration management

## Installation

```bash
pip install glassflow
```

## Quick Start

### Initialize client

```python
from glassflow.etl import Client

# Initialize GlassFlow client
client = Client(host="your-glassflow-etl-url")
```

### Create a pipeline

```python
pipeline_config = {
    "pipeline_id": "my-pipeline-id",
    "source": {
      "type": "kafka",
      "connection_params": {
        "brokers": [
          "http://my.kafka.broker:9093"
        ],
        "protocol": "PLAINTEXT",
        "skip_auth": True
      },
      "topics": [
        {
          "consumer_group_initial_offset": "latest",
          "name": "users",
          "schema": {
            "type": "json",
            "fields": [
              {
                "name": "event_id",
                "type": "string"
              },
              {
                "name": "user_id",
                "type": "string"
              },
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "email",
                "type": "string"
              },
              {
                "name": "created_at",
                "type": "string"
              }
            ]
          },
          "deduplication": {
            "enabled": True,
            "id_field": "event_id",
            "id_field_type": "string",
            "time_window": "1h"
          }
        }
      ]
    },
    "join": {
      "enabled": False
    },
    "sink": {
      "type": "clickhouse",
      "host": "http://my.clickhouse.server",
      "port": "9000",
      "database": "default",
      "username": "default",
      "password": "c2VjcmV0",
      "secure": False,
      "max_batch_size": 1000,
      "max_delay_time": "30s",
      "table": "users_dedup",
      "table_mapping": [
        {
          "source_id": "users",
          "field_name": "event_id",
          "column_name": "event_id",
          "column_type": "UUID"
        },
        {
          "source_id": "users",
          "field_name": "user_id",
          "column_name": "user_id",
          "column_type": "UUID"
        },
        {
          "source_id": "users",
          "field_name": "created_at",
          "column_name": "created_at",
          "column_type": "DateTime"
        },
        {
          "source_id": "users",
          "field_name": "name",
          "column_name": "name",
          "column_type": "String"
        },
        {
          "source_id": "users",
          "field_name": "email",
          "column_name": "email",
          "column_type": "String"
        }
      ]
    }
}

# Create a pipeline
pipeline = client.create_pipeline(pipeline_config)
```


## Get pipeline

```python
# Get a pipeline by ID
pipeline = client.get_pipeline("my-pipeline-id")
```

### List pipelines

```python
pipelines = client.list_pipelines()
for pipeline in pipelines:
    print(f"Pipeline ID: {pipeline['pipeline_id']}")
    print(f"Name: {pipeline['name']}")
    print(f"Transformation Type: {pipeline['transformation_type']}")
    print(f"Created At: {pipeline['created_at']}")
    print(f"State: {pipeline['state']}")
```

### Stop / Terminate / Resume Pipeline

```python
pipeline = client.get_pipeline("my-pipeline-id")
pipeline.stop()
print(pipeline.status)
```

```
STOPPING
```

```python
# Stop a pipeline ungracefully (terminate)
client.stop_pipeline("my-pipeline-id", terminate=True)
print(pipeline.status)
```

```
TERMINATING
```

```python
pipeline = client.get_pipeline("my-pipeline-id")
pipeline.resume()
print(pipeline.status)
```

```
RESUMING
```

### Delete pipeline

Only stopped or terminated pipelines can be deleted.

```python
# Delete a pipeline
client.delete_pipeline("my-pipeline-id")

# Or delete via pipeline instance
pipeline.delete()
```

## Pipeline Configuration

For detailed information about the pipeline configuration, see [GlassFlow docs](https://docs.glassflow.dev/configuration/pipeline-json-reference).

## Tracking

The SDK includes anonymous usage tracking to help improve the product. Tracking is enabled by default but can be disabled in two ways:

1. Using an environment variable:
```bash
export GF_TRACKING_ENABLED=false
```

2. Programmatically using the `disable_tracking` method:
```python
from glassflow.etl import Client

client = Client(host="my-glassflow-host")
client.disable_tracking()
```

The tracking collects anonymous information about:
- SDK version
- Platform (operating system)
- Python version
- Pipeline ID
- Whether joins or deduplication are enabled
- Kafka security protocol, auth mechanism used and whether authentication is disabled
- Errors during pipeline creation and deletion

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
