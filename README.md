# GlassFlow ETL Python SDK

<p align="left">
  <a target="_blank" href="https://pypi.python.org/pypi/glassflow">
    <img src="https://img.shields.io/pypi/v/glassflow.svg?labelColor=&color=e69e3a">
  </a>
  <a target="_blank" href="https://github.com/glassflow/glassflow-python-sdk/blob/main/LICENSE">
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
  <img src=https://img.shields.io/badge/coverage-94%25-brightgreen>
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

```python
from glassflow.etl import Pipeline


pipeline_config = {
  "pipeline_id": "test-pipeline",
  "source": {
    "type": "kafka",
    "provider": "aiven",
    "connection_params": {
      "brokers": ["localhoust:9092"],
      "protocol": "SASL_SSL",
      "mechanism": "SCRAM-SHA-256",
      "username": "user",
      "password": "pass"
    }
    "topics": [
      {
        "consumer_group_initial_offset": "earliest",
        "id": "test-topic",
        "name": "test-topic",
        "schema": {
          "type": "json",
          "fields": [
            {"name": "id", "type": "string" },
            {"name": "email", "type": "string"}
          ]
        },
        "deduplication": {
          "id_field": "id",
          "id_field_type": "string",
          "time_window": "1h",
          "enabled": True
        }
      }
    ],
  },
  "sink": {
    "type": "clickhouse",
    "host": "localhost:8443",
    "port": 8443,
    "database": "test",
    "username": "default",
    "password": "pass",
    "table_mapping": [
      {
        "source_id": "test_table",
        "field_name": "id",
        "column_name": "user_id",
        "column_type": "UUID"
      },
      {
        "source_id": "test_table",
        "field_name": "email",
        "column_name": "email",
        "column_type": "String"
      }
    ]
  }
}

# Create a pipeline from a JSON configuration
pipeline = Pipeline(pipeline_config)

# Create the pipeline
pipeline.create()
```

## Pipeline Configuration

For detailed information about the pipeline configuration, see [GlassFlow docs](https://docs.glassflow.dev/pipeline/pipeline-configuration).

## Tracking

The SDK includes anonymous usage tracking to help improve the product. Tracking is enabled by default but can be disabled in two ways:

1. Using an environment variable:
```bash
export GF_TRACKING_ENABLED=false
```

2. Programmatically using the `disable_tracking` method:
```python
pipeline = Pipeline(pipeline_config)
pipeline.disable_tracking()
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
