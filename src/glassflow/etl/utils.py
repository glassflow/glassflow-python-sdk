from typing import Any

from . import models


def migrate_pipeline_v1_to_v2(pipeline: dict[str, Any]) -> models.PipelineConfig:
    """Migrate a pipeline from v1 to v2."""
    schema_fields = {}
    for topic in pipeline["source"]["topics"]:
        for field in topic["schema"]["fields"]:
            schema_fields[f"{topic['name']}_{field['name']}"] = {
                "source_id": topic["name"],
                "name": field["name"],
                "type": field["type"],
            }

    for field in pipeline["sink"]["table_mapping"]:
        schema_fields[f"{field['source_id']}_{field['field_name']}"].update(
            {"column_name": field["column_name"], "column_type": field["column_type"]}
        )

    schema = models.Schema(
        fields=[
            models.SchemaField.model_validate(field) for field in schema_fields.values()
        ]
    )

    return models.PipelineConfig(
        pipeline_id=pipeline["pipeline_id"],
        name=pipeline.get("name", None),
        source=models.SourceConfig.model_validate(pipeline["source"]),
        join=models.JoinConfig.model_validate(pipeline.get("join", {})),
        sink=models.SinkConfig.model_validate(pipeline["sink"]),
        schema=schema,
    )
