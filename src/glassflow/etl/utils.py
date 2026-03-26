import copy
from typing import Any

from . import models


def migrate_pipeline_v2_to_v3(pipeline: dict[str, Any]) -> models.PipelineConfig:
    """Migrate a pipeline configuration from v2 to v3.

    Changes applied:
    - version: "v2" → "v3"
    - top-level ``schema.fields`` split into per-topic ``source.topics[].schema_fields``
      (source field names/types) and ``sink.mapping`` (column mapping)
    - topic deduplication: ``id_field`` → ``key``, ``id_field_type`` removed
    - join sources: ``join_key`` → ``key``, ``join_key_type`` removed
    - join: ``fields`` populated from top-level ``schema.fields`` (one entry per field
      per join source)
    - sink: flat connection fields (host, port, …) → nested ``connection_params``
    - sink: ``source_id`` derived from stateless transformation, join, or topic name
    - top-level ``schema`` key removed

    Args:
        pipeline: V2 pipeline configuration as a plain dict.

    Returns:
        PipelineConfig: Validated V3 pipeline configuration.
    """
    config = copy.deepcopy(pipeline)
    config["version"] = "v3"

    # --- top-level schema → per-topic schema_fields + sink.mapping -------
    schema_fields = (config.pop("schema", None) or {}).get("fields", [])

    if schema_fields:
        # Group source fields by topic name (source_id)
        fields_by_topic: dict[str, list[dict]] = {}
        for field in schema_fields:
            source_id = field.get("source_id", "")
            fields_by_topic.setdefault(source_id, []).append(field)

        # Attach schema_fields to matching topics
        for topic in config.get("source", {}).get("topics", []):
            topic_fields = fields_by_topic.get(topic["name"], [])
            if topic_fields:
                topic["schema_fields"] = [
                    {"name": f["name"], "type": f["type"]} for f in topic_fields
                ]

        # Build sink.mapping from fields that have column info
        sink_mapping = [
            {
                "name": f["name"],
                "column_name": f["column_name"],
                "column_type": f["column_type"],
            }
            for f in schema_fields
            if f.get("column_name") and f.get("column_type")
        ]
        if sink_mapping:
            config.setdefault("sink", {})["mapping"] = sink_mapping

    # --- source topics: deduplication ------------------------------------
    for topic in config.get("source", {}).get("topics", []):
        dedup = topic.get("deduplication")
        if isinstance(dedup, dict):
            if "id_field" in dedup:
                dedup["key"] = dedup.pop("id_field")
            dedup.pop("id_field_type", None)

    # --- join ------------------------------------------------------------
    join = config.get("join") or {}
    if join.get("enabled"):
        for src in join.get("sources") or []:
            if "join_key" in src:
                src["key"] = src.pop("join_key")
            src.pop("join_key_type", None)

        # Build join.fields from schema fields belonging to join sources
        join_source_ids = {
            src["source_id"] for src in (join.get("sources") or [])
        }
        join["fields"] = [
            {"source_id": f["source_id"], "name": f["name"]}
            for f in schema_fields
            if f.get("source_id") in join_source_ids
        ]


    # --- sink: flat connection fields → connection_params ----------------
    sink = config.get("sink", {})
    _conn_keys = {
        "host",
        "port",
        "http_port",
        "database",
        "username",
        "password",
        "secure",
        "skip_certificate_verification",
    }
    connection_params = {k: sink.pop(k) for k in _conn_keys if k in sink}
    if connection_params:
        sink["connection_params"] = connection_params

    # --- sink: derive source_id ------------------------------------------
    if "source_id" not in sink:
        st = config.get("stateless_transformation") or {}
        join_cfg = config.get("join") or {}
        if st.get("enabled") and st.get("id"):
            sink["source_id"] = st["id"]
        elif join_cfg.get("enabled") and join_cfg.get("id"):
            sink["source_id"] = join_cfg["id"]
        else:
            # Fall back to the unique source_id from the top-level schema fields
            source_ids = list(
                dict.fromkeys(
                    f["source_id"] for f in schema_fields if f.get("source_id")
                )
            )
            if len(source_ids) == 1:
                sink["source_id"] = source_ids[0]

    return models.PipelineConfig.model_validate(config)
