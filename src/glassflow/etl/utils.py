import copy
from typing import Any

from . import models


def migrate_pipeline_v2_to_v3(pipeline: dict[str, Any]) -> models.PipelineConfig:
    """Migrate a pipeline configuration from v2 to the new v3 format.

    Changes applied:
    - version: "v2" -> "v3"
    - source.topics[] -> flat sources[] list (each with own connection_params + topic)
    - top-level schema.fields -> per-source schema_fields + sink.mapping
    - topic deduplication -> transforms[] entries (type: dedup)
    - filter -> transforms[] entry (type: filter)
    - stateless_transformation -> transforms[] entry (type: stateless)
    - join: sources[] with orientation -> left_source/right_source,
      fields -> output_fields
    - sink: flat connection fields -> nested connection_params
    - sink: source_id derived from transformation or source
    - pipeline_resources -> resources, ingestor -> sources, transform -> list

    Args:
        pipeline: V2 pipeline configuration as a plain dict.

    Returns:
        PipelineConfig: Validated V3 pipeline configuration.
    """
    config = copy.deepcopy(pipeline)
    config["version"] = "v3"

    # --- top-level schema -> per-source schema_fields + sink.mapping -------
    schema_fields = (config.pop("schema", None) or {}).get("fields", [])

    # Group source fields by topic name (source_id)
    fields_by_topic: dict[str, list[dict]] = {}
    for field in schema_fields:
        source_id = field.get("source_id", "")
        fields_by_topic.setdefault(source_id, []).append(field)

    # --- flatten source.topics -> sources list -----------------------------
    old_source = config.pop("source", {})
    source_type = old_source.get("type", "kafka")
    connection_params = old_source.get("connection_params", {})
    topics = old_source.get("topics", [])

    sources = []
    transforms = []

    for topic in topics:
        source_id = topic.get("id") or topic.get("name")
        topic_name = topic.get("name")

        src_entry: dict[str, Any] = {
            "type": source_type,
            "source_id": source_id,
            "connection_params": copy.deepcopy(connection_params),
            "topic": topic_name,
        }

        if topic.get("consumer_group_initial_offset"):
            src_entry["consumer_group_initial_offset"] = topic[
                "consumer_group_initial_offset"
            ]

        # Attach schema_fields from old top-level schema
        topic_fields = fields_by_topic.get(topic_name, [])
        if topic_fields:
            src_entry["schema_fields"] = [
                {"name": f["name"], "type": f["type"]} for f in topic_fields
            ]
        elif topic.get("schema_fields"):
            src_entry["schema_fields"] = topic["schema_fields"]

        sources.append(src_entry)

        # Convert topic-level deduplication to a dedup transform entry
        dedup = topic.get("deduplication")
        if isinstance(dedup, dict) and dedup.get("enabled"):
            key = dedup.get("key") or dedup.get("id_field")
            time_window = dedup.get("time_window")
            if key and time_window:
                transforms.append(
                    {
                        "type": "dedup",
                        "source_id": source_id,
                        "config": {"key": key, "time_window": time_window},
                    }
                )

    # Handle OTLP sources (no topics)
    if not topics and source_type.startswith("otlp"):
        sources.append(
            {
                "type": source_type,
                "source_id": old_source.get("id", "otlp-src"),
            }
        )

    config["sources"] = sources

    # --- filter -> transform entry -----------------------------------------
    old_filter = config.pop("filter", None) or {}
    if old_filter.get("enabled") and old_filter.get("expression"):
        # Use the first source_id as the filter target
        filter_source_id = sources[0]["source_id"] if sources else "unknown"
        transforms.append(
            {
                "type": "filter",
                "source_id": filter_source_id,
                "config": {"expression": old_filter["expression"]},
            }
        )

    # --- stateless_transformation -> transform entry -----------------------
    old_st = config.pop("stateless_transformation", None) or {}
    if old_st.get("enabled") and old_st.get("config"):
        st_source_id = old_st.get("source_id") or (
            sources[0]["source_id"] if sources else "unknown"
        )
        old_transform_config = old_st["config"]
        # Rename 'transform' key to 'transforms' if needed
        st_config = {}
        if "transform" in old_transform_config:
            st_config["transforms"] = old_transform_config["transform"]
        elif "transforms" in old_transform_config:
            st_config["transforms"] = old_transform_config["transforms"]
        transforms.append(
            {
                "type": "stateless",
                "source_id": st_source_id,
                "config": st_config,
            }
        )

    config["transforms"] = transforms if transforms else None

    # --- join: sources with orientation -> left_source/right_source ---------
    old_join = config.get("join") or {}
    if old_join.get("enabled"):
        old_join_sources = old_join.pop("sources", []) or []
        left_src = None
        right_src = None
        for src in old_join_sources:
            key = src.get("key") or src.get("join_key")
            src.pop("join_key", None)
            src.pop("join_key_type", None)
            entry = {
                "source_id": src["source_id"],
                "key": key,
                "time_window": src.get("time_window", "1h"),
            }
            if src.get("orientation", "").lower() == "left":
                left_src = entry
            else:
                right_src = entry

        old_join["left_source"] = left_src
        old_join["right_source"] = right_src
        old_join.pop("sources", None)

        # Rename fields -> output_fields
        old_fields = old_join.pop("fields", None)
        if old_fields:
            old_join["output_fields"] = old_fields
        else:
            # Build output_fields from schema fields belonging to join sources
            join_source_ids = set()
            if left_src:
                join_source_ids.add(left_src["source_id"])
            if right_src:
                join_source_ids.add(right_src["source_id"])
            old_join["output_fields"] = [
                {"source_id": f["source_id"], "name": f["name"]}
                for f in schema_fields
                if f.get("source_id") in join_source_ids
            ]
    else:
        config.pop("join", None)

    # --- sink: flat connection fields -> connection_params ------------------
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
    conn_fields = {k: sink.pop(k) for k in _conn_keys if k in sink}
    if conn_fields:
        sink["connection_params"] = conn_fields

    # Remove provider from sink
    sink.pop("provider", None)

    # --- sink: derive source_id -------------------------------------------
    if "source_id" not in sink:
        if old_st.get("enabled") and old_st.get("id"):
            sink["source_id"] = old_st["id"]
        else:
            # Fall back to the unique source_id from the top-level schema fields
            source_id_set = list(
                dict.fromkeys(
                    f["source_id"] for f in schema_fields if f.get("source_id")
                )
            )
            if len(source_id_set) == 1:
                sink["source_id"] = source_id_set[0]

    # --- sink.mapping from top-level schema --------------------------------
    if "mapping" not in sink and schema_fields:
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
            sink["mapping"] = sink_mapping

    # --- pipeline_resources -> resources -----------------------------------
    old_resources = config.pop("pipeline_resources", None)
    if old_resources:
        new_resources: dict[str, Any] = {}
        if "nats" in old_resources:
            new_resources["nats"] = old_resources["nats"]
        if "sink" in old_resources:
            new_resources["sink"] = old_resources["sink"]
        if "ingestor" in old_resources:
            # Convert ingestor.base to sources list
            ingestor = old_resources["ingestor"]
            base = ingestor.get("base", {})
            if base and sources:
                new_resources["sources"] = [
                    {"source_id": s["source_id"], **base} for s in sources
                ]
        if "transform" in old_resources:
            old_transform = old_resources["transform"]
            if sources:
                new_resources["transform"] = [
                    {"source_id": sources[0]["source_id"], **old_transform}
                ]
        config["resources"] = new_resources

    # Remove exported_at/exported_by if present
    config.pop("exported_at", None)
    config.pop("exported_by", None)

    return models.PipelineConfig.model_validate(config)
