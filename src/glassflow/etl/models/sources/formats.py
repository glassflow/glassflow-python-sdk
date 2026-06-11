"""Source format models.

A source's ``format`` describes how its payload is encoded and how the schema
is supplied. It is a tagged object discriminated by ``type`` (``json`` in OSS;
``avro``/``protobuf`` in Enterprise), with format-specific configuration nested
under a key named after the type, e.g.::

    {"type": "protobuf", "protobuf": {"schema_source": "inline", "proto_text": "..."}}

New formats are added by subclassing :class:`SourceFormat` and registering them
with :func:`glassflow.etl.models.registry.register_format`; no change to this
package is required.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from ..registry import register_format


class SourceFormat(BaseModel):
    """Base class for all source formats."""

    type: str

    def validate_against_registry(self, has_schema_registry: bool) -> None:
        """Hook for formats to enforce schema-source rules that depend on the
        parent source's schema registry configuration.

        Called by the source's after-validator with whether the source has a
        schema registry configured. The base implementation is a no-op;
        formats that can read their schema from a registry (e.g. Enterprise
        avro/protobuf) override this to require that the registry is present
        when ``schema_source`` is ``"registry"``.
        """
        return None


@register_format
class JsonFormat(SourceFormat):
    """JSON-encoded payloads. This is the default and carries no extra config."""

    type: Literal["json"] = "json"
