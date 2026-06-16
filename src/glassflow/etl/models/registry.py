"""Extensible registry for source types.

Source types (``kafka``, ``otlp.logs``, ...) are open-ended: an edition can add
new ones without modifying this package. Instead of a static Pydantic
discriminated union (whose members are frozen at class-definition time), models
annotate their field with the base type and dispatch each value to the concrete
class by its ``type`` string at validation time, looked up here.

Editions register their classes at import::

    from glassflow.etl.models.registry import register_source
    register_source(KinesisSource)
"""

from __future__ import annotations

from typing import Any, Dict, Type, TypeVar

from pydantic import BaseModel

_T = TypeVar("_T", bound=BaseModel)

_SOURCE_CLASSES: Dict[str, Type[BaseModel]] = {}


def _type_key(cls: Type[BaseModel]) -> str:
    """Derive the discriminator key from a model's ``type`` field default."""
    field = cls.model_fields.get("type")
    if field is None or field.default is None:
        raise ValueError(
            f"{cls.__name__} must define a 'type' field with a literal default "
            "to be registered"
        )
    return str(field.default)


def register_source(cls: Type[_T]) -> Type[_T]:
    """Register a source class, keyed by its ``type`` default. Usable as a
    decorator. Idempotent for re-imports."""
    _SOURCE_CLASSES[_type_key(cls)] = cls
    return cls


def _resolve(value: Any, registry: Dict[str, Type[BaseModel]], kind: str) -> Any:
    """Coerce a raw dict to the registered concrete model by its ``type``.

    Already-constructed model instances and non-dict values pass through
    untouched so Pydantic can validate (or reject) them normally.
    """
    if isinstance(value, BaseModel) or not isinstance(value, dict):
        return value
    type_value = value.get("type")
    cls = registry.get(str(type_value)) if type_value is not None else None
    if cls is None:
        known = ", ".join(sorted(registry)) or "(none registered)"
        raise ValueError(f"Unknown {kind} type {type_value!r}. Known types: {known}")
    return cls.model_validate(value)


def resolve_source(value: Any) -> Any:
    return _resolve(value, _SOURCE_CLASSES, "source")
