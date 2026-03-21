"""
storeroon.reports.serialization — JSON round-trip for report data models.

Provides a generic ``from_dict`` function that reconstructs frozen dataclass
instances from plain dicts (as produced by ``dataclasses.asdict``), and a
registry mapping report names to their ``*FullData`` classes.
"""

from __future__ import annotations

import dataclasses
import types
import typing
from typing import Any, TypeVar, get_type_hints

from storeroon.reports.models import (
    AlbumConsistencyFullData,
    ArtistsFullData,
    DuplicatesFullData,
    GenresFullData,
    IssuesFullData,
    LyricsFullData,
    Overview2FullData,
    OverviewFullData,
    ReplayGainFullData,
    TagCoverageFullData,
    TagQualityFullData,
    TechnicalFullData,
)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Report name → FullData class registry
# ---------------------------------------------------------------------------

REPORT_DATA_CLASSES: dict[str, type] = {
    "overview": OverviewFullData,
    "overview2": Overview2FullData,
    "technical": TechnicalFullData,
    "tags": TagCoverageFullData,
    "tag_quality": TagQualityFullData,
    "album_consistency": AlbumConsistencyFullData,
    "duplicates": DuplicatesFullData,
    "issues": IssuesFullData,
    "artists": ArtistsFullData,
    "genres": GenresFullData,
    "lyrics": LyricsFullData,
    "replaygain": ReplayGainFullData,
}

# ---------------------------------------------------------------------------
# Generic dataclass deserializer
# ---------------------------------------------------------------------------


def from_dict(cls: type[T], raw: dict[str, Any]) -> T:
    """Reconstruct a (possibly nested) frozen dataclass from a plain dict.

    Handles:
    - Primitive fields (int, float, str, bool) — passed through
    - Nested dataclass fields — recursively reconstructed
    - ``list[DC]`` — each element reconstructed
    - ``dict[str, DC]`` / ``dict[str, list[DC]]`` — values reconstructed
    - ``DC | None`` — reconstructed if not None
    - ``str | None``, ``int | None``, etc. — passed through
    """
    if not dataclasses.is_dataclass(cls):
        raise TypeError(f"{cls} is not a dataclass")

    hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}

    for field_name, field_type in hints.items():
        if field_name not in raw:
            # Let the dataclass default handle it (e.g. default_factory).
            continue
        value = raw[field_name]
        kwargs[field_name] = _coerce(field_type, value)

    return cls(**kwargs)


def _coerce(tp: Any, value: Any) -> Any:
    """Coerce *value* to match the type annotation *tp*."""
    if value is None:
        return None

    origin = typing.get_origin(tp)

    # --- X | None  (Union / Optional) ---
    if origin is types.UnionType or origin is typing.Union:
        args = typing.get_args(tp)
        # Filter out NoneType.
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _coerce(non_none[0], value)
        # Multi-type union (shouldn't appear in these models).
        return value

    # --- list[X] ---
    if origin is list:
        (item_type,) = typing.get_args(tp)
        if dataclasses.is_dataclass(item_type):
            return [from_dict(item_type, item) for item in value]
        return list(value)

    # --- dict[K, V] ---
    if origin is dict:
        key_type, val_type = typing.get_args(tp)
        val_origin = typing.get_origin(val_type)

        # dict[str, list[DC]]
        if val_origin is list:
            (inner,) = typing.get_args(val_type)
            if dataclasses.is_dataclass(inner):
                return {k: [from_dict(inner, i) for i in v] for k, v in value.items()}
            return {k: list(v) for k, v in value.items()}

        # dict[str, DC]
        if dataclasses.is_dataclass(val_type):
            return {k: from_dict(val_type, v) for k, v in value.items()}

        # dict[str, int] / dict[str, str] / etc.
        return dict(value)

    # --- Nested dataclass ---
    if dataclasses.is_dataclass(tp) and isinstance(value, dict):
        return from_dict(tp, value)

    # --- Primitives (int, float, str, bool) ---
    return value
