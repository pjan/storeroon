"""
Configuration loader for storeroon.

Reads a TOML config file and provides typed access to all settings.
Search order (first match wins):
  1. Explicit path passed to ``load()``
  2. ``STOREROON_CONFIG`` environment variable
  3. ``storeroon.toml`` in the current working directory
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """Raised when the configuration file is missing, unreadable, or invalid."""


# ---------------------------------------------------------------------------
# Typed config sections
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CollectionConfig:
    root: Path

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> CollectionConfig:
        if "root" not in raw:
            raise ConfigError("[collection] section must contain a 'root' key")
        root = Path(raw["root"]).expanduser().resolve()
        return cls(root=root)


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    path: Path

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> DatabaseConfig:
        path_str = raw.get("path", "data/storeroon.db")
        return cls(path=Path(path_str).expanduser())


_DEFAULT_REQUIRED_TAGS: list[str] = [
    "TITLE",
    "ARTIST",
    "ALBUMARTIST",
    "ALBUM",
    "DATE",
    "TRACKNUMBER",
]


@dataclass(frozen=True, slots=True)
class ScanConfig:
    required_tags: tuple[str, ...]
    checksums: bool
    batch_size: int

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ScanConfig:
        required = raw.get("required_tags", _DEFAULT_REQUIRED_TAGS)
        # Normalise to upper-case so comparisons are always case-insensitive.
        required_upper = tuple(t.upper() for t in required)
        checksums = bool(raw.get("checksums", True))
        batch_size = int(raw.get("batch_size", 500))
        if batch_size < 1:
            raise ConfigError("[scan] batch_size must be >= 1")
        return cls(
            required_tags=required_upper,
            checksums=checksums,
            batch_size=batch_size,
        )


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> LoggingConfig:
        level = raw.get("level", "info").upper()
        valid = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if level not in valid:
            raise ConfigError(
                f"[logging] level must be one of {sorted(valid)}, got {level!r}"
            )
        return cls(level=level)


# ---------------------------------------------------------------------------
# Tag schema configuration (Sprint 2)
# ---------------------------------------------------------------------------

_DEFAULT_REQUIRED_TAG_FIELDS: list[str] = [
    "TITLE",
    "ARTIST",
    "ALBUM",
    "ALBUMARTIST",
    "ORIGINALDATE",
    "DATE",
    "CATALOGNUMBER",
    "TRACKNUMBER",
    "TRACKTOTAL",
    "DISCNUMBER",
    "DISCTOTAL",
    "RELEASETYPE",
    "RELEASESTATUS",
    "GENRE",
    "ACOUSTID_FINGERPRINT",
]

_DEFAULT_RECOMMENDED_TAG_FIELDS: list[str] = [
    "ISRC",
    "ACOUSTID_ID",
    "MUSICBRAINZ_TRACKID",
    "MUSICBRAINZ_RELEASETRACKID",
    "MUSICBRAINZ_ALBUMID",
    "MUSICBRAINZ_ARTISTID",
    "MUSICBRAINZ_ALBUMARTISTID",
    "MUSICBRAINZ_RELEASEGROUPID",
    "DISCOGS_RELEASE_ID",
    "DISCOGS_ARTIST_ID",
    "DISCOGS_MASTER_ID",
    "DISCOGS_LABEL_ID",
]

_DEFAULT_OTHER_TAG_FIELDS: list[str] = [
    "LYRICS",
    "REPLAYGAIN_TRACK_GAIN",
    "REPLAYGAIN_TRACK_PEAK",
    "REPLAYGAIN_ALBUM_GAIN",
    "REPLAYGAIN_ALBUM_PEAK",
    "MEDIA",
    "BARCODE",
    "COMMENT",
]

_DEFAULT_ALIASES: dict[str, str] = {
    "YEAR": "DATE",
    "ORIGINALYEAR": "ORIGINALDATE",
    "ALBUM ARTIST": "ALBUMARTIST",
    "TOTALTRACKS": "TRACKTOTAL",
    "TOTALDISCS": "DISCTOTAL",
    "DISCS": "DISCTOTAL",
}

_DEFAULT_STANDARD_OPTIONAL_FIELDS: list[str] = [
    "COMPOSER",
    "LYRICIST",
    "CONDUCTOR",
    "PERFORMER",
    "ENSEMBLE",
    "OPUS",
    "PART",
    "MOVEMENT",
    "WORK",
    "SUBTITLE",
    "GROUPING",
    "MOOD",
    "BPM",
    "KEY",
    "LANGUAGE",
    "SCRIPT",
    "REPLAYGAIN_REFERENCE_LOUDNESS",
    "ACCURATERIPCRC",
    "ACCURATERIPCOUNT",
    "ACCURATERIPRESULT",
    "ACCURATERIPDISCID",
    "ENCODEDBY",
    "ENCODING",
    "ENCODERSETTINGS",
    "SOURCE",
    "SOURCEMEDIA",
]


@dataclass(frozen=True, slots=True)
class TagsConfig:
    """Canonical tag schema configuration.

    All field lists are upper-cased tuples for consistent lookup.
    ``aliases`` maps non-canonical upper-cased keys to their canonical
    replacement (also upper-cased).
    """

    required: tuple[str, ...]
    recommended: tuple[str, ...]
    other: tuple[str, ...]
    aliases: dict[str, str]
    standard_optional: tuple[str, ...]
    strip: tuple[str, ...]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TagsConfig:
        """Build from the ``[tags]`` / ``[tags.*]`` section of the TOML."""

        def _fields(
            section: dict[str, Any],
            key: str = "fields",
            default: list[str] | None = None,
        ) -> tuple[str, ...]:
            values = section.get(key, default or [])
            return tuple(v.upper() for v in values)

        req_section = raw.get("required", {})
        rec_section = raw.get("recommended", {})
        other_section = raw.get("other", {})
        alias_section = raw.get("aliases", {})
        stdopt_section = raw.get("standard_optional", {})
        strip_section = raw.get("strip", {})

        # Aliases: keys and values are both upper-cased.
        aliases: dict[str, str] = {}
        source_aliases = alias_section if alias_section else _DEFAULT_ALIASES
        for alias_key, canonical_val in source_aliases.items():
            aliases[alias_key.upper()] = canonical_val.upper()

        return cls(
            required=_fields(req_section, default=_DEFAULT_REQUIRED_TAG_FIELDS),
            recommended=_fields(rec_section, default=_DEFAULT_RECOMMENDED_TAG_FIELDS),
            other=_fields(other_section, default=_DEFAULT_OTHER_TAG_FIELDS),
            aliases=aliases,
            standard_optional=_fields(
                stdopt_section, default=_DEFAULT_STANDARD_OPTIONAL_FIELDS
            ),
            strip=_fields(strip_section, default=[]),
        )

    def all_known_keys(self) -> frozenset[str]:
        """Return the set of all tag keys that appear in any config list."""
        keys: set[str] = set()
        keys.update(self.required)
        keys.update(self.recommended)
        keys.update(self.other)
        keys.update(self.aliases.keys())
        keys.update(self.aliases.values())
        keys.update(self.standard_optional)
        keys.update(self.strip)
        return frozenset(keys)

    def classify(self, key_upper: str) -> str:
        """Classify an upper-cased tag key against the config lists.

        Returns one of: ``'required'``, ``'recommended'``, ``'other'``,
        ``'alias'``, ``'standard_optional'``, ``'strip'``, or ``'unknown'``.
        """
        if key_upper in self.required:
            return "required"
        if key_upper in self.recommended:
            return "recommended"
        if key_upper in self.other:
            return "other"
        if key_upper in self.aliases:
            return "alias"
        if key_upper in self.standard_optional:
            return "standard_optional"
        if key_upper in self.strip:
            return "strip"
        return "unknown"


# ---------------------------------------------------------------------------
# Reports configuration (Sprint 2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReportsConfig:
    """Configuration for Sprint 2 analysis reports."""

    fuzzy_threshold: float
    output_dir: Path

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ReportsConfig:
        threshold = float(raw.get("fuzzy_threshold", 0.85))
        if not (0.0 <= threshold <= 1.0):
            raise ConfigError(
                "[reports] fuzzy_threshold must be between 0.0 and 1.0, "
                f"got {threshold}"
            )
        output_dir = Path(raw.get("output_dir", "reports"))
        return cls(fuzzy_threshold=threshold, output_dir=output_dir)


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Config:
    """Fully-parsed, immutable configuration."""

    config_path: Path
    collection: CollectionConfig
    database: DatabaseConfig
    scan: ScanConfig
    logging: LoggingConfig
    tags: TagsConfig
    reports: ReportsConfig


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

_SEARCH_PATHS: tuple[str, ...] = ("storeroon.toml",)


def _resolve_config_path(explicit: str | Path | None = None) -> Path:
    """Return the first config file that exists, or raise ``ConfigError``."""
    if explicit is not None:
        p = Path(explicit).expanduser().resolve()
        if not p.is_file():
            raise ConfigError(f"Config file not found: {p}")
        return p

    env = os.environ.get("STOREROON_CONFIG")
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_file():
            raise ConfigError(f"STOREROON_CONFIG points to a missing file: {p}")
        return p

    for candidate in _SEARCH_PATHS:
        p = Path(candidate).resolve()
        if p.is_file():
            return p

    raise ConfigError(
        "No config file found. Create storeroon.toml in the working directory, "
        "set the STOREROON_CONFIG environment variable, or pass an explicit path."
    )


def load(path: str | Path | None = None) -> Config:
    """Load and validate the configuration file.

    Parameters
    ----------
    path:
        Explicit path to a TOML config file.  When *None*, the loader falls
        back to ``$STOREROON_CONFIG`` and then ``storeroon.toml`` in the cwd.
    """
    config_path = _resolve_config_path(path)

    try:
        text = config_path.read_bytes()
    except OSError as exc:
        raise ConfigError(f"Cannot read config file {config_path}: {exc}") from exc

    try:
        raw = tomllib.loads(text.decode())
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {config_path}: {exc}") from exc

    return Config(
        config_path=config_path,
        collection=CollectionConfig.from_dict(raw.get("collection", {})),
        database=DatabaseConfig.from_dict(raw.get("database", {})),
        scan=ScanConfig.from_dict(raw.get("scan", {})),
        logging=LoggingConfig.from_dict(raw.get("logging", {})),
        tags=TagsConfig.from_dict(raw.get("tags", {})),
        reports=ReportsConfig.from_dict(raw.get("reports", {})),
    )
