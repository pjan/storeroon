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
    )
