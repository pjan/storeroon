"""
Idempotent migration runner for storeroon.

Reads SQL migration files and applies them to the database.  The Phase 1
schema uses ``IF NOT EXISTS`` throughout, so re-running is safe.

A ``schema_version`` table tracks which migrations have been applied.
Each migration is identified by its filename and a SHA-256 hash of its
contents, so accidental edits to already-applied migrations are detected.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from importlib import resources

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal bookkeeping table
# ---------------------------------------------------------------------------

_VERSION_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS schema_version (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT    NOT NULL UNIQUE,
    checksum    TEXT    NOT NULL,
    applied_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now'))
);
"""

# Migrations are applied in this order.  Add new filenames here as new
# phases introduce additional schema files.
_MIGRATION_FILES: tuple[str, ...] = ("schema.sql",)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def migrate(conn: sqlite3.Connection) -> list[str]:
    """Apply all pending migrations and return a list of filenames that were
    applied during this call.

    The function is idempotent: migrations whose filename already appears in
    ``schema_version`` are skipped (with a checksum consistency check).

    Parameters
    ----------
    conn:
        An open SQLite connection, ideally obtained via
        :func:`storeroon.db.connection.connect` so that WAL mode and
        foreign-key enforcement are already active.

    Returns
    -------
    list[str]
        Filenames of migrations that were freshly applied.

    Raises
    ------
    MigrationError
        If a previously-applied migration has been modified on disk (checksum
        mismatch), or if a migration file cannot be found / read.
    """
    _ensure_version_table(conn)

    applied: list[str] = []

    for filename in _MIGRATION_FILES:
        sql, checksum = _read_migration(filename)

        existing = conn.execute(
            "SELECT checksum FROM schema_version WHERE filename = ?",
            (filename,),
        ).fetchone()

        if existing is not None:
            stored_checksum = existing[0]
            if stored_checksum != checksum:
                raise MigrationError(
                    f"Migration {filename!r} was already applied with checksum "
                    f"{stored_checksum!r}, but the file on disk now has checksum "
                    f"{checksum!r}.  If this is intentional (e.g. during early "
                    f"development), delete the database and re-run."
                )
            log.debug("Migration %s already applied — skipping", filename)
            continue

        log.info("Applying migration: %s", filename)
        conn.executescript(sql)

        conn.execute(
            "INSERT INTO schema_version (filename, checksum) VALUES (?, ?)",
            (filename, checksum),
        )
        conn.commit()

        applied.append(filename)
        log.info("Migration %s applied successfully", filename)

    return applied


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MigrationError(Exception):
    """Raised when a migration cannot be applied or a checksum mismatch is
    detected."""


def _ensure_version_table(conn: sqlite3.Connection) -> None:
    """Create the ``schema_version`` bookkeeping table if it doesn't exist."""
    conn.executescript(_VERSION_TABLE_DDL)


def _read_migration(filename: str) -> tuple[str, str]:
    """Read a migration file bundled inside the ``storeroon.db`` package.

    Returns
    -------
    tuple[str, str]
        ``(sql_text, sha256_hex)``
    """
    # importlib.resources works regardless of how the package is installed
    # (editable, zip, wheel, etc.).
    try:
        ref = resources.files("storeroon.db").joinpath(filename)
        sql = ref.read_text(encoding="utf-8")
    except (FileNotFoundError, TypeError, ModuleNotFoundError) as exc:
        raise MigrationError(f"Cannot read migration file {filename!r}: {exc}") from exc

    checksum = hashlib.sha256(sql.encode()).hexdigest()
    return sql, checksum
