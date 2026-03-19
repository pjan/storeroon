"""
storeroon.reports.queries.tag_coverage — Report 3: Tag coverage and key inventory.

Sections:
    A — Canonical tag coverage (required / recommended / other)
    B — Alias usage
    C — Full tag key inventory with classification

Public API:
    full_data(conn, tags_config) -> TagCoverageFullData
    summary_data(conn, tags_config) -> TagCoverageSummaryData
"""

from __future__ import annotations

import sqlite3

from storeroon.config import TagsConfig
from storeroon.reports.models import (
    AliasUsageRow,
    TagCoverageFullData,
    TagCoverageRow,
    TagCoverageSummaryData,
    TagInventoryRow,
)
from storeroon.reports.utils import safe_pct

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_TOTAL_OK_FILES_SQL = """
SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'
"""

# Count of distinct file_ids that have a given tag_key_upper with a
# non-empty (after TRIM) value.
_TAG_PRESENT_SQL = """
SELECT COUNT(DISTINCT file_id) AS cnt
FROM raw_tags
WHERE tag_key_upper = ?
  AND TRIM(tag_value) != ''
  AND file_id IN (SELECT id FROM files WHERE status = 'ok')
"""

# Full tag key inventory: all distinct tag_key_upper values with file counts.
_FULL_INVENTORY_SQL = """
SELECT
    tag_key_upper,
    COUNT(DISTINCT file_id) AS file_count
FROM raw_tags
WHERE file_id IN (SELECT id FROM files WHERE status = 'ok')
GROUP BY tag_key_upper
ORDER BY file_count DESC
"""

# Alias usage: count of files where the alias key is present but the
# canonical key is absent.
_ALIAS_USAGE_SQL = """
SELECT COUNT(DISTINCT rt_alias.file_id) AS cnt
FROM raw_tags rt_alias
WHERE rt_alias.tag_key_upper = ?
  AND rt_alias.file_id IN (SELECT id FROM files WHERE status = 'ok')
  AND rt_alias.file_id NOT IN (
      SELECT DISTINCT file_id
      FROM raw_tags
      WHERE tag_key_upper = ?
        AND file_id IN (SELECT id FROM files WHERE status = 'ok')
  )
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_total_ok_files(conn: sqlite3.Connection) -> int:
    row = conn.execute(_TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _coverage_for_key(
    conn: sqlite3.Connection,
    tag_key: str,
    total_files: int,
) -> TagCoverageRow:
    """Compute coverage stats for a single tag key.

    Present = non-empty value exists.  Missing = empty or absent.
    """
    row = conn.execute(_TAG_PRESENT_SQL, (tag_key,)).fetchone()
    present = row[0] if row else 0
    missing = total_files - present
    if missing < 0:
        missing = 0

    return TagCoverageRow(
        tag_key=tag_key,
        present_count=present,
        present_pct=safe_pct(present, total_files),
        missing_count=missing,
        missing_pct=safe_pct(missing, total_files),
    )


def _coverage_for_group(
    conn: sqlite3.Connection,
    keys: tuple[str, ...],
    total_files: int,
) -> list[TagCoverageRow]:
    """Compute coverage for a group of tag keys, sorted by missing % descending."""
    rows = [_coverage_for_key(conn, key, total_files) for key in keys]
    rows.sort(key=lambda r: r.missing_pct, reverse=True)
    return rows


def _alias_usage(
    conn: sqlite3.Connection,
    aliases: dict[str, str],
    total_files: int,
) -> list[AliasUsageRow]:
    """For each alias->canonical mapping, count files using the alias without
    the canonical key."""
    result: list[AliasUsageRow] = []
    for alias_key, canonical_key in sorted(aliases.items()):
        row = conn.execute(_ALIAS_USAGE_SQL, (alias_key, canonical_key)).fetchone()
        count = row[0] if row else 0
        result.append(
            AliasUsageRow(
                canonical_key=canonical_key,
                alias_key=alias_key,
                files_using_alias=count,
                files_using_alias_pct=safe_pct(count, total_files),
            )
        )
    return result


def _full_inventory(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
    total_files: int,
) -> tuple[list[TagInventoryRow], list[TagInventoryRow]]:
    """Build the full tag key inventory and the filtered unknown-keys list.

    Returns (full_inventory, unknown_keys).
    """
    rows = conn.execute(_FULL_INVENTORY_SQL).fetchall()
    full: list[TagInventoryRow] = []
    unknown: list[TagInventoryRow] = []

    for r in rows:
        key_upper = r["tag_key_upper"]
        file_count = r["file_count"]
        coverage_pct = safe_pct(file_count, total_files)
        classification = tags_config.classify(key_upper)

        entry = TagInventoryRow(
            tag_key_upper=key_upper,
            file_count=file_count,
            coverage_pct=coverage_pct,
            classification=classification,
        )
        full.append(entry)

        if classification == "unknown":
            unknown.append(entry)

    unknown.sort(key=lambda r: r.file_count, reverse=True)

    return full, unknown


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
) -> TagCoverageFullData:
    """Return the complete dataset for ``report tags``."""
    total_files = _get_total_ok_files(conn)

    required_coverage = _coverage_for_group(conn, tags_config.required, total_files)
    recommended_coverage = _coverage_for_group(
        conn, tags_config.recommended, total_files
    )
    other_coverage = _coverage_for_group(conn, tags_config.other, total_files)
    alias_usage = _alias_usage(conn, tags_config.aliases, total_files)
    full_inv, unknown_keys = _full_inventory(conn, tags_config, total_files)

    return TagCoverageFullData(
        total_files=total_files,
        required_coverage=required_coverage,
        recommended_coverage=recommended_coverage,
        other_coverage=other_coverage,
        alias_usage=alias_usage,
        full_inventory=full_inv,
        unknown_keys=unknown_keys,
    )


def summary_data(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
) -> TagCoverageSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    total_files = _get_total_ok_files(conn)

    required_coverage = _coverage_for_group(conn, tags_config.required, total_files)
    recommended_coverage = _coverage_for_group(
        conn, tags_config.recommended, total_files
    )

    required_with_missing = [r for r in required_coverage if r.missing_count > 0]
    recommended_high_missing = [
        r for r in recommended_coverage if r.missing_pct > 20.0
    ]

    _, unknown_keys = _full_inventory(conn, tags_config, total_files)

    return TagCoverageSummaryData(
        total_files=total_files,
        required_with_missing=required_with_missing,
        recommended_high_missing=recommended_high_missing,
        unknown_key_count=len(unknown_keys),
        top_unknown_keys=unknown_keys[:5],
    )
