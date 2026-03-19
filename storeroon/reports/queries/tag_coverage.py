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

# Alias consistency: for files that have both the canonical and alias key,
# check whether values match.  Returns one row per file with both keys.
_ALIAS_BOTH_PRESENT_SQL = """
SELECT
    rt_canon.file_id,
    rt_canon.tag_value AS canonical_value,
    rt_alias.tag_value AS alias_value
FROM raw_tags rt_canon
JOIN raw_tags rt_alias
    ON rt_alias.file_id = rt_canon.file_id
    AND rt_alias.tag_key_upper = ?
    AND rt_alias.tag_index = 0
WHERE rt_canon.tag_key_upper = ?
  AND rt_canon.tag_index = 0
  AND rt_canon.file_id IN (SELECT id FROM files WHERE status = 'ok')
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
    """Compute coverage for a group of tag keys, sorted by coverage % descending (highest first)."""
    rows = [_coverage_for_key(conn, key, total_files) for key in keys]
    rows.sort(key=lambda r: r.present_pct, reverse=True)
    return rows


def _alias_usage(
    conn: sqlite3.Connection,
    aliases: dict[str, str],
    canonical_keys: frozenset[str],
) -> list[AliasUsageRow]:
    """Check value consistency for alias pairs where the canonical key
    is in the required or recommended config lists.

    For each qualifying alias pair, counts files where both keys are
    present and whether their values match.
    """
    result: list[AliasUsageRow] = []
    for alias_key, canonical_key in sorted(aliases.items()):
        # Only include pairs where the canonical target is required or recommended.
        if canonical_key not in canonical_keys:
            continue

        rows = conn.execute(
            _ALIAS_BOTH_PRESENT_SQL, (alias_key, canonical_key)
        ).fetchall()

        files_with_both = len(rows)
        files_matching = sum(
            1 for r in rows
            if r["canonical_value"].strip() == r["alias_value"].strip()
        )
        files_mismatched = files_with_both - files_matching

        if files_with_both > 0:
            result.append(
                AliasUsageRow(
                    canonical_key=canonical_key,
                    alias_key=alias_key,
                    files_with_both=files_with_both,
                    files_matching=files_matching,
                    files_mismatched=files_mismatched,
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
    canonical_keys = frozenset(tags_config.required + tags_config.recommended)
    alias_usage = _alias_usage(conn, tags_config.aliases, canonical_keys)
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
