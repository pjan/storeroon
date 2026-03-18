"""
Duplicate checksum detector for storeroon.

After import, queries the ``files`` table for rows sharing the same
``checksum_sha256`` and raises ``duplicate_checksum`` scan issues for each
group of duplicates found.

Only files with status ``'ok'`` and a non-NULL checksum are considered.
Already-resolved duplicate issues are not re-raised.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(slots=True)
class DuplicateStats:
    """Summary statistics from a duplicate-detection run."""

    groups_found: int = 0
    files_affected: int = 0
    issues_raised: int = 0


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Find checksum values that appear on more than one file.
_FIND_DUPLICATE_CHECKSUMS = """\
SELECT checksum_sha256, COUNT(*) AS cnt
FROM files
WHERE checksum_sha256 IS NOT NULL
  AND status = 'ok'
GROUP BY checksum_sha256
HAVING cnt > 1
ORDER BY cnt DESC
"""

# Fetch all files sharing a given checksum.
_FILES_FOR_CHECKSUM = """\
SELECT id, path, filename, size_bytes
FROM files
WHERE checksum_sha256 = ?
  AND status = 'ok'
ORDER BY path
"""

# Check whether an unresolved duplicate_checksum issue already exists for a
# specific file.  We avoid raising the same issue twice on repeated scans.
_EXISTING_ISSUE = """\
SELECT 1
FROM scan_issues
WHERE file_id = ?
  AND issue_type = 'duplicate_checksum'
  AND resolved = 0
LIMIT 1
"""

_INSERT_ISSUE = """\
INSERT INTO scan_issues
    (file_id, issue_type, severity, description, details)
VALUES (?, 'duplicate_checksum', 'warning', ?, ?)
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_duplicates(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> DuplicateStats:
    """Scan for files that share a SHA-256 checksum and raise scan issues.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    dry_run:
        When *True*, report what would be flagged without writing to the DB.

    Returns
    -------
    DuplicateStats
        Summary of how many duplicate groups, files, and issues were found.
    """
    stats = DuplicateStats()

    dup_rows = conn.execute(_FIND_DUPLICATE_CHECKSUMS).fetchall()

    for dup_row in dup_rows:
        checksum = dup_row[0]
        count = dup_row[1]
        stats.groups_found += 1

        file_rows = conn.execute(_FILES_FOR_CHECKSUM, (checksum,)).fetchall()
        stats.files_affected += len(file_rows)

        # Build a list of all paths in this duplicate group for the details
        # JSON payload — every issue in the group gets the full picture.
        group_paths = [row[1] for row in file_rows]

        for file_row in file_rows:
            file_id = file_row[0]
            file_path = file_row[1]

            # Skip if an unresolved duplicate issue already exists.
            existing = conn.execute(_EXISTING_ISSUE, (file_id,)).fetchone()
            if existing is not None:
                log.debug(
                    "Duplicate issue already exists for file %s — skipping",
                    file_path,
                )
                continue

            # Build the other paths (all duplicates except the current file).
            other_paths = [p for p in group_paths if p != file_path]

            description = (
                f"File shares SHA-256 checksum with {len(other_paths)} "
                f"other file{'s' if len(other_paths) != 1 else ''}"
            )

            details = json.dumps(
                {
                    "checksum_sha256": checksum,
                    "duplicate_count": count,
                    "other_paths": other_paths,
                },
                ensure_ascii=False,
            )

            if dry_run:
                log.info(
                    "[DRY RUN] Would raise duplicate_checksum issue for %s "
                    "(%d duplicates)",
                    file_path,
                    count,
                )
            else:
                conn.execute(_INSERT_ISSUE, (file_id, description, details))
                log.debug("Raised duplicate_checksum issue for %s", file_path)

            stats.issues_raised += 1

    if not dry_run and stats.issues_raised > 0:
        conn.commit()

    if stats.groups_found > 0:
        log.info(
            "Duplicate detection complete: %d group%s, %d file%s affected, "
            "%d issue%s %s",
            stats.groups_found,
            "s" if stats.groups_found != 1 else "",
            stats.files_affected,
            "s" if stats.files_affected != 1 else "",
            stats.issues_raised,
            "s" if stats.issues_raised != 1 else "",
            "would be raised" if dry_run else "raised",
        )
    else:
        log.info("Duplicate detection complete: no duplicates found")

    return stats
