"""
storeroon.reports.queries.duplicates — Report 8: Duplicates.

Three independent passes:
    Pass 1 — Exact: files sharing the same SHA-256 checksum.
    Pass 2 — Same recording (MBID): files sharing the same MUSICBRAINZ_TRACKID.
    Pass 3 — Probable: files sharing (ALBUMARTIST, ALBUM, DISCNUMBER, TRACKNUMBER)
              but with different checksums. Excludes pairs already caught by Pass 1.

Public API:
    full_data(conn) -> DuplicatesFullData
    summary_data(conn) -> DuplicatesSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    DuplicatesFullData,
    DuplicatesSummaryData,
    ExactDuplicateGroup,
    MbidDuplicateFile,
    MbidDuplicateGroup,
    ProbableDuplicateGroup,
)

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Pass 1 — Exact duplicates by SHA-256 checksum.
_EXACT_DUPLICATES_SQL = """
SELECT checksum_sha256, COUNT(*) AS copy_count
FROM files
WHERE checksum_sha256 IS NOT NULL
  AND status = 'ok'
GROUP BY checksum_sha256
HAVING COUNT(*) > 1
ORDER BY copy_count DESC
"""

_FILES_FOR_CHECKSUM_SQL = """
SELECT path
FROM files
WHERE checksum_sha256 = ?
  AND status = 'ok'
ORDER BY path
"""

# Pass 2 — Same recording by MUSICBRAINZ_TRACKID.
_MBID_DUPLICATES_SQL = """
SELECT rt.tag_value AS mbid, COUNT(DISTINCT rt.file_id) AS file_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'MUSICBRAINZ_TRACKID'
  AND rt.tag_index = 0
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
HAVING COUNT(DISTINCT rt.file_id) > 1
ORDER BY file_count DESC
"""

_FILES_FOR_MBID_SQL = """
SELECT
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir,
    MAX(CASE WHEN rt2.tag_key_upper = 'ALBUM' THEN rt2.tag_value END) AS album,
    MAX(CASE WHEN rt2.tag_key_upper = 'DATE'  THEN rt2.tag_value END) AS date
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
LEFT JOIN raw_tags rt2 ON rt2.file_id = f.id AND rt2.tag_index = 0
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'MUSICBRAINZ_TRACKID'
  AND rt.tag_value = ?
  AND rt.tag_index = 0
GROUP BY f.id
ORDER BY f.path
"""

# Pass 3 — Probable duplicates by (ALBUMARTIST, ALBUM, DISCNUMBER, TRACKNUMBER).
# We use a pivot approach to collect the four tag values per file, then group.
_PROBABLE_PIVOT_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    f.checksum_sha256,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUMARTIST'  THEN TRIM(LOWER(rt.tag_value)) END) AS albumartist,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUM'        THEN TRIM(LOWER(rt.tag_value)) END) AS album,
    MAX(CASE WHEN rt.tag_key_upper = 'DISCNUMBER'   THEN TRIM(rt.tag_value) END) AS discnumber,
    MAX(CASE WHEN rt.tag_key_upper = 'TRACKNUMBER'  THEN TRIM(rt.tag_value) END) AS tracknumber,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUMARTIST'  THEN rt.tag_value END) AS albumartist_raw,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUM'        THEN rt.tag_value END) AS album_raw
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id AND rt.tag_index = 0
WHERE f.status = 'ok'
  AND rt.tag_key_upper IN ('ALBUMARTIST', 'ALBUM', 'DISCNUMBER', 'TRACKNUMBER')
GROUP BY f.id
HAVING albumartist IS NOT NULL
   AND album IS NOT NULL
   AND tracknumber IS NOT NULL
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_exact_duplicates(conn: sqlite3.Connection) -> list[ExactDuplicateGroup]:
    """Pass 1: find groups of files sharing the same SHA-256 checksum."""
    dup_rows = conn.execute(_EXACT_DUPLICATES_SQL).fetchall()
    groups: list[ExactDuplicateGroup] = []
    for dr in dup_rows:
        checksum = dr["checksum_sha256"]
        copy_count = dr["copy_count"]
        file_rows = conn.execute(_FILES_FOR_CHECKSUM_SQL, (checksum,)).fetchall()
        paths = [r["path"] for r in file_rows]
        groups.append(
            ExactDuplicateGroup(
                checksum=checksum,
                copy_count=copy_count,
                paths=paths,
            )
        )
    return groups


def _find_mbid_duplicates(conn: sqlite3.Connection) -> list[MbidDuplicateGroup]:
    """Pass 2: find files sharing the same MUSICBRAINZ_TRACKID."""
    dup_rows = conn.execute(_MBID_DUPLICATES_SQL).fetchall()
    groups: list[MbidDuplicateGroup] = []
    for dr in dup_rows:
        mbid = dr["mbid"]
        file_count = dr["file_count"]
        file_rows = conn.execute(_FILES_FOR_MBID_SQL, (mbid,)).fetchall()
        files: list[MbidDuplicateFile] = []
        dirs: set[str] = set()
        for fr in file_rows:
            files.append(
                MbidDuplicateFile(
                    path=fr["path"],
                    album=fr["album"] or "",
                    date=fr["date"] or "",
                )
            )
            dirs.add(fr["album_dir"])
        same_directory = len(dirs) == 1
        groups.append(
            MbidDuplicateGroup(
                mbid=mbid,
                file_count=file_count,
                files=files,
                same_directory=same_directory,
            )
        )
    return groups


def _find_probable_duplicates(
    conn: sqlite3.Connection,
    exact_checksum_groups: list[ExactDuplicateGroup],
) -> list[ProbableDuplicateGroup]:
    """Pass 3: find files sharing (ALBUMARTIST, ALBUM, DISCNUMBER, TRACKNUMBER)
    but with different checksums. Excludes pairs already caught by Pass 1."""
    # Build a set of (checksum) pairs that are exact duplicates so we can
    # exclude them. We track the set of checksums that appear in any exact
    # duplicate group — if ALL files in a probable group share the same
    # checksum, it's an exact duplicate and should be excluded.
    exact_checksums: set[str] = set()
    for g in exact_checksum_groups:
        exact_checksums.add(g.checksum)

    # Fetch the pivot data.
    rows = conn.execute(_PROBABLE_PIVOT_SQL).fetchall()

    # Group by (albumartist, album, discnumber, tracknumber).
    # discnumber defaults to "1" if absent for grouping purposes.
    groups: dict[tuple[str, str, str, str], list[dict[str, str | None]]] = defaultdict(
        list
    )

    for r in rows:
        albumartist = r["albumartist"] or ""
        album = r["album"] or ""
        discnumber = r["discnumber"] or "1"
        tracknumber = r["tracknumber"] or ""

        if not tracknumber:
            continue

        key = (albumartist, album, discnumber, tracknumber)
        groups[key].append(
            {
                "path": r["path"],
                "checksum": r["checksum_sha256"],
                "albumartist_raw": r["albumartist_raw"],
                "album_raw": r["album_raw"],
            }
        )

    results: list[ProbableDuplicateGroup] = []
    for key, file_entries in sorted(groups.items()):
        if len(file_entries) < 2:
            continue

        # Check if all files have the same checksum — if so, this is
        # already caught by Pass 1.
        checksums = {e["checksum"] for e in file_entries if e["checksum"]}
        if len(checksums) <= 1 and checksums.issubset(exact_checksums):
            continue

        # Also skip if all files share a single checksum even if not in
        # exact_checksums (shouldn't happen, but be safe).
        if len(checksums) <= 1:
            # All same checksum but not in the exact set — still effectively
            # exact duplicates, skip.
            continue

        paths = [str(e["path"] or "") for e in file_entries]
        checksum_list = [str(e["checksum"] or "") for e in file_entries]

        # Use the raw (non-normalised) values for display.
        albumartist_raw = file_entries[0]["albumartist_raw"] or key[0]
        album_raw = file_entries[0]["album_raw"] or key[1]

        results.append(
            ProbableDuplicateGroup(
                albumartist=albumartist_raw,
                album=album_raw,
                discnumber=key[2],
                tracknumber=key[3],
                file_count=len(file_entries),
                paths=paths,
                checksums=checksum_list,
            )
        )

    # Sort by albumartist, album, discnumber, tracknumber.
    results.sort(
        key=lambda g: (
            g.albumartist.lower(),
            g.album.lower(),
            g.discnumber,
            g.tracknumber,
        )
    )
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(conn: sqlite3.Connection) -> DuplicatesFullData:
    """Return the complete dataset for ``report duplicates``."""
    exact = _find_exact_duplicates(conn)
    mbid = _find_mbid_duplicates(conn)
    probable = _find_probable_duplicates(conn, exact)

    return DuplicatesFullData(
        exact=exact,
        mbid=mbid,
        probable=probable,
    )


def summary_data(conn: sqlite3.Connection) -> DuplicatesSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Count of each duplicate type. Any exact duplicates are displayed
    prominently.
    """
    exact = _find_exact_duplicates(conn)
    mbid = _find_mbid_duplicates(conn)
    probable = _find_probable_duplicates(conn, exact)

    return DuplicatesSummaryData(
        exact_count=len(exact),
        mbid_count=len(mbid),
        probable_count=len(probable),
    )
