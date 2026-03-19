"""
storeroon.reports.queries.ids — Report 7: External ID coverage and integrity.

Query ``raw_tags`` directly (not ``v_common_tags``) for all ID tag instances.

Two independent sections — MusicBrainz and Discogs — each with identical
sub-section structure:
    1. Coverage table
    2. Partial album coverage
    3. Duplicate IDs
    4. Quick-win backfill candidates

Public API:
    full_data(conn, artist_filter=None) -> IdsFullData
    summary_data(conn) -> IdsSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Callable

from storeroon.reports.models import (
    BackfillCandidate,
    DuplicateIdEntry,
    IdCoverageRow,
    IdSectionData,
    IdsFullData,
    IdsSummaryData,
    PartialAlbumCoverage,
)
from storeroon.reports.utils import is_valid_discogs_id, is_valid_uuid, safe_pct

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MB_KEYS: tuple[str, ...] = (
    "MUSICBRAINZ_TRACKID",
    "MUSICBRAINZ_RELEASETRACKID",
    "MUSICBRAINZ_ALBUMID",
    "MUSICBRAINZ_ARTISTID",
    "MUSICBRAINZ_ALBUMARTISTID",
    "MUSICBRAINZ_RELEASEGROUPID",
)

_DISCOGS_KEYS: tuple[str, ...] = (
    "DISCOGS_RELEASE_ID",
    "DISCOGS_ARTIST_ID",
    "DISCOGS_MASTER_ID",
    "DISCOGS_LABEL_ID",
)

# The album-level key used for partial-coverage and backfill checks.
_MB_ALBUM_KEY = "MUSICBRAINZ_ALBUMID"
_MB_BACKFILL_TARGET = "MUSICBRAINZ_RELEASEGROUPID"

_DISCOGS_ALBUM_KEY = "DISCOGS_RELEASE_ID"
_DISCOGS_BACKFILL_TARGET = "DISCOGS_MASTER_ID"

# The per-track key used for duplicate checking.
_MB_TRACK_KEY = "MUSICBRAINZ_TRACKID"

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_TOTAL_OK_FILES_SQL = """
SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'
"""

_TOTAL_OK_FILES_FILTERED_SQL = """
SELECT COUNT(DISTINCT f.id) AS cnt
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

# All (file_id, tag_value) pairs for a given tag key across ok files.
_TAG_VALUES_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
"""

_TAG_VALUES_FILTERED_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
  AND rt.file_id IN (
      SELECT DISTINCT rt2.file_id
      FROM raw_tags rt2
      WHERE rt2.tag_key_upper = 'ALBUMARTIST'
        AND LOWER(rt2.tag_value) LIKE '%' || LOWER(?) || '%'
  )
"""

# All ok file IDs with their album directory.
_FILES_WITH_ALBUM_DIR_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
WHERE f.status = 'ok'
"""

_FILES_WITH_ALBUM_DIR_FILTERED_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

# For duplicate MB track IDs: find tag values appearing on more than one file.
_DUPLICATE_TAG_VALUES_SQL = """
SELECT rt.tag_value, COUNT(DISTINCT rt.file_id) AS file_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
HAVING COUNT(DISTINCT rt.file_id) > 1
ORDER BY file_count DESC
"""

# Files for a given tag value.
_FILES_FOR_TAG_VALUE_SQL = """
SELECT f.id AS file_id, f.path,
       SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_value = ?
  AND rt.tag_index = 0
ORDER BY f.path
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_total_ok_files(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> int:
    """Return count of files with status='ok', optionally filtered."""
    if artist_filter:
        row = conn.execute(_TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
    else:
        row = conn.execute(_TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _build_file_tag_map(
    conn: sqlite3.Connection,
    tag_key: str,
    artist_filter: str | None = None,
) -> dict[int, str]:
    """Return a mapping of file_id -> tag_value for a given tag key.

    Only includes the first value (tag_index=0) per file.
    """
    if artist_filter:
        rows = conn.execute(
            _TAG_VALUES_FILTERED_SQL, (tag_key, artist_filter)
        ).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()
    return {r["file_id"]: r["tag_value"] for r in rows}


def _build_file_album_dir_map(
    conn: sqlite3.Connection,
    artist_filter: str | None = None,
) -> tuple[dict[int, str], dict[int, str]]:
    """Return (file_id -> album_dir, file_id -> path) mappings."""
    if artist_filter:
        rows = conn.execute(
            _FILES_WITH_ALBUM_DIR_FILTERED_SQL, (artist_filter,)
        ).fetchall()
    else:
        rows = conn.execute(_FILES_WITH_ALBUM_DIR_SQL).fetchall()
    dir_map: dict[int, str] = {}
    path_map: dict[int, str] = {}
    for r in rows:
        dir_map[r["file_id"]] = r["album_dir"]
        path_map[r["file_id"]] = r["path"]
    return dir_map, path_map


def _coverage_row(
    tag_key: str,
    tag_values: dict[int, str],
    total_files: int,
    validator: Callable[[str], bool],
) -> IdCoverageRow:
    """Compute coverage stats for a single ID tag key."""
    valid = 0
    malformed = 0
    for value in tag_values.values():
        v = value.strip()
        if not v:
            continue
        if validator(v):
            valid += 1
        else:
            malformed += 1

    # Files that have the tag at all (non-empty).
    files_with_tag = valid + malformed
    absent = total_files - files_with_tag
    if absent < 0:
        absent = 0

    return IdCoverageRow(
        tag_key=tag_key,
        valid_count=valid,
        valid_pct=safe_pct(valid, total_files),
        malformed_count=malformed,
        malformed_pct=safe_pct(malformed, total_files),
        absent_count=absent,
        absent_pct=safe_pct(absent, total_files),
    )


def _partial_album_coverage(
    album_key_values: dict[int, str],
    file_album_dirs: dict[int, str],
    all_file_ids: set[int],
    validator: Callable[[str], bool],
) -> list[PartialAlbumCoverage]:
    """Find albums where the album-level ID is present on some tracks but not all.

    Parameters
    ----------
    album_key_values:
        file_id -> tag_value for the album-level key.
    file_album_dirs:
        file_id -> album_dir for all ok files.
    all_file_ids:
        Set of all ok file IDs in scope.
    validator:
        Function to check if a value is valid.
    """
    # Group file IDs by album dir.
    album_files: dict[str, list[int]] = defaultdict(list)
    for fid in all_file_ids:
        adir = file_album_dirs.get(fid)
        if adir is not None:
            album_files[adir].append(fid)

    results: list[PartialAlbumCoverage] = []
    for album_dir, fids in sorted(album_files.items()):
        with_id = 0
        without_id = 0
        for fid in fids:
            val = album_key_values.get(fid)
            if val and val.strip() and validator(val.strip()):
                with_id += 1
            else:
                without_id += 1
        # Only flag if SOME tracks have it and SOME don't (partial).
        if with_id > 0 and without_id > 0:
            results.append(
                PartialAlbumCoverage(
                    album_dir=album_dir,
                    tracks_with_id=with_id,
                    tracks_without_id=without_id,
                    total_tracks=len(fids),
                )
            )

    # Sort by tracks_without_id descending (worst coverage first).
    results.sort(key=lambda r: r.tracks_without_id, reverse=True)
    return results


def _find_duplicate_ids(
    conn: sqlite3.Connection,
    tag_key: str,
) -> list[DuplicateIdEntry]:
    """Find ID values that appear on more than one file."""
    dup_rows = conn.execute(_DUPLICATE_TAG_VALUES_SQL, (tag_key,)).fetchall()
    results: list[DuplicateIdEntry] = []
    for dr in dup_rows:
        id_value = dr["tag_value"]
        file_count = dr["file_count"]
        file_rows = conn.execute(
            _FILES_FOR_TAG_VALUE_SQL, (tag_key, id_value)
        ).fetchall()
        paths = [r["path"] for r in file_rows]
        dirs = {r["album_dir"] for r in file_rows}
        same_directory = len(dirs) == 1
        results.append(
            DuplicateIdEntry(
                id_value=id_value,
                file_count=file_count,
                file_paths=paths,
                same_directory=same_directory,
            )
        )
    return results


def _backfill_candidates(
    source_values: dict[int, str],
    target_values: dict[int, str],
    source_validator: Callable[[str], bool],
    description: str,
) -> BackfillCandidate | None:
    """Count tracks where the source ID is present+valid but the target is absent.

    Parameters
    ----------
    source_values:
        file_id -> tag_value for the source key (e.g. MUSICBRAINZ_ALBUMID).
    target_values:
        file_id -> tag_value for the target key (e.g. MUSICBRAINZ_RELEASEGROUPID).
    source_validator:
        Validation function for the source key.
    description:
        Human-readable description for the backfill candidate entry.
    """
    affected_tracks = 0
    distinct_source_ids: set[str] = set()
    for fid, source_val in source_values.items():
        sv = source_val.strip()
        if not sv or not source_validator(sv):
            continue
        # Check if target is absent or empty.
        target_val = target_values.get(fid)
        if not target_val or not target_val.strip():
            affected_tracks += 1
            distinct_source_ids.add(sv.lower())

    if affected_tracks == 0:
        return None

    return BackfillCandidate(
        description=description,
        affected_tracks=affected_tracks,
        distinct_source_ids=len(distinct_source_ids),
    )


def _build_mb_section(
    conn: sqlite3.Connection,
    total_files: int,
    file_album_dirs: dict[int, str],
    all_file_ids: set[int],
    artist_filter: str | None = None,
) -> IdSectionData:
    """Build the MusicBrainz section of the report."""
    # Coverage table.
    coverage: list[IdCoverageRow] = []
    tag_maps: dict[str, dict[int, str]] = {}
    for key in _MB_KEYS:
        tag_map = _build_file_tag_map(conn, key, artist_filter)
        tag_maps[key] = tag_map
        coverage.append(_coverage_row(key, tag_map, total_files, is_valid_uuid))
    # Sort by valid % ascending (worst-covered first).
    coverage.sort(key=lambda r: r.valid_pct)

    # Partial album coverage.
    album_key_values = tag_maps.get(_MB_ALBUM_KEY, {})
    partial = _partial_album_coverage(
        album_key_values, file_album_dirs, all_file_ids, is_valid_uuid
    )

    # Duplicate MUSICBRAINZ_TRACKID.
    duplicates = _find_duplicate_ids(conn, _MB_TRACK_KEY)

    # Quick-win backfill: ALBUMID present but RELEASEGROUPID absent.
    source_vals = tag_maps.get(_MB_ALBUM_KEY, {})
    target_vals = tag_maps.get(_MB_BACKFILL_TARGET, {})
    backfill = _backfill_candidates(
        source_vals,
        target_vals,
        is_valid_uuid,
        (
            f"Tracks with valid {_MB_ALBUM_KEY} but missing "
            f"{_MB_BACKFILL_TARGET} — backfillable via MusicBrainz API "
            f"with one lookup per unique album ID"
        ),
    )

    return IdSectionData(
        source_name="MusicBrainz",
        coverage=coverage,
        partial_albums=partial,
        duplicate_ids=duplicates,
        backfill=backfill,
    )


def _build_discogs_section(
    conn: sqlite3.Connection,
    total_files: int,
    file_album_dirs: dict[int, str],
    all_file_ids: set[int],
    artist_filter: str | None = None,
) -> IdSectionData:
    """Build the Discogs section of the report."""
    # Coverage table.
    coverage: list[IdCoverageRow] = []
    tag_maps: dict[str, dict[int, str]] = {}
    for key in _DISCOGS_KEYS:
        tag_map = _build_file_tag_map(conn, key, artist_filter)
        tag_maps[key] = tag_map
        coverage.append(_coverage_row(key, tag_map, total_files, is_valid_discogs_id))
    # Sort by valid % ascending.
    coverage.sort(key=lambda r: r.valid_pct)

    # Partial album coverage.
    album_key_values = tag_maps.get(_DISCOGS_ALBUM_KEY, {})
    partial = _partial_album_coverage(
        album_key_values, file_album_dirs, all_file_ids, is_valid_discogs_id
    )

    # Duplicate DISCOGS_RELEASE_ID (across different album directories).
    duplicates = _find_duplicate_ids(conn, _DISCOGS_ALBUM_KEY)

    # Quick-win backfill: RELEASE_ID present but MASTER_ID absent.
    source_vals = tag_maps.get(_DISCOGS_ALBUM_KEY, {})
    target_vals = tag_maps.get(_DISCOGS_BACKFILL_TARGET, {})
    backfill = _backfill_candidates(
        source_vals,
        target_vals,
        is_valid_discogs_id,
        (
            f"Tracks with valid {_DISCOGS_ALBUM_KEY} but missing "
            f"{_DISCOGS_BACKFILL_TARGET} — backfillable via Discogs API "
            f"with one lookup per unique release ID"
        ),
    )

    return IdSectionData(
        source_name="Discogs",
        coverage=coverage,
        partial_albums=partial,
        duplicate_ids=duplicates,
        backfill=backfill,
    )


def _overall_coverage_pct(
    tag_maps: dict[str, dict[int, str]],
    keys: tuple[str, ...],
    total_files: int,
    all_file_ids: set[int],
    validator: Callable[[str], bool],
) -> float:
    """Compute the % of files where ALL given ID keys are present and valid."""
    if total_files == 0:
        return 0.0
    fully_covered = 0
    for fid in all_file_ids:
        all_valid = True
        for key in keys:
            val = tag_maps.get(key, {}).get(fid)
            if not val or not val.strip() or not validator(val.strip()):
                all_valid = False
                break
        if all_valid:
            fully_covered += 1
    return safe_pct(fully_covered, total_files)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
) -> IdsFullData:
    """Return the complete dataset for ``report ids``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    artist_filter:
        Optional case-insensitive ALBUMARTIST substring filter.
    """
    total_files = _get_total_ok_files(conn, artist_filter)
    file_album_dirs, file_paths = _build_file_album_dir_map(conn, artist_filter)
    all_file_ids = set(file_album_dirs.keys())

    mb = _build_mb_section(
        conn, total_files, file_album_dirs, all_file_ids, artist_filter
    )
    discogs = _build_discogs_section(
        conn, total_files, file_album_dirs, all_file_ids, artist_filter
    )

    return IdsFullData(
        total_files=total_files,
        musicbrainz=mb,
        discogs=discogs,
    )


def summary_data(conn: sqlite3.Connection) -> IdsSummaryData:
    """Return headline metrics only for the ``summary`` command.

    - MusicBrainz: overall coverage %, malformed count, partial album count,
      backfill track count.
    - Discogs: same structure.
    """
    total_files = _get_total_ok_files(conn)
    file_album_dirs, _ = _build_file_album_dir_map(conn)
    all_file_ids = set(file_album_dirs.keys())

    # Build tag maps for overall coverage calculation.
    mb_tag_maps: dict[str, dict[int, str]] = {}
    mb_malformed = 0
    for key in _MB_KEYS:
        tag_map = _build_file_tag_map(conn, key)
        mb_tag_maps[key] = tag_map
        row = _coverage_row(key, tag_map, total_files, is_valid_uuid)
        mb_malformed += row.malformed_count

    mb_overall_pct = _overall_coverage_pct(
        mb_tag_maps, _MB_KEYS, total_files, all_file_ids, is_valid_uuid
    )

    mb_album_vals = mb_tag_maps.get(_MB_ALBUM_KEY, {})
    mb_partial = _partial_album_coverage(
        mb_album_vals, file_album_dirs, all_file_ids, is_valid_uuid
    )
    mb_backfill = _backfill_candidates(
        mb_tag_maps.get(_MB_ALBUM_KEY, {}),
        mb_tag_maps.get(_MB_BACKFILL_TARGET, {}),
        is_valid_uuid,
        "",
    )

    discogs_tag_maps: dict[str, dict[int, str]] = {}
    discogs_malformed = 0
    for key in _DISCOGS_KEYS:
        tag_map = _build_file_tag_map(conn, key)
        discogs_tag_maps[key] = tag_map
        row = _coverage_row(key, tag_map, total_files, is_valid_discogs_id)
        discogs_malformed += row.malformed_count

    discogs_overall_pct = _overall_coverage_pct(
        discogs_tag_maps,
        _DISCOGS_KEYS,
        total_files,
        all_file_ids,
        is_valid_discogs_id,
    )

    discogs_album_vals = discogs_tag_maps.get(_DISCOGS_ALBUM_KEY, {})
    discogs_partial = _partial_album_coverage(
        discogs_album_vals, file_album_dirs, all_file_ids, is_valid_discogs_id
    )
    discogs_backfill = _backfill_candidates(
        discogs_tag_maps.get(_DISCOGS_ALBUM_KEY, {}),
        discogs_tag_maps.get(_DISCOGS_BACKFILL_TARGET, {}),
        is_valid_discogs_id,
        "",
    )

    return IdsSummaryData(
        total_files=total_files,
        mb_overall_coverage_pct=mb_overall_pct,
        mb_malformed_count=mb_malformed,
        mb_partial_album_count=len(mb_partial),
        mb_backfill_track_count=mb_backfill.affected_tracks if mb_backfill else 0,
        discogs_overall_coverage_pct=discogs_overall_pct,
        discogs_malformed_count=discogs_malformed,
        discogs_partial_album_count=len(discogs_partial),
        discogs_backfill_track_count=(
            discogs_backfill.affected_tracks if discogs_backfill else 0
        ),
    )
