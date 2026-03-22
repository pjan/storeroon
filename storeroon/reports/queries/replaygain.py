"""
storeroon.reports.queries.replaygain — Report 13: ReplayGain coverage.

Query ``raw_tags`` for the four ReplayGain tag keys. Parse gain values
(float from strings like ``-6.23 dB`` — strip the `` dB`` suffix). Flag
values outside the range -20.0 dB to +10.0 dB as outliers.

Full report tables:
    1. Coverage for all four tags (valid / malformed / absent)
    2. Partially-tagged albums (some tracks have album-level RG, others don't)
    3. Track gain value distribution (histogram in 2 dB buckets)
    4. Outliers (gain values outside [-20.0, +10.0] dB)

Public API:
    full_data(conn, artist_filter=None) -> ReplayGainFullData
    summary_data(conn) -> ReplayGainSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    BucketCount,
    PartialRgAlbum,
    ReplayGainCoverageRow,
    ReplayGainFullData,
    ReplayGainSummaryData,
    RgOutlier,
)
from storeroon.reports.utils import (
    TOTAL_OK_FILES_FILTERED_SQL,
    TOTAL_OK_FILES_SQL,
    parse_replaygain_db,
    safe_pct,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_RG_KEYS: tuple[str, ...] = (
    "REPLAYGAIN_TRACK_GAIN",
    "REPLAYGAIN_TRACK_PEAK",
    "REPLAYGAIN_ALBUM_GAIN",
    "REPLAYGAIN_ALBUM_PEAK",
)

# Keys that represent album-level ReplayGain tags.
_RG_ALBUM_KEYS: tuple[str, ...] = (
    "REPLAYGAIN_ALBUM_GAIN",
    "REPLAYGAIN_ALBUM_PEAK",
)

# Keys that represent track-level ReplayGain tags.
_RG_TRACK_KEYS: tuple[str, ...] = (
    "REPLAYGAIN_TRACK_GAIN",
    "REPLAYGAIN_TRACK_PEAK",
)

# Gain keys (not peak) — used for distribution and outlier checks.
_RG_GAIN_KEYS: tuple[str, ...] = (
    "REPLAYGAIN_TRACK_GAIN",
    "REPLAYGAIN_ALBUM_GAIN",
)

# Outlier thresholds (in dB).
_OUTLIER_MIN_DB = -20.0
_OUTLIER_MAX_DB = 10.0

# Histogram bucket boundaries for track gain distribution (2 dB steps).
_GAIN_BUCKET_BOUNDARIES: list[tuple[str, float, float]] = [
    ("<-18 dB", -999.0, -18.0),
    ("-18 to -16 dB", -18.0, -16.0),
    ("-16 to -14 dB", -16.0, -14.0),
    ("-14 to -12 dB", -14.0, -12.0),
    ("-12 to -10 dB", -12.0, -10.0),
    ("-10 to -8 dB", -10.0, -8.0),
    ("-8 to -6 dB", -8.0, -6.0),
    ("-6 to -4 dB", -6.0, -4.0),
    ("-4 to -2 dB", -4.0, -2.0),
    ("-2 to 0 dB", -2.0, 0.0),
    ("0 to +2 dB", 0.0, 2.0),
    ("+2 to +4 dB", 2.0, 4.0),
    ("+4 to +6 dB", 4.0, 6.0),
    ("+6 to +8 dB", 6.0, 8.0),
    (">+8 dB", 8.0, 999.0),
]

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Fetch all (file_id, tag_value) pairs for a given ReplayGain tag key.
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

# All ok file IDs with their album directory and path.
_FILES_WITH_ALBUM_DIR_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
WHERE f.status = 'ok'
"""

_FILES_WITH_ALBUM_DIR_FILTERED_SQL = """
SELECT DISTINCT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_valid_rg_value(value: str) -> bool:
    """Check whether a ReplayGain tag value is parseable.

    For gain tags this means a float optionally followed by `` dB``.
    For peak tags this means a non-negative float (no unit suffix expected,
    but we tolerate a trailing `` dB`` or similar).
    """
    return parse_replaygain_db(value) is not None


def _get_total_ok_files(
    conn: sqlite3.Connection,
    artist_filter: str | None = None,
) -> int:
    """Return count of files with status='ok', optionally filtered."""
    if artist_filter:
        row = conn.execute(TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
    else:
        row = conn.execute(TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _build_file_tag_map(
    conn: sqlite3.Connection,
    tag_key: str,
    artist_filter: str | None = None,
) -> dict[int, str]:
    """Return a mapping of file_id -> tag_value for a given tag key."""
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


def _coverage_for_key(
    tag_key: str,
    tag_values: dict[int, str],
    total_files: int,
) -> ReplayGainCoverageRow:
    """Compute coverage stats for a single ReplayGain tag key."""
    valid = 0
    malformed = 0
    for value in tag_values.values():
        v = value.strip()
        if not v:
            continue
        if _is_valid_rg_value(v):
            valid += 1
        else:
            malformed += 1

    files_with_tag = valid + malformed
    absent = total_files - files_with_tag
    if absent < 0:
        absent = 0

    return ReplayGainCoverageRow(
        tag_key=tag_key,
        valid_count=valid,
        valid_pct=safe_pct(valid, total_files),
        malformed_count=malformed,
        malformed_pct=safe_pct(malformed, total_files),
        absent_count=absent,
        absent_pct=safe_pct(absent, total_files),
    )


def _find_partial_albums(
    album_gain_values: dict[int, str],
    album_peak_values: dict[int, str],
    file_album_dirs: dict[int, str],
    all_file_ids: set[int],
) -> list[PartialRgAlbum]:
    """Find albums where some tracks have album-level ReplayGain and others don't.

    A track is considered to "have album-level RG" if it has a valid
    ``REPLAYGAIN_ALBUM_GAIN`` value. The peak is not required for this check,
    but its absence alongside a present gain is unusual enough to surface.
    """
    # Group file IDs by album directory.
    album_files: dict[str, list[int]] = defaultdict(list)
    for fid in all_file_ids:
        adir = file_album_dirs.get(fid)
        if adir is not None:
            album_files[adir].append(fid)

    results: list[PartialRgAlbum] = []
    for album_dir, fids in sorted(album_files.items()):
        with_rg = 0
        without_rg = 0
        for fid in fids:
            gain_val = album_gain_values.get(fid)
            if gain_val and gain_val.strip() and _is_valid_rg_value(gain_val.strip()):
                with_rg += 1
            else:
                without_rg += 1
        # Only flag partial coverage (some have it, some don't).
        if with_rg > 0 and without_rg > 0:
            results.append(
                PartialRgAlbum(
                    album_dir=album_dir,
                    tracks_with_rg=with_rg,
                    tracks_without_rg=without_rg,
                    total_tracks=len(fids),
                )
            )

    # Sort by tracks_without_rg descending (worst coverage first).
    results.sort(key=lambda r: r.tracks_without_rg, reverse=True)
    return results


def _build_gain_distribution(
    track_gain_values: dict[int, str],
) -> list[BucketCount]:
    """Build a histogram of REPLAYGAIN_TRACK_GAIN values in 2 dB buckets."""
    parsed_values: list[float] = []
    for value in track_gain_values.values():
        db = parse_replaygain_db(value)
        if db is not None:
            parsed_values.append(db)

    if not parsed_values:
        return [
            BucketCount(label=label, count=0, percentage=0.0)
            for label, _, _ in _GAIN_BUCKET_BOUNDARIES
        ]

    total = len(parsed_values)
    bucket_counts: dict[str, int] = {
        label: 0 for label, _, _ in _GAIN_BUCKET_BOUNDARIES
    }

    for db_val in parsed_values:
        for label, low, high in _GAIN_BUCKET_BOUNDARIES:
            if low <= db_val < high:
                bucket_counts[label] += 1
                break
        else:
            # Value falls outside all defined buckets (shouldn't happen given
            # the wide -999/+999 bounds, but handle gracefully).
            # Assign to the nearest boundary bucket.
            if db_val < _GAIN_BUCKET_BOUNDARIES[0][1]:
                bucket_counts[_GAIN_BUCKET_BOUNDARIES[0][0]] += 1
            else:
                bucket_counts[_GAIN_BUCKET_BOUNDARIES[-1][0]] += 1

    return [
        BucketCount(
            label=label,
            count=bucket_counts[label],
            percentage=safe_pct(bucket_counts[label], total),
        )
        for label, _, _ in _GAIN_BUCKET_BOUNDARIES
    ]


def _find_outliers(
    conn: sqlite3.Connection,
    file_path_map: dict[int, str],
    artist_filter: str | None = None,
) -> list[RgOutlier]:
    """Find tracks where the parsed gain value is outside [-20.0, +10.0] dB.

    Checks both REPLAYGAIN_TRACK_GAIN and REPLAYGAIN_ALBUM_GAIN.
    """
    outliers: list[RgOutlier] = []

    for gain_key in _RG_GAIN_KEYS:
        tag_map = _build_file_tag_map(conn, gain_key, artist_filter)
        for fid, value in tag_map.items():
            db_val = parse_replaygain_db(value)
            if db_val is None:
                continue
            if db_val < _OUTLIER_MIN_DB or db_val > _OUTLIER_MAX_DB:
                path = file_path_map.get(fid, "(unknown path)")
                outliers.append(
                    RgOutlier(
                        path=path,
                        tag_key=gain_key,
                        value=value.strip(),
                        parsed_db=db_val,
                    )
                )

    # Sort by absolute dB value descending (most extreme first).
    outliers.sort(key=lambda o: abs(o.parsed_db), reverse=True)
    return outliers


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
) -> ReplayGainFullData:
    """Return the complete dataset for ``report replaygain``.

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

    # Coverage for all four tags.
    coverage: list[ReplayGainCoverageRow] = []
    tag_maps: dict[str, dict[int, str]] = {}
    for key in _RG_KEYS:
        tag_map = _build_file_tag_map(conn, key, artist_filter)
        tag_maps[key] = tag_map
        coverage.append(_coverage_for_key(key, tag_map, total_files))

    # Partially-tagged albums.
    album_gain_values = tag_maps.get("REPLAYGAIN_ALBUM_GAIN", {})
    album_peak_values = tag_maps.get("REPLAYGAIN_ALBUM_PEAK", {})
    partial_albums = _find_partial_albums(
        album_gain_values, album_peak_values, file_album_dirs, all_file_ids
    )

    # Track gain value distribution.
    track_gain_values = tag_maps.get("REPLAYGAIN_TRACK_GAIN", {})
    gain_distribution = _build_gain_distribution(track_gain_values)

    # Outliers.
    outliers = _find_outliers(conn, file_paths, artist_filter)

    return ReplayGainFullData(
        total_files=total_files,
        coverage=coverage,
        partial_albums=partial_albums,
        gain_distribution=gain_distribution,
        outliers=outliers,
    )


def summary_data(conn: sqlite3.Connection) -> ReplayGainSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Track-level coverage %, album-level coverage %, count of partially-tagged
    albums, count of outlier values.
    """
    total_files = _get_total_ok_files(conn)
    file_album_dirs, file_paths = _build_file_album_dir_map(conn)
    all_file_ids = set(file_album_dirs.keys())

    # Track-level coverage: % of files with valid REPLAYGAIN_TRACK_GAIN.
    track_gain_map = _build_file_tag_map(conn, "REPLAYGAIN_TRACK_GAIN")
    track_gain_cov = _coverage_for_key(
        "REPLAYGAIN_TRACK_GAIN", track_gain_map, total_files
    )
    track_coverage_pct = track_gain_cov.valid_pct

    # Album-level coverage: % of files with valid REPLAYGAIN_ALBUM_GAIN.
    album_gain_map = _build_file_tag_map(conn, "REPLAYGAIN_ALBUM_GAIN")
    album_gain_cov = _coverage_for_key(
        "REPLAYGAIN_ALBUM_GAIN", album_gain_map, total_files
    )
    album_coverage_pct = album_gain_cov.valid_pct

    # Partially-tagged albums.
    album_peak_map = _build_file_tag_map(conn, "REPLAYGAIN_ALBUM_PEAK")
    partial_albums = _find_partial_albums(
        album_gain_map, album_peak_map, file_album_dirs, all_file_ids
    )

    # Outliers.
    outliers = _find_outliers(conn, file_paths)

    return ReplayGainSummaryData(
        track_coverage_pct=track_coverage_pct,
        album_coverage_pct=album_coverage_pct,
        partial_album_count=len(partial_albums),
        outlier_count=len(outliers),
    )
