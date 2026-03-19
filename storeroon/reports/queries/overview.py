"""
storeroon.reports.queries.overview — Report 1: Collection overview.

Aggregate queries against ``files``, ``flac_properties``, and ``raw_tags``.
Builds a hierarchical breakdown: Artist → Folder type → Release group → Pressing.

Public API:
    full_data(conn) -> OverviewFullData
    summary_data(conn) -> OverviewSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    ArtistBreakdown,
    DistributionSummary,
    FolderTypeBreakdown,
    OverviewFullData,
    OverviewSummaryData,
    OverviewTotals,
    PressingBreakdown,
    ReleaseGroupBreakdown,
)
from storeroon.reports.utils import median, safe_div

# ---------------------------------------------------------------------------
# Path segment helpers
# ---------------------------------------------------------------------------


def _split_path(path: str) -> tuple[str, str, str, str]:
    """Split a file path into (artist, folder_type, release_group, pressing).

    Expects: ``artist/folder_type/release_group/pressing/filename.flac``
    Returns four segment names. Missing segments default to 'Unknown'.
    """
    parts = path.split("/")
    artist = parts[0] if len(parts) > 0 else "Unknown"
    folder_type = parts[1] if len(parts) > 1 else "Unknown"
    release_group = parts[2] if len(parts) > 2 else "Unknown"
    pressing = parts[3] if len(parts) > 3 else "Unknown"
    return artist, folder_type, release_group, pressing


# ---------------------------------------------------------------------------
# Top-level totals
# ---------------------------------------------------------------------------

_TOTALS_SQL = """
SELECT
    COUNT(*)                                              AS total_tracks,
    COALESCE(SUM(fp.duration_seconds), 0.0)               AS total_duration,
    COALESCE(SUM(f.size_bytes), 0)                        AS total_size
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
WHERE f.status = 'ok'
"""

_ALBUM_DIR_EXPR = "SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1)"

_DISTINCT_ARTISTS_SQL = f"""
SELECT COUNT(DISTINCT SUBSTR(f.path, 1, INSTR(f.path, '/') - 1))
FROM files f
WHERE f.status = 'ok'
"""

_DISTINCT_DISCS_SQL = f"""
SELECT COUNT(DISTINCT {_ALBUM_DIR_EXPR})
FROM files f
WHERE f.status = 'ok'
"""


def _query_totals(conn: sqlite3.Connection) -> OverviewTotals:
    row = conn.execute(_TOTALS_SQL).fetchone()
    artists = conn.execute(_DISTINCT_ARTISTS_SQL).fetchone()[0]
    discs = conn.execute(_DISTINCT_DISCS_SQL).fetchone()[0]
    return OverviewTotals(
        total_tracks=row["total_tracks"],
        total_artists=artists,
        total_discs=discs,
        total_duration_seconds=row["total_duration"] or 0.0,
        total_size_bytes=row["total_size"] or 0,
    )


# ---------------------------------------------------------------------------
# Hierarchical breakdown: Artist → Folder type → Release group → Pressing
# ---------------------------------------------------------------------------

_FILES_SQL = """
SELECT
    f.path,
    f.filename,
    f.size_bytes,
    fp.duration_seconds,
    COALESCE(dn.disc_number, '1') AS disc_number
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value AS disc_number
    FROM raw_tags
    WHERE tag_key_upper = 'DISCNUMBER'
      AND tag_index = 0
) dn ON dn.file_id = f.id
WHERE f.status = 'ok'
"""


def _query_hierarchy(conn: sqlite3.Connection) -> list[ArtistBreakdown]:
    """Build the full 4-level hierarchy from file paths."""
    rows = conn.execute(_FILES_SQL).fetchall()

    # Accumulate data per pressing (artist, folder_type, release_group, pressing)
    # For each pressing, collect: track_count, size, duration, disc_numbers
    PressKey = tuple[str, str, str, str]  # (artist, folder_type, release_group, pressing)

    pressing_tracks: dict[PressKey, int] = defaultdict(int)
    pressing_size: dict[PressKey, int] = defaultdict(int)
    pressing_duration: dict[PressKey, float] = defaultdict(float)
    pressing_discs: dict[PressKey, set[str]] = defaultdict(set)
    pressing_dirs: dict[PressKey, str] = {}

    for row in rows:
        path: str = row["path"]
        filename: str = row["filename"]
        size: int = row["size_bytes"] or 0
        duration: float = row["duration_seconds"] or 0.0
        disc_num: str = row["disc_number"] or "1"

        artist, folder_type, release_group, pressing = _split_path(path)
        key: PressKey = (artist, folder_type, release_group, pressing)

        pressing_tracks[key] += 1
        pressing_size[key] += size
        pressing_duration[key] += duration
        pressing_discs[key].add(disc_num)

        if key not in pressing_dirs:
            # album_dir = path without filename
            album_dir = path[: len(path) - len(filename) - 1] if filename else path
            pressing_dirs[key] = album_dir

    # Build bottom-up: Pressing → ReleaseGroup → FolderType → Artist

    # Group pressings by (artist, folder_type, release_group)
    rg_groups: dict[tuple[str, str, str], list[PressingBreakdown]] = defaultdict(list)
    for key in sorted(pressing_tracks.keys()):
        artist, folder_type, release_group, pressing_name = key
        pb = PressingBreakdown(
            pressing_dir=pressing_dirs.get(key, ""),
            pressing_name=pressing_name,
            track_count=pressing_tracks[key],
            disc_count=len(pressing_discs[key]),
            total_size_bytes=pressing_size[key],
            total_duration_seconds=pressing_duration[key],
        )
        rg_groups[(artist, folder_type, release_group)].append(pb)

    # Group release groups by (artist, folder_type)
    ft_groups: dict[tuple[str, str], list[ReleaseGroupBreakdown]] = defaultdict(list)
    for (artist, folder_type, release_group), pressings in sorted(rg_groups.items()):
        rgb = ReleaseGroupBreakdown(
            release_group=release_group,
            track_count=sum(p.track_count for p in pressings),
            disc_count=sum(p.disc_count for p in pressings),
            total_size_bytes=sum(p.total_size_bytes for p in pressings),
            total_duration_seconds=sum(p.total_duration_seconds for p in pressings),
            pressings=pressings,
        )
        ft_groups[(artist, folder_type)].append(rgb)

    # Group folder types by artist
    artist_groups: dict[str, list[FolderTypeBreakdown]] = defaultdict(list)
    for (artist, folder_type), release_groups in sorted(ft_groups.items()):
        ftb = FolderTypeBreakdown(
            folder_type=folder_type,
            track_count=sum(rg.track_count for rg in release_groups),
            disc_count=sum(rg.disc_count for rg in release_groups),
            total_size_bytes=sum(rg.total_size_bytes for rg in release_groups),
            total_duration_seconds=sum(rg.total_duration_seconds for rg in release_groups),
            release_groups=release_groups,
        )
        artist_groups[artist].append(ftb)

    # Build final artist list, sorted by track count descending
    result: list[ArtistBreakdown] = []
    for artist in sorted(artist_groups.keys()):
        fts = artist_groups[artist]
        result.append(
            ArtistBreakdown(
                artist=artist,
                track_count=sum(ft.track_count for ft in fts),
                disc_count=sum(ft.disc_count for ft in fts),
                total_size_bytes=sum(ft.total_size_bytes for ft in fts),
                total_duration_seconds=sum(ft.total_duration_seconds for ft in fts),
                folder_types=fts,
            )
        )

    result.sort(key=lambda a: a.track_count, reverse=True)
    return result


# ---------------------------------------------------------------------------
# Distribution summary (median / average stats)
# ---------------------------------------------------------------------------

_DURATIONS_SQL = """
SELECT duration_seconds
FROM flac_properties
WHERE duration_seconds IS NOT NULL
ORDER BY duration_seconds
"""

_FILE_SIZES_SQL = """
SELECT f.size_bytes
FROM files f
WHERE f.status = 'ok' AND f.size_bytes IS NOT NULL
ORDER BY f.size_bytes
"""

_BITRATES_SQL = """
SELECT approx_bitrate_kbps
FROM flac_properties
WHERE approx_bitrate_kbps IS NOT NULL
ORDER BY approx_bitrate_kbps
"""


def _query_distribution(conn: sqlite3.Connection) -> DistributionSummary:
    durations = [row[0] for row in conn.execute(_DURATIONS_SQL).fetchall()]
    sizes = [row[0] for row in conn.execute(_FILE_SIZES_SQL).fetchall()]
    bitrates = [row[0] for row in conn.execute(_BITRATES_SQL).fetchall()]

    return DistributionSummary(
        median_track_duration_seconds=median(durations),
        median_file_size_bytes=int(median(sizes)),
        avg_bitrate_kbps=safe_div(sum(bitrates), len(bitrates)),
        median_bitrate_kbps=median(bitrates),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(conn: sqlite3.Connection) -> OverviewFullData:
    """Return the complete dataset for ``report overview``."""
    totals = _query_totals(conn)
    by_artist = _query_hierarchy(conn)
    distribution = _query_distribution(conn)
    return OverviewFullData(
        totals=totals,
        by_artist=by_artist,
        distribution=distribution,
    )


def summary_data(conn: sqlite3.Connection) -> OverviewSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    totals = _query_totals(conn)
    return OverviewSummaryData(totals=totals)
