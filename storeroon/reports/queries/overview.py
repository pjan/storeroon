"""
storeroon.reports.queries.overview — Report 1: Collection overview.

Builds a folder-based hierarchical breakdown:
    Artist (ALBUMARTIST) → Release type (RELEASETYPE) → Album (folder)

Level 0 (album) = all files in the same parent directory.
Display: "{originaldate} - {album} [{catalognumber}]"

Public API:
    full_data(conn) -> OverviewFullData
    summary_data(conn) -> OverviewSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    AlbumBreakdown,
    ArtistBreakdown,
    OverviewFullData,
    OverviewSummaryData,
    OverviewTotals,
    ReleaseTypeBreakdown,
)

# ---------------------------------------------------------------------------
# Query: one row per file with folder and tag metadata
# ---------------------------------------------------------------------------

_FILES_SQL = """
SELECT
    f.id,
    f.size_bytes,
    fp.duration_seconds,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir,
    COALESCE(t_aa.tag_value, 'Unknown')   AS albumartist,
    COALESCE(t_rt.tag_value, 'unknown')   AS releasetype,
    COALESCE(t_al.tag_value, 'Unknown')   AS album,
    t_cn.tag_value                         AS catalognumber,
    t_od.tag_value                         AS originaldate
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value FROM raw_tags
    WHERE tag_key_upper = 'ALBUMARTIST' AND tag_index = 0
) t_aa ON t_aa.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value FROM raw_tags
    WHERE tag_key_upper = 'RELEASETYPE' AND tag_index = 0
) t_rt ON t_rt.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value FROM raw_tags
    WHERE tag_key_upper = 'ALBUM' AND tag_index = 0
) t_al ON t_al.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value FROM raw_tags
    WHERE tag_key_upper = 'CATALOGNUMBER' AND tag_index = 0
) t_cn ON t_cn.file_id = f.id
LEFT JOIN (
    SELECT file_id, tag_value FROM raw_tags
    WHERE tag_key_upper = 'ORIGINALDATE' AND tag_index = 0
) t_od ON t_od.file_id = f.id
WHERE f.status = 'ok'
"""


# ---------------------------------------------------------------------------
# Hierarchy builder
# ---------------------------------------------------------------------------


def _make_display_name(
    original_date: str | None, album: str, catalog_number: str | None
) -> str:
    """Build display string: '{YYYY} - {album} [{catalognumber}]'."""
    parts: list[str] = []
    if original_date and original_date.strip():
        parts.append(original_date.strip()[:4])
    parts.append(album)
    name = " - ".join(parts)
    if catalog_number and catalog_number.strip():
        name += f" [{catalog_number.strip()}]"
    return name


def _build_hierarchy(
    conn: sqlite3.Connection,
) -> tuple[list[ArtistBreakdown], OverviewTotals]:
    """Build the 3-level hierarchy grouped by folder."""
    rows = conn.execute(_FILES_SQL).fetchall()

    # Accumulate per folder (album_dir)
    folder_tracks: dict[str, int] = defaultdict(int)
    folder_size: dict[str, int] = defaultdict(int)
    folder_duration: dict[str, float] = defaultdict(float)
    # Per-folder metadata (take first non-null from any file in folder)
    folder_meta: dict[str, dict[str, str | None]] = {}

    total_tracks = 0
    total_size = 0
    total_duration = 0.0

    for row in rows:
        adir: str = row["album_dir"] or ""
        size: int = row["size_bytes"] or 0
        duration: float = row["duration_seconds"] or 0.0

        folder_tracks[adir] += 1
        folder_size[adir] += size
        folder_duration[adir] += duration

        if adir not in folder_meta:
            folder_meta[adir] = {
                "albumartist": row["albumartist"],
                "releasetype": (row["releasetype"] or "unknown").lower(),
                "album": row["album"],
                "catalognumber": row["catalognumber"],
                "originaldate": row["originaldate"],
            }

        total_tracks += 1
        total_size += size
        total_duration += duration

    # Build AlbumBreakdown per folder
    all_albums: list[tuple[str, str, str, AlbumBreakdown]] = []
    # (artist, releasetype, sort_key, AlbumBreakdown)

    for adir, meta in folder_meta.items():
        artist = meta["albumartist"] or "Unknown"
        rtype = meta["releasetype"] or "unknown"
        display = _make_display_name(
            meta["originaldate"], meta["album"] or "Unknown", meta["catalognumber"]
        )
        ab = AlbumBreakdown(
            album_dir=adir,
            display_name=display,
            track_count=folder_tracks[adir],
            total_size_bytes=folder_size[adir],
            total_duration_seconds=folder_duration[adir],
        )
        all_albums.append((artist, rtype, display.lower(), ab))

    # Group by (artist, releasetype)
    rtype_groups: dict[tuple[str, str], list[AlbumBreakdown]] = defaultdict(list)
    for artist, rtype, sort_key, ab in all_albums:
        rtype_groups[(artist, rtype)].append(ab)

    # Sort albums within each release type by display name
    for albums in rtype_groups.values():
        albums.sort(key=lambda a: a.display_name.lower())

    # Group by artist
    artist_groups: dict[str, list[ReleaseTypeBreakdown]] = defaultdict(list)
    for (artist, rtype), albums in rtype_groups.items():
        rtb = ReleaseTypeBreakdown(
            release_type=rtype,
            album_count=len(albums),
            track_count=sum(a.track_count for a in albums),
            total_size_bytes=sum(a.total_size_bytes for a in albums),
            total_duration_seconds=sum(a.total_duration_seconds for a in albums),
            albums=albums,
        )
        artist_groups[artist].append(rtb)

    # Sort release types within each artist
    for rtypes in artist_groups.values():
        rtypes.sort(key=lambda rt: rt.release_type.lower())

    # Build final artist list, sorted alphabetically
    result: list[ArtistBreakdown] = []
    for artist in sorted(artist_groups.keys(), key=str.lower):
        rtypes = artist_groups[artist]
        result.append(
            ArtistBreakdown(
                artist=artist,
                album_count=sum(rt.album_count for rt in rtypes),
                track_count=sum(rt.track_count for rt in rtypes),
                total_size_bytes=sum(rt.total_size_bytes for rt in rtypes),
                total_duration_seconds=sum(rt.total_duration_seconds for rt in rtypes),
                release_types=rtypes,
            )
        )

    all_artists = {meta["albumartist"] or "Unknown" for meta in folder_meta.values()}

    totals = OverviewTotals(
        total_album_artists=len(all_artists),
        total_albums=len(folder_meta),
        total_tracks=total_tracks,
        total_duration_seconds=total_duration,
        total_size_bytes=total_size,
    )

    return result, totals


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(conn: sqlite3.Connection) -> OverviewFullData:
    """Return the complete dataset for ``report overview``."""
    by_artist, totals = _build_hierarchy(conn)
    return OverviewFullData(totals=totals, by_artist=by_artist)


def summary_data(conn: sqlite3.Connection) -> OverviewSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    _, totals = _build_hierarchy(conn)
    return OverviewSummaryData(totals=totals)
