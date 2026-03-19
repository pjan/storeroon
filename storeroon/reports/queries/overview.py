"""
storeroon.reports.queries.overview — Report 1: Collection overview.

Aggregate queries against ``files``, ``flac_properties``, and ``raw_tags``.
Builds a tag-based hierarchical breakdown:
    Artist (ALBUMARTIST) → Release type (RELEASETYPE) → Album (ALBUM) → Catalog (CATALOGNUMBER)

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
    CatalogBreakdown,
    OverviewFullData,
    OverviewSummaryData,
    OverviewTotals,
    ReleaseTypeBreakdown,
)

# ---------------------------------------------------------------------------
# Query: one row per file with relevant tag values
# ---------------------------------------------------------------------------

_FILES_SQL = """
SELECT
    f.id,
    f.size_bytes,
    fp.duration_seconds,
    COALESCE(t_aa.tag_value, 'Unknown')         AS albumartist,
    COALESCE(t_rt.tag_value, 'unknown')          AS releasetype,
    COALESCE(t_al.tag_value, 'Unknown')          AS album,
    COALESCE(t_cn.tag_value, 'none')             AS catalognumber,
    COALESCE(t_td.tag_value, '1')                AS totaldiscs
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
    WHERE tag_key_upper = 'TOTALDISCS' AND tag_index = 0
) t_td ON t_td.file_id = f.id
WHERE f.status = 'ok'
"""


def _parse_totaldiscs(value: str) -> int:
    """Parse TOTALDISCS to an int, defaulting to 1."""
    try:
        n = int(value.strip())
        return max(n, 1)
    except (ValueError, AttributeError):
        return 1


# ---------------------------------------------------------------------------
# Hierarchy builder
# ---------------------------------------------------------------------------

# Key type for catalog-level grouping
_CatKey = tuple[str, str, str, str]  # (artist, releasetype, album, catalognumber)


def _build_hierarchy(
    conn: sqlite3.Connection,
) -> tuple[list[ArtistBreakdown], OverviewTotals]:
    """Build the full 4-level hierarchy from tag values and compute totals."""
    rows = conn.execute(_FILES_SQL).fetchall()

    # Accumulate per catalog-number group
    cat_tracks: dict[_CatKey, int] = defaultdict(int)
    cat_size: dict[_CatKey, int] = defaultdict(int)
    cat_duration: dict[_CatKey, float] = defaultdict(float)
    cat_totaldiscs: dict[_CatKey, int] = {}  # take max TOTALDISCS per catalog

    total_tracks = 0
    total_size = 0
    total_duration = 0.0

    for row in rows:
        artist: str = row["albumartist"] or "Unknown"
        rtype: str = (row["releasetype"] or "unknown").lower()
        album: str = row["album"] or "Unknown"
        catno: str = (row["catalognumber"] or "none").strip() or "none"
        totaldiscs = _parse_totaldiscs(row["totaldiscs"])
        size: int = row["size_bytes"] or 0
        duration: float = row["duration_seconds"] or 0.0

        key: _CatKey = (artist, rtype, album, catno)

        cat_tracks[key] += 1
        cat_size[key] += size
        cat_duration[key] += duration
        # Keep the maximum TOTALDISCS seen for this catalog group
        if key not in cat_totaldiscs or totaldiscs > cat_totaldiscs[key]:
            cat_totaldiscs[key] = totaldiscs

        total_tracks += 1
        total_size += size
        total_duration += duration

    # Compute totals
    all_artists: set[str] = set()
    all_albums: set[tuple[str, str]] = set()  # (artist, album)
    all_releases: set[_CatKey] = set()

    for key in cat_tracks:
        artist, rtype, album, catno = key
        all_artists.add(artist)
        all_albums.add((artist, album))
        all_releases.add(key)

    totals = OverviewTotals(
        total_album_artists=len(all_artists),
        total_albums=len(all_albums),
        total_releases=len(all_releases),
        total_tracks=total_tracks,
        total_duration_seconds=total_duration,
        total_size_bytes=total_size,
    )

    # Build bottom-up: Catalog → Album → ReleaseType → Artist

    # Group catalogs by (artist, releasetype, album)
    album_groups: dict[tuple[str, str, str], list[CatalogBreakdown]] = defaultdict(list)
    for key in sorted(cat_tracks.keys()):
        artist, rtype, album, catno = key
        cb = CatalogBreakdown(
            catalog_number=catno,
            track_count=cat_tracks[key],
            disc_count=cat_totaldiscs.get(key, 1),
            total_size_bytes=cat_size[key],
            total_duration_seconds=cat_duration[key],
        )
        album_groups[(artist, rtype, album)].append(cb)

    # Group albums by (artist, releasetype)
    rtype_groups: dict[tuple[str, str], list[AlbumBreakdown]] = defaultdict(list)
    for (artist, rtype, album), catalogs in sorted(album_groups.items()):
        ab = AlbumBreakdown(
            album=album,
            track_count=sum(c.track_count for c in catalogs),
            disc_count=sum(c.disc_count for c in catalogs),
            total_size_bytes=sum(c.total_size_bytes for c in catalogs),
            total_duration_seconds=sum(c.total_duration_seconds for c in catalogs),
            catalogs=catalogs,
        )
        rtype_groups[(artist, rtype)].append(ab)

    # Group release types by artist
    artist_groups: dict[str, list[ReleaseTypeBreakdown]] = defaultdict(list)
    for (artist, rtype), albums in sorted(rtype_groups.items()):
        rtb = ReleaseTypeBreakdown(
            release_type=rtype,
            track_count=sum(a.track_count for a in albums),
            disc_count=sum(a.disc_count for a in albums),
            total_size_bytes=sum(a.total_size_bytes for a in albums),
            total_duration_seconds=sum(a.total_duration_seconds for a in albums),
            albums=albums,
        )
        artist_groups[artist].append(rtb)

    # Build final artist list, sorted by track count descending
    result: list[ArtistBreakdown] = []
    for artist in sorted(artist_groups.keys()):
        rtypes = artist_groups[artist]
        result.append(
            ArtistBreakdown(
                artist=artist,
                track_count=sum(rt.track_count for rt in rtypes),
                disc_count=sum(rt.disc_count for rt in rtypes),
                total_size_bytes=sum(rt.total_size_bytes for rt in rtypes),
                total_duration_seconds=sum(rt.total_duration_seconds for rt in rtypes),
                release_types=rtypes,
            )
        )

    result.sort(key=lambda a: a.track_count, reverse=True)
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
