"""
storeroon.reports.queries.overview2 — Collection overview with scan issues.

Same folder-based hierarchy as overview, but enriched with per-album
issue counts from scan_issues.

Public API:
    full_data(conn) -> Overview2FullData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    AlbumBreakdown2,
    ArtistBreakdown2,
    IssuesTotals,
    Overview2FullData,
    OverviewTotals,
    ReleaseTypeBreakdown2,
)

# ---------------------------------------------------------------------------
# SQL
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
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ALBUMARTIST' AND tag_index = 0) t_aa ON t_aa.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'RELEASETYPE' AND tag_index = 0) t_rt ON t_rt.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ALBUM' AND tag_index = 0) t_al ON t_al.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'CATALOGNUMBER' AND tag_index = 0) t_cn ON t_cn.file_id = f.id
LEFT JOIN (SELECT file_id, tag_value FROM raw_tags WHERE tag_key_upper = 'ORIGINALDATE' AND tag_index = 0) t_od ON t_od.file_id = f.id
WHERE f.status = 'ok'
"""

_ISSUES_SQL = """
SELECT
    si.file_id,
    si.severity,
    f.path,
    f.filename
FROM scan_issues si
JOIN files f ON f.id = si.file_id
WHERE si.resolved = 0
  AND f.status = 'ok'
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_display_name(
    original_date: str | None, album: str, catalog_number: str | None
) -> str:
    """Build display: '{YYYY} - {album} [{catalognumber}]'."""
    parts: list[str] = []
    if original_date and original_date.strip():
        # Extract just the year (first 4 chars) for display
        year = original_date.strip()[:4]
        parts.append(year)
    parts.append(album)
    name = " - ".join(parts)
    if catalog_number and catalog_number.strip():
        name += f" [{catalog_number.strip()}]"
    return name


def _album_dir_from_path(path: str, filename: str) -> str:
    return path[: len(path) - len(filename) - 1] if filename else path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(conn: sqlite3.Connection) -> Overview2FullData:
    """Return collection overview with scan issue counts per album."""

    # ── Phase 1: Build folder hierarchy from files ──
    file_rows = conn.execute(_FILES_SQL).fetchall()

    folder_tracks: dict[str, int] = defaultdict(int)
    folder_size: dict[str, int] = defaultdict(int)
    folder_duration: dict[str, float] = defaultdict(float)
    folder_meta: dict[str, dict[str, str | None]] = {}
    folder_file_ids: dict[str, set[int]] = defaultdict(set)

    total_tracks = 0
    total_size = 0
    total_duration = 0.0

    for row in file_rows:
        adir: str = row["album_dir"] or ""
        size: int = row["size_bytes"] or 0
        duration: float = row["duration_seconds"] or 0.0

        folder_tracks[adir] += 1
        folder_size[adir] += size
        folder_duration[adir] += duration
        folder_file_ids[adir].add(row["id"])

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

    # ── Phase 2: Count issues per album folder ──
    issue_rows = conn.execute(_ISSUES_SQL).fetchall()

    folder_issues: dict[str, dict[str, int]] = defaultdict(lambda: {
        "critical": 0, "error": 0, "warning": 0, "info": 0,
    })
    files_with_issues: set[int] = set()
    total_issues = 0
    albums_with_issues: set[str] = set()

    for ir in issue_rows:
        adir = _album_dir_from_path(ir["path"], ir["filename"])
        sev = ir["severity"]
        if sev in ("critical", "error"):
            folder_issues[adir]["critical" if sev == "critical" else "error"] += 1
        elif sev == "warning":
            folder_issues[adir]["warning"] += 1
        else:
            folder_issues[adir]["info"] += 1
        files_with_issues.add(ir["file_id"])
        total_issues += 1
        albums_with_issues.add(adir)

    # ── Phase 3: Build hierarchy ──
    # Album level
    rtype_groups: dict[tuple[str, str], list[AlbumBreakdown2]] = defaultdict(list)

    for adir, meta in folder_meta.items():
        artist = meta["albumartist"] or "Unknown"
        rtype = meta["releasetype"] or "unknown"
        display = _make_display_name(
            meta["originaldate"], meta["album"] or "Unknown", meta["catalognumber"]
        )
        issues = folder_issues.get(adir, {"critical": 0, "error": 0, "warning": 0, "info": 0})

        ab = AlbumBreakdown2(
            album_dir=adir,
            display_name=display,
            track_count=folder_tracks[adir],
            total_size_bytes=folder_size[adir],
            total_duration_seconds=folder_duration[adir],
            critical_count=issues["critical"],
            error_count=issues["error"],
            warning_count=issues["warning"],
            info_count=issues["info"],
        )
        rtype_groups[(artist, rtype)].append(ab)

    # Sort albums by display name
    for albums in rtype_groups.values():
        albums.sort(key=lambda a: a.display_name.lower())

    # Release type level
    artist_groups: dict[str, list[ReleaseTypeBreakdown2]] = defaultdict(list)
    for (artist, rtype), albums in rtype_groups.items():
        rtb = ReleaseTypeBreakdown2(
            release_type=rtype,
            album_count=len(albums),
            track_count=sum(a.track_count for a in albums),
            total_size_bytes=sum(a.total_size_bytes for a in albums),
            total_duration_seconds=sum(a.total_duration_seconds for a in albums),
            critical_count=sum(a.critical_count for a in albums),
            error_count=sum(a.error_count for a in albums),
            warning_count=sum(a.warning_count for a in albums),
            info_count=sum(a.info_count for a in albums),
            albums=albums,
        )
        artist_groups[artist].append(rtb)

    # Sort release types
    for rtypes in artist_groups.values():
        rtypes.sort(key=lambda rt: rt.release_type.lower())

    # Artist level
    result: list[ArtistBreakdown2] = []
    for artist in sorted(artist_groups.keys(), key=str.lower):
        rtypes = artist_groups[artist]
        result.append(
            ArtistBreakdown2(
                artist=artist,
                album_count=sum(rt.album_count for rt in rtypes),
                track_count=sum(rt.track_count for rt in rtypes),
                total_size_bytes=sum(rt.total_size_bytes for rt in rtypes),
                total_duration_seconds=sum(rt.total_duration_seconds for rt in rtypes),
                critical_count=sum(rt.critical_count for rt in rtypes),
                error_count=sum(rt.error_count for rt in rtypes),
                warning_count=sum(rt.warning_count for rt in rtypes),
                info_count=sum(rt.info_count for rt in rtypes),
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

    issues_totals = IssuesTotals(
        albums_with_issues=len(albums_with_issues),
        files_with_issues=len(files_with_issues),
        total_issues=total_issues,
    )

    return Overview2FullData(
        totals=totals,
        issues_totals=issues_totals,
        by_artist=result,
    )
