"""
storeroon.reports.queries.issues — Report 9: Scan issues (album-centric).

Reads from the ``scan_issues`` table populated during Phase 1 import.
All queries filtered by ``resolved = FALSE`` by default.

Album-level aggregation:
    - Groups issues by album (using album directory path)
    - Shows artist, album, catalog number for each album
    - Aggregates counts by severity (error, warning, info)
    - Provides drill-down to file-level details for each album

Public API:
    full_data(conn, min_severity="info") -> IssuesFullData
    album_detail(conn, album_dir: str) -> AlbumIssuesDetail
    summary_data(conn) -> IssuesSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

import json

from storeroon.reports.models import (
    AlbumIssuesDetail,
    AlbumIssuesSummary,
    AlbumLevelIssue,
    AlbumReportData,
    FileIssueDetail,
    IssuesFullData,
    IssuesSummaryData,
    TrackDetail,
    TrackIssue,
)
from storeroon.reports.utils import severity_at_least, severity_order

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Get all issues with file and album metadata
_ISSUES_WITH_ALBUM_SQL = """
SELECT
    si.id,
    si.file_id,
    si.issue_type,
    si.severity,
    si.description,
    si.details,
    f.path AS file_path,
    f.filename AS file_filename,
    MAX(CASE WHEN rt_artist.tag_key_upper = 'ALBUMARTIST' THEN rt_artist.tag_value END) AS artist,
    MAX(CASE WHEN rt_album.tag_key_upper = 'ALBUM' THEN rt_album.tag_value END) AS album,
    MAX(CASE WHEN rt_cat.tag_key_upper = 'CATALOGNUMBER' THEN rt_cat.tag_value END) AS catalog_number
FROM scan_issues si
LEFT JOIN files f ON f.id = si.file_id
LEFT JOIN raw_tags rt_artist ON rt_artist.file_id = f.id AND rt_artist.tag_key_upper = 'ALBUMARTIST' AND rt_artist.tag_index = 0
LEFT JOIN raw_tags rt_album ON rt_album.file_id = f.id AND rt_album.tag_key_upper = 'ALBUM' AND rt_album.tag_index = 0
LEFT JOIN raw_tags rt_cat ON rt_cat.file_id = f.id AND rt_cat.tag_key_upper = 'CATALOGNUMBER' AND rt_cat.tag_index = 0
WHERE si.resolved = 0
GROUP BY si.id
ORDER BY
    CASE si.severity
        WHEN 'critical' THEN 0
        WHEN 'error'    THEN 1
        WHEN 'warning'  THEN 2
        WHEN 'info'     THEN 3
        ELSE 4
    END,
    f.path
"""

# Count of open issues grouped by severity
_ISSUES_BY_SEVERITY_SQL = """
SELECT severity, COUNT(*) AS cnt
FROM scan_issues
WHERE resolved = 0
GROUP BY severity
ORDER BY
    CASE severity
        WHEN 'critical' THEN 0
        WHEN 'error'    THEN 1
        WHEN 'warning'  THEN 2
        WHEN 'info'     THEN 3
        ELSE 4
    END
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _album_dir_from_path(file_path: str | None) -> str:
    """Extract the parent directory from a file path string.

    Returns a placeholder if the path is None or has no separator.
    """
    if not file_path:
        return "(no file)"
    idx = file_path.rfind("/")
    if idx < 0:
        idx = file_path.rfind("\\")
    if idx < 0:
        return "(root)"
    return file_path[:idx]


def _fetch_all_issues(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all open issues with album metadata."""
    rows = conn.execute(_ISSUES_WITH_ALBUM_SQL).fetchall()
    result = []
    for r in rows:
        result.append(
            {
                "issue_id": r["id"],
                "file_id": r["file_id"],
                "issue_type": r["issue_type"],
                "severity": r["severity"],
                "description": r["description"],
                "details": r["details"],
                "file_path": r["file_path"],
                "file_filename": r["file_filename"],
                "artist": r["artist"] or "(Unknown Artist)",
                "album": r["album"] or "(Unknown Album)",
                "catalog_number": r["catalog_number"],
            }
        )
    return result


def _filter_by_severity(
    issues: list[dict],
    min_severity: str,
) -> list[dict]:
    """Filter issues to those at or above the given minimum severity."""
    return [i for i in issues if severity_at_least(i["severity"], min_severity)]


def _aggregate_by_album(issues: list[dict]) -> dict[str, dict]:
    """Aggregate issues by album directory.

    Returns a dict mapping album_dir to album metadata + severity counts.
    """
    albums: dict[str, dict] = {}

    for issue in issues:
        album_dir = _album_dir_from_path(issue["file_path"])

        if album_dir not in albums:
            albums[album_dir] = {
                "album_dir": album_dir,
                "artist": issue["artist"],
                "album": issue["album"],
                "catalog_number": issue["catalog_number"],
                "error_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "total_count": 0,
                "file_ids": set(),
            }

        # Count by severity
        severity = issue["severity"]
        if severity in ("critical", "error"):
            albums[album_dir]["error_count"] += 1
        elif severity == "warning":
            albums[album_dir]["warning_count"] += 1
        elif severity == "info":
            albums[album_dir]["info_count"] += 1

        albums[album_dir]["total_count"] += 1
        if issue["file_id"]:
            albums[album_dir]["file_ids"].add(issue["file_id"])

    return albums


def _build_album_summaries(albums_dict: dict[str, dict]) -> list[AlbumIssuesSummary]:
    """Convert album aggregation dict to sorted list of AlbumIssuesSummary."""
    summaries = []
    for album_data in albums_dict.values():
        summaries.append(
            AlbumIssuesSummary(
                artist=album_data["artist"],
                album=album_data["album"],
                catalog_number=album_data["catalog_number"],
                album_dir=album_data["album_dir"],
                error_count=album_data["error_count"],
                warning_count=album_data["warning_count"],
                info_count=album_data["info_count"],
                total_count=album_data["total_count"],
            )
        )

    # Sort by severity: most errors first, then warnings, then total count
    summaries.sort(key=lambda a: (-a.error_count, -a.warning_count, -a.total_count))

    return summaries


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    min_severity: str = "info",
) -> IssuesFullData:
    """Return the complete dataset for ``report issues`` (album overview).

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    min_severity:
        Minimum severity to include: ``'info'``, ``'warning'``, ``'error'``,
        or ``'critical'``. Issues below this threshold are excluded.
    """
    all_issues = _fetch_all_issues(conn)
    filtered = _filter_by_severity(all_issues, min_severity)

    albums_dict = _aggregate_by_album(filtered)
    album_summaries = _build_album_summaries(albums_dict)

    # Count unique files with issues
    all_file_ids = set()
    for album_data in albums_dict.values():
        all_file_ids.update(album_data["file_ids"])

    return IssuesFullData(
        total_albums=len(album_summaries),
        total_files_with_issues=len(all_file_ids),
        total_issues=len(filtered),
        albums=album_summaries,
    )


def album_detail(
    conn: sqlite3.Connection,
    album_dir: str,
) -> AlbumIssuesDetail | None:
    """Return detailed issue information for a specific album.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    album_dir:
        The album directory path (parent directory of the files).

    Returns
    -------
    AlbumIssuesDetail | None
        Detailed issue data for the album, or None if no issues found.
    """
    all_issues = _fetch_all_issues(conn)

    # Filter to issues for this album
    album_issues = [
        i for i in all_issues if _album_dir_from_path(i["file_path"]) == album_dir
    ]

    if not album_issues:
        return None

    # Get album metadata from first issue
    first = album_issues[0]
    artist = first["artist"]
    album = first["album"]
    catalog_number = first["catalog_number"]

    # Count files and severity
    file_ids = set()
    error_count = 0
    warning_count = 0
    info_count = 0

    for issue in album_issues:
        if issue["file_id"]:
            file_ids.add(issue["file_id"])

        severity = issue["severity"]
        if severity in ("critical", "error"):
            error_count += 1
        elif severity == "warning":
            warning_count += 1
        elif severity == "info":
            info_count += 1

    # Count total files in this album (not just files with issues)
    total_files_sql = """
    SELECT COUNT(DISTINCT f.id) AS cnt
    FROM files f
    WHERE f.path LIKE ? || '/%'
    """
    total_files_row = conn.execute(total_files_sql, (album_dir,)).fetchone()
    total_files = total_files_row["cnt"] if total_files_row else 0

    # Build file issue details
    file_issues = []
    for issue in album_issues:
        file_issues.append(
            FileIssueDetail(
                file_path=issue["file_path"] or "(no file)",
                file_name=issue["file_filename"] or "(unknown)",
                issue_type=issue["issue_type"],
                severity=issue["severity"],
                description=issue["description"],
                details=issue["details"],
            )
        )

    # Sort by severity (critical/error first) then by file path
    file_issues.sort(key=lambda i: (severity_order(i.severity), i.file_path))

    return AlbumIssuesDetail(
        artist=artist,
        album=album,
        catalog_number=catalog_number,
        album_dir=album_dir,
        total_files=total_files,
        files_with_issues=len(file_ids),
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        issues=file_issues,
    )


def summary_data(conn: sqlite3.Connection) -> IssuesSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Total albums with issues, total issues, count by severity, top 5 albums.
    """
    all_issues = _fetch_all_issues(conn)

    # Count by severity
    sev_rows = conn.execute(_ISSUES_BY_SEVERITY_SQL).fetchall()
    by_severity: dict[str, int] = {}
    for r in sev_rows:
        by_severity[r["severity"]] = r["cnt"]

    # Aggregate by album
    albums_dict = _aggregate_by_album(all_issues)
    album_summaries = _build_album_summaries(albums_dict)

    # Top 5 albums by issue count
    top_albums = album_summaries[:5]

    return IssuesSummaryData(
        total_albums_with_issues=len(album_summaries),
        total_issues=len(all_issues),
        by_severity=by_severity,
        top_albums=top_albums,
    )


# ---------------------------------------------------------------------------
# Album report (rich detail page)
# ---------------------------------------------------------------------------

_ALBUM_FILES_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    f.filename,
    COALESCE(t_dn.tag_value, '1') AS discnumber,
    COALESCE(t_tn.tag_value, '0') AS tracknumber,
    COALESCE(t_title.tag_value, f.filename) AS title,
    t_aa.tag_value AS albumartist,
    t_al.tag_value AS album,
    t_od.tag_value AS originaldate,
    t_cn.tag_value AS catalognumber
FROM files f
LEFT JOIN raw_tags t_dn ON t_dn.file_id = f.id AND t_dn.tag_key_upper = 'DISCNUMBER' AND t_dn.tag_index = 0
LEFT JOIN raw_tags t_tn ON t_tn.file_id = f.id AND t_tn.tag_key_upper = 'TRACKNUMBER' AND t_tn.tag_index = 0
LEFT JOIN raw_tags t_title ON t_title.file_id = f.id AND t_title.tag_key_upper = 'TITLE' AND t_title.tag_index = 0
LEFT JOIN raw_tags t_aa ON t_aa.file_id = f.id AND t_aa.tag_key_upper = 'ALBUMARTIST' AND t_aa.tag_index = 0
LEFT JOIN raw_tags t_al ON t_al.file_id = f.id AND t_al.tag_key_upper = 'ALBUM' AND t_al.tag_index = 0
LEFT JOIN raw_tags t_od ON t_od.file_id = f.id AND t_od.tag_key_upper = 'ORIGINALDATE' AND t_od.tag_index = 0
LEFT JOIN raw_tags t_cn ON t_cn.file_id = f.id AND t_cn.tag_key_upper = 'CATALOGNUMBER' AND t_cn.tag_index = 0
WHERE f.status = 'ok'
  AND SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) = ?
ORDER BY CAST(t_dn.tag_value AS INTEGER), CAST(t_tn.tag_value AS INTEGER)
"""

_ALBUM_ISSUES_SQL = """
SELECT
    si.file_id,
    si.issue_type,
    si.severity,
    si.description,
    si.details
FROM scan_issues si
WHERE si.resolved = 0
  AND si.file_id IN (
      SELECT f.id FROM files f
      WHERE SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) = ?
  )
ORDER BY si.file_id, si.severity, si.issue_type
"""

_BLOCKER_TYPES = frozenset({"file_unreadable", "tag_read_error"})
_OPTIMIZATION_TYPES = frozenset({"no_audio_md5", "missing_other_tag", "invalid_other_tag"})


def _classify_bucket(issue_type: str) -> str:
    """Classify an issue into a UI bucket."""
    if issue_type in _BLOCKER_TYPES:
        return "blocker"
    if issue_type in _OPTIMIZATION_TYPES:
        return "optimization"
    return "metadata"


def _extract_field(issue_type: str, details_json: str | None) -> str | None:
    """Extract the tag field name from issue details, if applicable."""
    if details_json:
        try:
            d = json.loads(details_json)
            return d.get("tag")
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _parse_int(value: str, default: int = 0) -> int:
    """Parse a string to int, returning default on failure."""
    try:
        return int(value.strip().split("/")[0])  # handle legacy N/T format
    except (ValueError, AttributeError):
        return default


def album_report(
    conn: sqlite3.Connection,
    album_dir: str,
) -> AlbumReportData | None:
    """Return rich album report data for the detail page.

    Returns None if the album directory has no files.
    """
    # Fetch all files in this album
    file_rows = conn.execute(_ALBUM_FILES_SQL, (album_dir,)).fetchall()
    if not file_rows:
        return None

    # Album metadata from first file
    first = file_rows[0]
    artist = first["albumartist"] or "Unknown Artist"
    album = first["album"] or "Unknown Album"
    original_date = first["originaldate"]
    catalog_number = first["catalognumber"]
    total_tracks = len(file_rows)

    # Build file_id → file info map
    file_map: dict[int, dict] = {}
    for r in file_rows:
        file_map[r["file_id"]] = {
            "file_id": r["file_id"],
            "path": r["path"],
            "filename": r["filename"],
            "discnumber": _parse_int(r["discnumber"], 1),
            "tracknumber": _parse_int(r["tracknumber"], 0),
            "title": r["title"] or r["filename"],
        }

    # Fetch all issues for this album
    issue_rows = conn.execute(_ALBUM_ISSUES_SQL, (album_dir,)).fetchall()

    # Group issues by file_id
    issues_by_file: dict[int, list[TrackIssue]] = defaultdict(list)
    album_level_issues: list[AlbumLevelIssue] = []
    critical_count = 0
    error_count = 0
    warning_count = 0
    info_count = 0

    for ir in issue_rows:
        sev = ir["severity"]
        if sev == "critical":
            critical_count += 1
        elif sev == "error":
            error_count += 1
        elif sev == "warning":
            warning_count += 1
        else:
            info_count += 1

        fid = ir["file_id"]
        ti = TrackIssue(
            issue_type=ir["issue_type"],
            severity=sev,
            description=ir["description"],
            field=_extract_field(ir["issue_type"], ir["details"]),
            bucket=_classify_bucket(ir["issue_type"]),
        )

        if fid and fid in file_map:
            issues_by_file[fid].append(ti)
        else:
            album_level_issues.append(AlbumLevelIssue(
                severity=sev,
                description=ir["description"],
            ))

    # ── Album consistency checks ──
    from storeroon.reports.queries.album_consistency import (
        _CONSISTENCY_FIELDS,
        _check_field_consistency,
        _check_track_numbering,
    )

    # Field consistency violations → warnings
    for field_name in _CONSISTENCY_FIELDS:
        violation = _check_field_consistency(conn, album_dir, field_name, total_tracks)
        if violation is not None:
            vals = ", ".join(
                f"'{v}' ({violation.track_counts_per_value.get(v, '?')})"
                for v in violation.distinct_values[:5]
            )
            if len(violation.distinct_values) > 5:
                vals += f" … +{len(violation.distinct_values) - 5} more"
            desc = f"Inconsistent {field_name}: {vals}"
            if violation.null_track_count > 0:
                desc += f" ({violation.null_track_count} tracks missing the tag)"
            album_level_issues.append(AlbumLevelIssue(severity="warning", description=desc))
            warning_count += 1

    # Track numbering violations → critical for missing tracks, warning for others
    numbering_violations = _check_track_numbering(conn, album_dir, total_tracks)
    for nv in numbering_violations:
        if nv.check_type in ("missing_track", "missing_disc"):
            sev = "critical"
            critical_count += 1
        else:
            sev = "warning"
            warning_count += 1
        album_level_issues.append(AlbumLevelIssue(severity=sev, description=nv.description))

    # Build track details
    tracks: list[TrackDetail] = []
    for fid, finfo in file_map.items():
        tracks.append(TrackDetail(
            file_id=fid,
            file_path=finfo["path"],
            file_name=finfo["filename"],
            discnumber=finfo["discnumber"],
            tracknumber=finfo["tracknumber"],
            title=finfo["title"],
            issues=issues_by_file.get(fid, []),
        ))

    tracks.sort(key=lambda t: (t.discnumber, t.tracknumber))

    # Health score: average of per-track scores.
    # - Any critical issue (including album-level) → album score is 0
    # - Per track: starts at 100, any error → 0, each warning → -5 (min 0)
    # - Info issues have no impact
    if critical_count > 0 or total_tracks == 0:
        health = 0
    else:
        track_scores: list[int] = []
        for track in tracks:
            has_error = any(i.severity in ("critical", "error") for i in track.issues)
            if has_error:
                track_scores.append(0)
            else:
                warning_count_track = sum(1 for i in track.issues if i.severity == "warning")
                track_scores.append(max(0, 100 - warning_count_track * 5))
        health = round(sum(track_scores) / len(track_scores)) if track_scores else 100

    return AlbumReportData(
        artist=artist,
        album=album,
        original_date=original_date,
        catalog_number=catalog_number,
        album_dir=album_dir,
        total_tracks=total_tracks,
        health_score=health,
        critical_count=critical_count,
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        album_level_issues=album_level_issues,
        tracks=tracks,
    )
