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

from storeroon.reports.models import (
    AlbumIssuesDetail,
    AlbumIssuesSummary,
    FileIssueDetail,
    IssuesFullData,
    IssuesSummaryData,
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
