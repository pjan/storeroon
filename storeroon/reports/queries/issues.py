"""
storeroon.reports.queries.issues — Report 9: Scan issues.

Reads from the ``scan_issues`` table populated during Phase 1 import.
All queries filtered by ``resolved = FALSE`` by default; ``--min-severity``
flag filters to that severity and above.

Full report tables:
    1. Issues by severity and type (pivot table)
    2. Most-affected albums (by album directory path)
    3. Most-affected artists (by ALBUMARTIST tag value)
    4. Per-issue-type drill-down (detail rows grouped by issue_type)

Public API:
    full_data(conn, min_severity="info") -> IssuesFullData
    summary_data(conn) -> IssuesSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict

from storeroon.reports.models import (
    AlbumIssueRow,
    ArtistIssueRow,
    IssueDetailRow,
    IssuePivotRow,
    IssuesFullData,
    IssuesSummaryData,
)
from storeroon.reports.utils import severity_at_least, severity_order

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# All open (unresolved) issues, optionally with file path context.
_ALL_OPEN_ISSUES_SQL = """
SELECT
    si.id,
    si.file_id,
    si.issue_type,
    si.severity,
    si.description,
    si.details,
    f.path AS file_path,
    f.filename AS file_filename
FROM scan_issues si
LEFT JOIN files f ON f.id = si.file_id
WHERE si.resolved = 0
ORDER BY
    CASE si.severity
        WHEN 'critical' THEN 0
        WHEN 'error'    THEN 1
        WHEN 'warning'  THEN 2
        WHEN 'info'     THEN 3
        ELSE 4
    END,
    si.issue_type,
    f.path
"""

# Count of open issues grouped by severity.
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

# ALBUMARTIST tag value for a given file_id (first value only).
_ALBUMARTIST_FOR_FILE_SQL = """
SELECT tag_value
FROM raw_tags
WHERE file_id = ?
  AND tag_key_upper = 'ALBUMARTIST'
  AND tag_index = 0
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _album_dir_from_path(file_path: str | None) -> str:
    """Extract the parent directory from a file path string.

    Returns an empty string if the path is None or has no separator.
    """
    if not file_path:
        return "(no file)"
    idx = file_path.rfind("/")
    if idx < 0:
        idx = file_path.rfind("\\")
    if idx < 0:
        return "(root)"
    return file_path[:idx]


def _get_albumartist(conn: sqlite3.Connection, file_id: int | None) -> str:
    """Look up the ALBUMARTIST tag value for a file_id.

    Returns "(unknown)" if not found or file_id is None.
    """
    if file_id is None:
        return "(no file)"
    row = conn.execute(_ALBUMARTIST_FOR_FILE_SQL, (file_id,)).fetchone()
    if row and row[0]:
        return row[0]
    return "(unknown)"


def _fetch_all_open_issues(
    conn: sqlite3.Connection,
) -> list[dict[str, str | int | None]]:
    """Fetch all open issues with file context, returned as plain dicts."""
    rows = conn.execute(_ALL_OPEN_ISSUES_SQL).fetchall()
    result: list[dict[str, str | int | None]] = []
    for r in rows:
        result.append(
            {
                "file_id": r["file_id"],
                "issue_type": r["issue_type"],
                "severity": r["severity"],
                "description": r["description"],
                "details": r["details"],
                "file_path": r["file_path"],
                "file_filename": r["file_filename"],
            }
        )
    return result


def _filter_by_severity(
    issues: list[dict[str, str | int | None]],
    min_severity: str,
) -> list[dict[str, str | int | None]]:
    """Filter issues to those at or above the given minimum severity."""
    return [i for i in issues if severity_at_least(str(i["severity"]), min_severity)]


def _build_pivot(
    issues: list[dict[str, str | int | None]],
) -> list[IssuePivotRow]:
    """Build the severity × issue_type pivot table.

    Returns rows sorted by severity (critical first) then count descending.
    """
    counter: Counter[tuple[str, str]] = Counter()
    for i in issues:
        sev = str(i["severity"])
        itype = str(i["issue_type"])
        counter[(sev, itype)] += 1

    rows: list[IssuePivotRow] = [
        IssuePivotRow(severity=sev, issue_type=itype, count=count)
        for (sev, itype), count in counter.items()
    ]
    rows.sort(key=lambda r: (severity_order(r.severity), -r.count))
    return rows


def _build_by_album(
    issues: list[dict[str, str | int | None]],
) -> list[AlbumIssueRow]:
    """Group issues by album directory path, sorted by count descending."""
    counter: Counter[str] = Counter()
    for i in issues:
        album_dir = _album_dir_from_path(i.get("file_path"))  # type: ignore[arg-type]
        counter[album_dir] += 1

    rows = [
        AlbumIssueRow(album_dir=album_dir, issue_count=count)
        for album_dir, count in counter.most_common()
    ]
    return rows


def _build_by_artist(
    conn: sqlite3.Connection,
    issues: list[dict[str, str | int | None]],
) -> list[ArtistIssueRow]:
    """Group issues by ALBUMARTIST tag value, sorted by count descending."""
    counter: Counter[str] = Counter()
    # Cache artist lookups to avoid repeated queries.
    artist_cache: dict[int | None, str] = {}

    for i in issues:
        file_id: int | None = i.get("file_id")  # type: ignore[assignment]
        if file_id not in artist_cache:
            artist_cache[file_id] = _get_albumartist(conn, file_id)
        artist = artist_cache[file_id]
        counter[artist] += 1

    rows = [
        ArtistIssueRow(artist=artist, issue_count=count)
        for artist, count in counter.most_common()
    ]
    return rows


def _build_by_type(
    issues: list[dict[str, str | int | None]],
) -> dict[str, list[IssueDetailRow]]:
    """Group issues by issue_type, each with full detail rows."""
    grouped: dict[str, list[IssueDetailRow]] = defaultdict(list)
    for i in issues:
        itype = str(i["issue_type"])
        grouped[itype].append(
            IssueDetailRow(
                file_path=i["file_path"] if i["file_path"] else None,  # type: ignore[arg-type]
                issue_type=itype,
                severity=str(i["severity"]),
                description=str(i["description"]),
                details=i["details"] if i["details"] else None,  # type: ignore[arg-type]
            )
        )

    # Sort each group by severity then path.
    for itype in grouped:
        grouped[itype].sort(
            key=lambda r: (severity_order(r.severity), r.file_path or "")
        )

    return dict(grouped)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    min_severity: str = "info",
) -> IssuesFullData:
    """Return the complete dataset for ``report issues``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    min_severity:
        Minimum severity to include: ``'info'``, ``'warning'``, ``'error'``,
        or ``'critical'``. Issues below this threshold are excluded.
    """
    all_issues = _fetch_all_open_issues(conn)
    filtered = _filter_by_severity(all_issues, min_severity)

    total_open = len(filtered)
    pivot = _build_pivot(filtered)
    by_album = _build_by_album(filtered)
    by_artist = _build_by_artist(conn, filtered)
    by_type = _build_by_type(filtered)

    return IssuesFullData(
        total_open=total_open,
        pivot=pivot,
        by_album=by_album,
        by_artist=by_artist,
        by_type=by_type,
    )


def summary_data(conn: sqlite3.Connection) -> IssuesSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Total open issues, count by severity, top 5 issue types by count.
    """
    all_issues = _fetch_all_open_issues(conn)
    total_open = len(all_issues)

    # Count by severity.
    sev_rows = conn.execute(_ISSUES_BY_SEVERITY_SQL).fetchall()
    by_severity: dict[str, int] = {}
    for r in sev_rows:
        by_severity[r["severity"]] = r["cnt"]

    # Top 5 issue types by count (across all severities).
    type_counter: Counter[str] = Counter()
    for i in all_issues:
        itype = str(i["issue_type"])
        sev = str(i["severity"])
        type_counter[itype] += 1

    # Build pivot rows for the top 5 types. We aggregate across severities
    # for the summary — use the most severe severity observed for each type.
    type_severity: dict[str, str] = {}
    for i in all_issues:
        itype = str(i["issue_type"])
        sev = str(i["severity"])
        if itype not in type_severity or severity_order(sev) < severity_order(
            type_severity[itype]
        ):
            type_severity[itype] = sev

    top_types = [
        IssuePivotRow(
            severity=type_severity.get(itype, "info"),
            issue_type=itype,
            count=count,
        )
        for itype, count in type_counter.most_common(5)
    ]

    return IssuesSummaryData(
        total_open=total_open,
        by_severity=by_severity,
        top_issue_types=top_types,
    )
