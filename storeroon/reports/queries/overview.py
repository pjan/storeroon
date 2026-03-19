"""
storeroon.reports.queries.overview — Report 1: Collection overview.

Aggregate queries against ``files``, ``flac_properties``, and ``raw_tags``.
Groups by release type directory component (second path segment).

Public API:
    full_data(conn) -> OverviewFullData
    summary_data(conn) -> OverviewSummaryData
"""

from __future__ import annotations

import sqlite3

from storeroon.reports.models import (
    DistributionSummary,
    OverviewFullData,
    OverviewSummaryData,
    OverviewTotals,
    ReleaseTypeBreakdown,
)
from storeroon.reports.utils import median, safe_div

# ---------------------------------------------------------------------------
# SQL fragments
# ---------------------------------------------------------------------------

# Extract the release type (second path segment, e.g. "Albums", "EPs").
# Paths in the DB use forward slashes: "Artist/Albums/…/01 Track.flac"
_RELEASE_TYPE_EXPR = """
    CASE
        WHEN INSTR(path, '/') > 0
        THEN SUBSTR(
            path,
            INSTR(path, '/') + 1,
            CASE
                WHEN INSTR(SUBSTR(path, INSTR(path, '/') + 1), '/') > 0
                THEN INSTR(SUBSTR(path, INSTR(path, '/') + 1), '/') - 1
                ELSE LENGTH(path)
            END
        )
        ELSE 'Unknown'
    END
"""

# Album directory: everything up to (but not including) the filename.
_ALBUM_DIR_EXPR = """
    SUBSTR(path, 1, LENGTH(path) - LENGTH(filename) - 1)
"""

# ---------------------------------------------------------------------------
# Top-level totals
# ---------------------------------------------------------------------------

_TOTALS_SQL = """
SELECT
    COUNT(*)                                          AS total_tracks,
    COUNT(DISTINCT aa.albumartist)                    AS total_artists,
    COUNT(DISTINCT aa.albumartist || '|||' || aa.album) AS total_albums,
    COALESCE(SUM(fp.duration_seconds), 0.0)           AS total_duration,
    COALESCE(SUM(f.size_bytes), 0)                    AS total_size
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
LEFT JOIN (
    SELECT
        rt1.file_id,
        MAX(CASE WHEN rt1.tag_key_upper = 'ALBUMARTIST' THEN rt1.tag_value END) AS albumartist,
        MAX(CASE WHEN rt1.tag_key_upper = 'ALBUM'       THEN rt1.tag_value END) AS album
    FROM raw_tags rt1
    WHERE rt1.tag_key_upper IN ('ALBUMARTIST', 'ALBUM')
    GROUP BY rt1.file_id
) aa ON aa.file_id = f.id
WHERE f.status = 'ok'
"""


def _query_totals(conn: sqlite3.Connection) -> OverviewTotals:
    row = conn.execute(_TOTALS_SQL).fetchone()
    return OverviewTotals(
        total_tracks=row["total_tracks"],
        total_artists=row["total_artists"],
        total_albums=row["total_albums"],
        total_duration_seconds=row["total_duration"] or 0.0,
        total_size_bytes=row["total_size"] or 0,
    )


# ---------------------------------------------------------------------------
# Breakdown by release type
# ---------------------------------------------------------------------------

_BY_RELEASE_TYPE_SQL = f"""
SELECT
    {_RELEASE_TYPE_EXPR}                              AS release_type,
    COUNT(*)                                          AS track_count,
    COUNT(DISTINCT {_ALBUM_DIR_EXPR})                 AS album_count,
    COALESCE(SUM(f.size_bytes), 0)                    AS total_size,
    COALESCE(SUM(fp.duration_seconds), 0.0)           AS total_duration,
    COALESCE(AVG(fp.duration_seconds), 0.0)           AS avg_track_duration
FROM files f
LEFT JOIN flac_properties fp ON fp.file_id = f.id
WHERE f.status = 'ok'
GROUP BY release_type
ORDER BY track_count DESC
"""


def _query_by_release_type(conn: sqlite3.Connection) -> list[ReleaseTypeBreakdown]:
    rows = conn.execute(_BY_RELEASE_TYPE_SQL).fetchall()
    result: list[ReleaseTypeBreakdown] = []
    for r in rows:
        track_count = r["track_count"]
        album_count = r["album_count"]
        total_duration = r["total_duration"] or 0.0
        avg_album_dur = safe_div(total_duration, album_count)
        avg_track_dur = r["avg_track_duration"] or 0.0
        result.append(
            ReleaseTypeBreakdown(
                release_type=r["release_type"] or "Unknown",
                track_count=track_count,
                album_count=album_count,
                total_size_bytes=r["total_size"] or 0,
                total_duration_seconds=total_duration,
                avg_album_duration_seconds=avg_album_dur,
                avg_track_duration_seconds=avg_track_dur,
            )
        )
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
    by_type = _query_by_release_type(conn)
    distribution = _query_distribution(conn)
    return OverviewFullData(
        totals=totals,
        by_release_type=by_type,
        distribution=distribution,
    )


def summary_data(conn: sqlite3.Connection) -> OverviewSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    totals = _query_totals(conn)
    return OverviewSummaryData(totals=totals)
