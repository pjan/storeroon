"""
storeroon.reports.queries.lyrics — Report 12: Lyrics coverage.

Query ``raw_tags`` for ``tag_key_upper = 'LYRICS'`` and separately check for
``tag_key_upper = 'UNSYNCEDLYRICS'`` (an alternate key some taggers use).
Treat either as "has embedded lyrics".

Full report tables:
    1. Overall coverage: files with non-empty LYRICS or UNSYNCEDLYRICS /
       files with the tag but empty value / files with no lyrics tag.
    2. Coverage by artist: for each ALBUMARTIST, count of tracks with lyrics /
       total tracks / %. Sorted by % ascending (worst coverage first).
    3. Coverage by album: same, grouped by album directory path.
    4. Key variant note: count of files using UNSYNCEDLYRICS vs LYRICS.

Public API:
    full_data(conn, artist_filter=None) -> LyricsFullData
    summary_data(conn) -> LyricsSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict

from storeroon.reports.models import (
    LyricsCoverageByEntity,
    LyricsCoverageOverall,
    LyricsFullData,
    LyricsSummaryData,
)
from storeroon.reports.utils import (
    TOTAL_OK_FILES_FILTERED_SQL,
    TOTAL_OK_FILES_SQL,
    safe_pct,
)

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# All ok file IDs.
_ALL_OK_FILE_IDS_SQL = """
SELECT id AS file_id FROM files WHERE status = 'ok'
"""

_ALL_OK_FILE_IDS_FILTERED_SQL = """
SELECT DISTINCT f.id AS file_id
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

# Files that have a LYRICS tag (any value, including empty).
_FILES_WITH_LYRICS_SQL = """
SELECT DISTINCT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'LYRICS'
  AND rt.tag_index = 0
"""

# Files that have an UNSYNCEDLYRICS tag (any value, including empty).
_FILES_WITH_UNSYNCEDLYRICS_SQL = """
SELECT DISTINCT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'UNSYNCEDLYRICS'
  AND rt.tag_index = 0
"""

# ALBUMARTIST for each ok file (first value only).
_FILE_ALBUMARTIST_SQL = """
SELECT rt.file_id, rt.tag_value AS albumartist
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND rt.tag_index = 0
"""

# Album directory for each ok file.
_FILE_ALBUM_DIR_SQL = """
SELECT
    f.id AS file_id,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
WHERE f.status = 'ok'
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_total_ok_files(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> int:
    """Return count of files with status='ok', optionally filtered."""
    if artist_filter:
        row = conn.execute(TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
    else:
        row = conn.execute(TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _get_all_ok_file_ids(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> set[int]:
    """Return the set of all ok file IDs, optionally filtered by artist."""
    if artist_filter:
        rows = conn.execute(_ALL_OK_FILE_IDS_FILTERED_SQL, (artist_filter,)).fetchall()
    else:
        rows = conn.execute(_ALL_OK_FILE_IDS_SQL).fetchall()
    return {r["file_id"] for r in rows}


def _build_lyrics_maps(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
) -> tuple[dict[int, str], dict[int, str]]:
    """Build file_id -> tag_value maps for LYRICS and UNSYNCEDLYRICS.

    Only includes files that are in the ok_file_ids set.

    Returns (lyrics_map, unsyncedlyrics_map).
    """
    lyrics_map: dict[int, str] = {}
    rows = conn.execute(_FILES_WITH_LYRICS_SQL).fetchall()
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            lyrics_map[fid] = r["tag_value"]

    unsynced_map: dict[int, str] = {}
    rows = conn.execute(_FILES_WITH_UNSYNCEDLYRICS_SQL).fetchall()
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            unsynced_map[fid] = r["tag_value"]

    return lyrics_map, unsynced_map


def _classify_lyrics(
    ok_file_ids: set[int],
    lyrics_map: dict[int, str],
    unsynced_map: dict[int, str],
) -> tuple[set[int], set[int], set[int]]:
    """Classify each file into one of three categories:

    1. Has non-empty lyrics (via LYRICS or UNSYNCEDLYRICS).
    2. Has lyrics tag(s) but all are empty/whitespace.
    3. No lyrics tag at all.

    Returns (with_lyrics, empty_lyrics, no_lyrics) — each a set of file_ids.
    """
    with_lyrics: set[int] = set()
    empty_lyrics: set[int] = set()
    no_lyrics: set[int] = set()

    for fid in ok_file_ids:
        lyrics_val = lyrics_map.get(fid)
        unsynced_val = unsynced_map.get(fid)

        has_any_tag = lyrics_val is not None or unsynced_val is not None

        if not has_any_tag:
            no_lyrics.add(fid)
            continue

        # Check if any of the tag values is non-empty.
        has_nonempty = False
        if lyrics_val is not None and lyrics_val.strip():
            has_nonempty = True
        if unsynced_val is not None and unsynced_val.strip():
            has_nonempty = True

        if has_nonempty:
            with_lyrics.add(fid)
        else:
            empty_lyrics.add(fid)

    return with_lyrics, empty_lyrics, no_lyrics


def _build_overall(
    total_files: int,
    with_lyrics: set[int],
    empty_lyrics: set[int],
    no_lyrics: set[int],
    lyrics_map: dict[int, str],
    unsynced_map: dict[int, str],
) -> LyricsCoverageOverall:
    """Build the overall coverage stats."""
    # Count files using each key variant (non-empty only).
    lyrics_key_count = sum(1 for fid, val in lyrics_map.items() if val.strip())
    unsyncedlyrics_key_count = sum(
        1 for fid, val in unsynced_map.items() if val.strip()
    )

    return LyricsCoverageOverall(
        with_lyrics_count=len(with_lyrics),
        with_lyrics_pct=safe_pct(len(with_lyrics), total_files),
        empty_lyrics_count=len(empty_lyrics),
        empty_lyrics_pct=safe_pct(len(empty_lyrics), total_files),
        no_lyrics_count=len(no_lyrics),
        no_lyrics_pct=safe_pct(len(no_lyrics), total_files),
        lyrics_key_count=lyrics_key_count,
        unsyncedlyrics_key_count=unsyncedlyrics_key_count,
    )


def _build_by_artist(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
    with_lyrics: set[int],
) -> list[LyricsCoverageByEntity]:
    """Build lyrics coverage grouped by ALBUMARTIST.

    Returns a list sorted by coverage % ascending (worst coverage first).
    """
    rows = conn.execute(_FILE_ALBUMARTIST_SQL).fetchall()

    # Group file IDs by artist.
    artist_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            artist = r["albumartist"] or "(unknown)"
            artist_files[artist].add(fid)

    result: list[LyricsCoverageByEntity] = []
    for artist, fids in artist_files.items():
        total = len(fids)
        with_count = len(fids & with_lyrics)
        coverage = safe_pct(with_count, total)
        result.append(
            LyricsCoverageByEntity(
                name=artist,
                with_lyrics=with_count,
                total=total,
                coverage_pct=coverage,
            )
        )

    # Sort by coverage % ascending (worst first).
    result.sort(key=lambda r: (r.coverage_pct, r.name))
    return result


def _build_by_album(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
    with_lyrics: set[int],
) -> list[LyricsCoverageByEntity]:
    """Build lyrics coverage grouped by album directory path.

    Returns a list sorted by coverage % ascending (worst coverage first).
    """
    rows = conn.execute(_FILE_ALBUM_DIR_SQL).fetchall()

    # Group file IDs by album directory.
    album_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            album_dir = r["album_dir"] or "(root)"
            album_files[album_dir].add(fid)

    result: list[LyricsCoverageByEntity] = []
    for album_dir, fids in album_files.items():
        total = len(fids)
        with_count = len(fids & with_lyrics)
        coverage = safe_pct(with_count, total)
        result.append(
            LyricsCoverageByEntity(
                name=album_dir,
                with_lyrics=with_count,
                total=total,
                coverage_pct=coverage,
            )
        )

    # Sort by coverage % ascending (worst first).
    result.sort(key=lambda r: (r.coverage_pct, r.name))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
) -> LyricsFullData:
    """Return the complete dataset for ``report lyrics``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    artist_filter:
        Optional case-insensitive ALBUMARTIST substring filter.
    """
    total_files = _get_total_ok_files(conn, artist_filter)
    ok_file_ids = _get_all_ok_file_ids(conn, artist_filter)

    lyrics_map, unsynced_map = _build_lyrics_maps(conn, ok_file_ids)
    with_lyrics, empty_lyrics, no_lyrics = _classify_lyrics(
        ok_file_ids, lyrics_map, unsynced_map
    )

    overall = _build_overall(
        total_files, with_lyrics, empty_lyrics, no_lyrics, lyrics_map, unsynced_map
    )
    by_artist = _build_by_artist(conn, ok_file_ids, with_lyrics)
    by_album = _build_by_album(conn, ok_file_ids, with_lyrics)

    return LyricsFullData(
        total_files=total_files,
        overall=overall,
        by_artist=by_artist,
        by_album=by_album,
    )


def summary_data(conn: sqlite3.Connection) -> LyricsSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Overall coverage %, count of artists with zero lyrics coverage.
    """
    total_files = _get_total_ok_files(conn)
    ok_file_ids = _get_all_ok_file_ids(conn)

    lyrics_map, unsynced_map = _build_lyrics_maps(conn, ok_file_ids)
    with_lyrics, empty_lyrics, no_lyrics = _classify_lyrics(
        ok_file_ids, lyrics_map, unsynced_map
    )

    coverage_pct = safe_pct(len(with_lyrics), total_files)

    # Count artists with zero coverage.
    rows = conn.execute(_FILE_ALBUMARTIST_SQL).fetchall()
    artist_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            artist = r["albumartist"] or "(unknown)"
            artist_files[artist].add(fid)

    artists_zero = 0
    for artist, fids in artist_files.items():
        with_count = len(fids & with_lyrics)
        if with_count == 0:
            artists_zero += 1

    return LyricsSummaryData(
        coverage_pct=coverage_pct,
        artists_with_zero_coverage=artists_zero,
    )
