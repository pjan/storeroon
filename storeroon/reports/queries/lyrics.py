"""
storeroon.reports.queries.lyrics — Lyrics coverage report.

Evaluates lyrics for each track across two dimensions:

1. **Embedded lyrics** (LYRICS / UNSYNCEDLYRICS Vorbis comment tags):
   - ``timed`` — contains LRC-style timestamps ``[MM:SS``
   - ``plain`` — non-empty text without timestamps
   - ``none``  — no tag or empty tag

2. **Sidecar .lrc file** (same name as the FLAC, with ``.lrc`` extension):
   - ``timed`` — file exists and contains timestamps
   - ``plain`` — file exists but no timestamps
   - ``none``  — file does not exist

Public API:
    full_data(conn, *, artist_filter=None, collection_root=None) -> LyricsFullData
    summary_data(conn, *, collection_root=None) -> LyricsSummaryData
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

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

log = logging.getLogger("storeroon.reports")

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

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

_FILES_WITH_LYRICS_SQL = """
SELECT DISTINCT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'LYRICS'
  AND rt.tag_index = 0
"""

_FILES_WITH_UNSYNCEDLYRICS_SQL = """
SELECT DISTINCT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'UNSYNCEDLYRICS'
  AND rt.tag_index = 0
"""

_FILE_ALBUMARTIST_SQL = """
SELECT rt.file_id, rt.tag_value AS albumartist
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND rt.tag_index = 0
"""

_FILE_PATHS_SQL = """
SELECT f.id AS file_id, f.path
FROM files f
WHERE f.status = 'ok'
"""

_FILE_ALBUM_DIR_SQL = """
SELECT
    f.id AS file_id,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
WHERE f.status = 'ok'
"""

# ---------------------------------------------------------------------------
# Timed lyrics detection
# ---------------------------------------------------------------------------

# Matches LRC-style timestamps like [01:23.45] or [1:23.
_RE_LRC_TIMESTAMP = re.compile(r"\[\d{1,2}:\d{2}")


def _is_timed_lyrics(text: str) -> bool:
    """Return True if *text* contains at least one LRC timestamp pattern."""
    return _RE_LRC_TIMESTAMP.search(text) is not None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_total_ok_files(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> int:
    if artist_filter:
        row = conn.execute(TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
    else:
        row = conn.execute(TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _get_all_ok_file_ids(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> set[int]:
    if artist_filter:
        rows = conn.execute(_ALL_OK_FILE_IDS_FILTERED_SQL, (artist_filter,)).fetchall()
    else:
        rows = conn.execute(_ALL_OK_FILE_IDS_SQL).fetchall()
    return {r["file_id"] for r in rows}


def _build_lyrics_maps(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
) -> tuple[dict[int, str], dict[int, str]]:
    """Build file_id -> tag_value maps for LYRICS and UNSYNCEDLYRICS."""
    lyrics_map: dict[int, str] = {}
    for r in conn.execute(_FILES_WITH_LYRICS_SQL).fetchall():
        fid = r["file_id"]
        if fid in ok_file_ids:
            lyrics_map[fid] = r["tag_value"]

    unsynced_map: dict[int, str] = {}
    for r in conn.execute(_FILES_WITH_UNSYNCEDLYRICS_SQL).fetchall():
        fid = r["file_id"]
        if fid in ok_file_ids:
            unsynced_map[fid] = r["tag_value"]

    return lyrics_map, unsynced_map


def _classify_embedded(
    ok_file_ids: set[int],
    lyrics_map: dict[int, str],
    unsynced_map: dict[int, str],
) -> dict[int, str]:
    """Classify each file's embedded lyrics as 'timed', 'plain', or 'none'.

    If both LYRICS and UNSYNCEDLYRICS exist, prefer the timed one.
    """
    result: dict[int, str] = {}

    for fid in ok_file_ids:
        lyrics_val = lyrics_map.get(fid)
        unsynced_val = unsynced_map.get(fid)

        # Collect non-empty values
        candidates: list[str] = []
        if lyrics_val is not None and lyrics_val.strip():
            candidates.append(lyrics_val)
        if unsynced_val is not None and unsynced_val.strip():
            candidates.append(unsynced_val)

        if not candidates:
            result[fid] = "none"
            continue

        # Check if any candidate is timed
        for c in candidates:
            if _is_timed_lyrics(c):
                result[fid] = "timed"
                break
        else:
            result[fid] = "plain"

    return result


def _classify_sidecars(
    ok_file_ids: set[int],
    file_paths: dict[int, str],
    collection_root: Path | None,
) -> dict[int, str]:
    """Classify each file's sidecar .lrc as 'timed', 'plain', or 'none'."""
    result: dict[int, str] = {}

    if collection_root is None:
        for fid in ok_file_ids:
            result[fid] = "none"
        return result

    for fid in ok_file_ids:
        rel_path = file_paths.get(fid)
        if not rel_path:
            result[fid] = "none"
            continue

        # Replace .flac (case-insensitive) with .lrc
        if rel_path.lower().endswith(".flac"):
            lrc_rel = rel_path[:-5] + ".lrc"
        else:
            lrc_rel = rel_path + ".lrc"

        lrc_path = collection_root / lrc_rel
        if not lrc_path.is_file():
            result[fid] = "none"
            continue

        try:
            content = lrc_path.read_text(encoding="utf-8", errors="replace")
            if content.strip() and _is_timed_lyrics(content):
                result[fid] = "timed"
            elif content.strip():
                result[fid] = "plain"
            else:
                result[fid] = "none"
        except OSError:
            result[fid] = "none"

    return result


def _get_file_paths(
    conn: sqlite3.Connection, ok_file_ids: set[int]
) -> dict[int, str]:
    """Return file_id → relative path map."""
    result: dict[int, str] = {}
    for r in conn.execute(_FILE_PATHS_SQL).fetchall():
        fid = r["file_id"]
        if fid in ok_file_ids:
            result[fid] = r["path"]
    return result


def _build_overall(
    total_files: int,
    embedded: dict[int, str],
    sidecars: dict[int, str],
) -> LyricsCoverageOverall:
    """Build overall coverage stats from classification dicts."""
    emb_timed = sum(1 for v in embedded.values() if v == "timed")
    emb_plain = sum(1 for v in embedded.values() if v == "plain")
    emb_none = sum(1 for v in embedded.values() if v == "none")

    sc_timed = sum(1 for v in sidecars.values() if v == "timed")
    sc_plain = sum(1 for v in sidecars.values() if v == "plain")
    sc_none = sum(1 for v in sidecars.values() if v == "none")

    return LyricsCoverageOverall(
        total_files=total_files,
        embedded_timed_count=emb_timed,
        embedded_timed_pct=safe_pct(emb_timed, total_files),
        embedded_plain_count=emb_plain,
        embedded_plain_pct=safe_pct(emb_plain, total_files),
        embedded_none_count=emb_none,
        embedded_none_pct=safe_pct(emb_none, total_files),
        sidecar_timed_count=sc_timed,
        sidecar_timed_pct=safe_pct(sc_timed, total_files),
        sidecar_plain_count=sc_plain,
        sidecar_plain_pct=safe_pct(sc_plain, total_files),
        sidecar_none_count=sc_none,
        sidecar_none_pct=safe_pct(sc_none, total_files),
    )


def _build_by_entity(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
    embedded: dict[int, str],
    sidecars: dict[int, str],
    group_sql: str,
    group_key: str,
) -> list[LyricsCoverageByEntity]:
    """Build lyrics coverage grouped by a key (artist or album_dir)."""
    rows = conn.execute(group_sql).fetchall()

    entity_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            entity_files[r[group_key] or "(unknown)"].add(fid)

    result: list[LyricsCoverageByEntity] = []
    for name, fids in entity_files.items():
        total = len(fids)
        emb_any = sum(1 for fid in fids if embedded.get(fid, "none") != "none")
        sc_any = sum(1 for fid in fids if sidecars.get(fid, "none") != "none")
        result.append(
            LyricsCoverageByEntity(
                name=name,
                embedded_any=emb_any,
                sidecar_any=sc_any,
                total=total,
                embedded_pct=safe_pct(emb_any, total),
                sidecar_pct=safe_pct(sc_any, total),
            )
        )

    # Sort by embedded coverage ascending (worst first)
    result.sort(key=lambda r: (r.embedded_pct, r.name))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
    collection_root: Path | None = None,
) -> LyricsFullData:
    """Return the complete dataset for ``report lyrics``."""
    total_files = _get_total_ok_files(conn, artist_filter)
    ok_file_ids = _get_all_ok_file_ids(conn, artist_filter)

    lyrics_map, unsynced_map = _build_lyrics_maps(conn, ok_file_ids)
    embedded = _classify_embedded(ok_file_ids, lyrics_map, unsynced_map)

    file_paths = _get_file_paths(conn, ok_file_ids)
    sidecars = _classify_sidecars(ok_file_ids, file_paths, collection_root)

    overall = _build_overall(total_files, embedded, sidecars)
    by_artist = _build_by_entity(
        conn, ok_file_ids, embedded, sidecars, _FILE_ALBUMARTIST_SQL, "albumartist"
    )
    by_album = _build_by_entity(
        conn, ok_file_ids, embedded, sidecars, _FILE_ALBUM_DIR_SQL, "album_dir"
    )

    return LyricsFullData(
        total_files=total_files,
        overall=overall,
        by_artist=by_artist,
        by_album=by_album,
    )


def summary_data(
    conn: sqlite3.Connection,
    *,
    collection_root: Path | None = None,
) -> LyricsSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    total_files = _get_total_ok_files(conn)
    ok_file_ids = _get_all_ok_file_ids(conn)

    lyrics_map, unsynced_map = _build_lyrics_maps(conn, ok_file_ids)
    embedded = _classify_embedded(ok_file_ids, lyrics_map, unsynced_map)

    file_paths = _get_file_paths(conn, ok_file_ids)
    sidecars = _classify_sidecars(ok_file_ids, file_paths, collection_root)

    emb_any = sum(1 for v in embedded.values() if v != "none")
    sc_any = sum(1 for v in sidecars.values() if v != "none")

    # Count artists with zero coverage (neither embedded nor sidecar).
    rows = conn.execute(_FILE_ALBUMARTIST_SQL).fetchall()
    artist_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            artist_files[r["albumartist"] or "(unknown)"].add(fid)

    artists_zero = 0
    for artist, fids in artist_files.items():
        has_any = any(
            embedded.get(fid, "none") != "none" or sidecars.get(fid, "none") != "none"
            for fid in fids
        )
        if not has_any:
            artists_zero += 1

    return LyricsSummaryData(
        embedded_any_pct=safe_pct(emb_any, total_files),
        sidecar_any_pct=safe_pct(sc_any, total_files),
        artists_with_zero_lyrics=artists_zero,
    )
