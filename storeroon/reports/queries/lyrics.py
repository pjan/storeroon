"""
storeroon.reports.queries.lyrics — Lyrics coverage report.

Queries the ``lyrics_analysis`` table (populated at scan time) to build
coverage statistics for embedded lyrics and sidecar .lrc files.

Each file is classified independently on two axes:
    embedded: synced / unsynced / absent
    sidecar:  synced / unsynced / absent

Public API:
    full_data(conn, *, artist_filter=None) -> LyricsFullData
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

_LYRICS_ANALYSIS_SQL = """
SELECT la.file_id, la.embedded, la.sidecar
FROM lyrics_analysis la
JOIN files f ON f.id = la.file_id
WHERE f.status = 'ok'
"""

_FILE_ALBUMARTIST_SQL = """
SELECT rt.file_id, rt.tag_value AS albumartist
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND rt.tag_index = 0
"""

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


def _load_lyrics_analysis(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
) -> tuple[dict[int, str], dict[int, str]]:
    """Load lyrics analysis from the database.

    Returns (embedded_map, sidecar_map) — file_id → 'synced'/'unsynced'/'absent'.
    Files not in lyrics_analysis (e.g. pre-migration data) default to 'absent'.
    """
    embedded: dict[int, str] = {fid: "absent" for fid in ok_file_ids}
    sidecar: dict[int, str] = {fid: "absent" for fid in ok_file_ids}

    for r in conn.execute(_LYRICS_ANALYSIS_SQL).fetchall():
        fid = r["file_id"]
        if fid in ok_file_ids:
            embedded[fid] = r["embedded"]
            sidecar[fid] = r["sidecar"]

    return embedded, sidecar


def _build_overall(
    total_files: int,
    embedded: dict[int, str],
    sidecar: dict[int, str],
) -> LyricsCoverageOverall:
    emb_synced = sum(1 for v in embedded.values() if v == "synced")
    emb_unsynced = sum(1 for v in embedded.values() if v == "unsynced")
    emb_absent = sum(1 for v in embedded.values() if v == "absent")

    sc_synced = sum(1 for v in sidecar.values() if v == "synced")
    sc_unsynced = sum(1 for v in sidecar.values() if v == "unsynced")
    sc_absent = sum(1 for v in sidecar.values() if v == "absent")

    return LyricsCoverageOverall(
        total_files=total_files,
        embedded_synced_count=emb_synced,
        embedded_synced_pct=safe_pct(emb_synced, total_files),
        embedded_unsynced_count=emb_unsynced,
        embedded_unsynced_pct=safe_pct(emb_unsynced, total_files),
        embedded_absent_count=emb_absent,
        embedded_absent_pct=safe_pct(emb_absent, total_files),
        sidecar_synced_count=sc_synced,
        sidecar_synced_pct=safe_pct(sc_synced, total_files),
        sidecar_unsynced_count=sc_unsynced,
        sidecar_unsynced_pct=safe_pct(sc_unsynced, total_files),
        sidecar_absent_count=sc_absent,
        sidecar_absent_pct=safe_pct(sc_absent, total_files),
    )


def _build_by_entity(
    conn: sqlite3.Connection,
    ok_file_ids: set[int],
    embedded: dict[int, str],
    sidecar: dict[int, str],
    group_sql: str,
    group_key: str,
) -> list[LyricsCoverageByEntity]:
    rows = conn.execute(group_sql).fetchall()

    entity_files: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        fid = r["file_id"]
        if fid in ok_file_ids:
            entity_files[r[group_key] or "(unknown)"].add(fid)

    result: list[LyricsCoverageByEntity] = []
    for name, fids in entity_files.items():
        total = len(fids)
        emb_any = sum(1 for fid in fids if embedded.get(fid, "absent") != "absent")
        sc_any = sum(1 for fid in fids if sidecar.get(fid, "absent") != "absent")
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

    result.sort(key=lambda r: (r.embedded_pct, r.name))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
) -> LyricsFullData:
    """Return the complete dataset for ``report lyrics``."""
    total_files = _get_total_ok_files(conn, artist_filter)
    ok_file_ids = _get_all_ok_file_ids(conn, artist_filter)

    embedded, sidecar = _load_lyrics_analysis(conn, ok_file_ids)

    overall = _build_overall(total_files, embedded, sidecar)
    by_artist = _build_by_entity(
        conn, ok_file_ids, embedded, sidecar, _FILE_ALBUMARTIST_SQL, "albumartist"
    )
    by_album = _build_by_entity(
        conn, ok_file_ids, embedded, sidecar, _FILE_ALBUM_DIR_SQL, "album_dir"
    )

    return LyricsFullData(
        total_files=total_files,
        overall=overall,
        by_artist=by_artist,
        by_album=by_album,
    )


def summary_data(conn: sqlite3.Connection) -> LyricsSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    total_files = _get_total_ok_files(conn)
    ok_file_ids = _get_all_ok_file_ids(conn)

    embedded, sidecar = _load_lyrics_analysis(conn, ok_file_ids)

    emb_any = sum(1 for v in embedded.values() if v != "absent")
    sc_any = sum(1 for v in sidecar.values() if v != "absent")

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
            embedded.get(fid, "absent") != "absent" or sidecar.get(fid, "absent") != "absent"
            for fid in fids
        )
        if not has_any:
            artists_zero += 1

    return LyricsSummaryData(
        embedded_any_pct=safe_pct(emb_any, total_files),
        sidecar_any_pct=safe_pct(sc_any, total_files),
        artists_with_zero_lyrics=artists_zero,
    )
