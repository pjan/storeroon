"""
storeroon.reports.queries.collection_issues — Collection issues overview.

Aggregates all issues across the collection into coverage bars:
    1. Album health — % of albums without each album-level issue type
    2. Track health — % of files without each non-tag issue type
    3. Tag quality — valid/invalid/misencoded/missing per tag key

Public API:
    full_data(conn, tags_config) -> CollectionIssuesFullData
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from typing import Callable

from storeroon.config import TagsConfig
from storeroon.reports.models import (
    AlbumHealthBar,
    AliasUsageRow,
    CollectionIssuesFullData,
    TagBar,
    TrackHealthBar,
)
from storeroon.reports.utils import safe_pct

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_TOTAL_OK_FILES_SQL = "SELECT COUNT(*) FROM files WHERE status = 'ok'"

_ALL_ALBUM_DIRS_SQL = """
SELECT DISTINCT SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir,
       COUNT(*) AS file_count
FROM files f
WHERE f.status = 'ok'
GROUP BY album_dir
"""

# Non-tag issues per file
_TRACK_ISSUES_SQL = """
SELECT si.issue_type, COUNT(DISTINCT si.file_id) AS file_count
FROM scan_issues si
JOIN files f ON f.id = si.file_id
WHERE si.resolved = 0
  AND f.status = 'ok'
  AND si.issue_type IN ('file_unreadable', 'no_audio_md5', 'tag_read_error')
GROUP BY si.issue_type
"""

# Tag values for format validation (one row per file per tag)
_TAG_VALUES_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

_FILES_WITH_TAG_SQL = """
SELECT DISTINCT rt.file_id
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

# Encoding suspect issues per tag key
_ENCODING_SUSPECT_SQL = """
SELECT
    json_extract(si.details, '$.tag') AS tag_key,
    COUNT(DISTINCT si.file_id) AS file_count
FROM scan_issues si
JOIN files f ON f.id = si.file_id
WHERE si.resolved = 0
  AND f.status = 'ok'
  AND si.issue_type = 'tag_encoding_suspect'
  AND si.details IS NOT NULL
GROUP BY tag_key
"""

# Alias consistency: for files that have either the canonical or alias key,
# return both values so we can check consistency.
_ALIAS_CONSISTENCY_SQL = """
SELECT
    f.id AS file_id,
    rt_canon.tag_value AS canonical_value,
    rt_alias.tag_value AS alias_value
FROM files f
LEFT JOIN raw_tags rt_canon
    ON rt_canon.file_id = f.id
    AND rt_canon.tag_key_upper = ?
    AND rt_canon.tag_index = 0
    AND TRIM(rt_canon.tag_value) != ''
LEFT JOIN raw_tags rt_alias
    ON rt_alias.file_id = f.id
    AND rt_alias.tag_key_upper = ?
    AND rt_alias.tag_index = 0
    AND TRIM(rt_alias.tag_value) != ''
WHERE f.status = 'ok'
  AND (rt_canon.tag_value IS NOT NULL OR rt_alias.tag_value IS NOT NULL)
"""

# ---------------------------------------------------------------------------
# Tag format validators
# ---------------------------------------------------------------------------

from storeroon.reports.utils import (
    RE_ISRC_HYPHEN,
    RE_POSITIVE_INT,
    RE_TRACKNUMBER_LEGACY,
    is_valid_date,
    is_valid_discogs_id,
    is_valid_isrc,
    is_valid_uuid,
)

_DATE_TAGS = frozenset({"DATE", "ORIGINALDATE"})
_POSITIVE_INT_TAGS = frozenset({"TRACKTOTAL", "DISCNUMBER", "DISCTOTAL", "TOTALDISCS"})
_UUID_TAGS = frozenset({
    "MUSICBRAINZ_TRACKID", "MUSICBRAINZ_RELEASETRACKID", "MUSICBRAINZ_ALBUMID",
    "MUSICBRAINZ_ARTISTID", "MUSICBRAINZ_ALBUMARTISTID", "MUSICBRAINZ_RELEASEGROUPID",
})
_DISCOGS_ID_TAGS = frozenset({
    "DISCOGS_RELEASE_ID", "DISCOGS_ARTIST_ID", "DISCOGS_MASTER_ID", "DISCOGS_LABEL_ID",
})


def _get_validator(tag_key: str) -> Callable[[str], bool] | None:
    """Return the format validator for a tag key, or None if no validation."""
    key = tag_key.upper()
    if key in _DATE_TAGS:
        return is_valid_date
    if key == "TRACKNUMBER":
        def _validate_tn(v: str) -> bool:
            v = v.strip()
            if RE_TRACKNUMBER_LEGACY.match(v):
                m = RE_TRACKNUMBER_LEGACY.match(v)
                return m is not None and int(m.group(1)) > 0
            if RE_POSITIVE_INT.match(v):
                return int(v) > 0
            return False
        return _validate_tn
    if key in _POSITIVE_INT_TAGS:
        def _validate_int(v: str) -> bool:
            v = v.strip()
            return bool(RE_POSITIVE_INT.match(v)) and int(v) > 0
        return _validate_int
    if key == "ISRC":
        def _validate_isrc(v: str) -> bool:
            v = v.strip()
            if not is_valid_isrc(v):
                return False
            clean = v.replace("-", "")
            return not (len(clean) == 12 and clean[-5:] == "00000")
        return _validate_isrc
    if key in _UUID_TAGS:
        return is_valid_uuid
    if key in _DISCOGS_ID_TAGS:
        return is_valid_discogs_id
    return None


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _alias_usage(
    conn: sqlite3.Connection,
    aliases: dict[str, str],
    canonical_keys: frozenset[str],
) -> list[AliasUsageRow]:
    """Check value consistency for alias pairs where the canonical key
    is in the required or recommended config lists.

    For each qualifying alias pair, looks at all files that have the
    canonical key set. Of those, what percentage also have the alias
    key set to the exact same value?
    """
    result: list[AliasUsageRow] = []
    for alias_key, canonical_key in sorted(aliases.items()):
        if canonical_key not in canonical_keys:
            continue

        rows = conn.execute(
            _ALIAS_CONSISTENCY_SQL, (canonical_key, alias_key)
        ).fetchall()

        files_with_canonical = [
            r for r in rows if r["canonical_value"] is not None
        ]
        if not files_with_canonical:
            continue

        total = len(files_with_canonical)
        consistent = sum(
            1 for r in files_with_canonical
            if r["alias_value"] is not None
            and r["canonical_value"].strip() == r["alias_value"].strip()
        )

        result.append(
            AliasUsageRow(
                canonical_key=canonical_key,
                alias_key=alias_key,
                consistency_pct=safe_pct(consistent, total),
            )
        )
    return result


def _build_album_health(
    conn: sqlite3.Connection,
    aliases: dict[str, str] | None = None,
    canonical_keys: frozenset[str] | None = None,
) -> tuple[list[AlbumHealthBar], int]:
    """Run album consistency checks across all albums and count affected albums per issue type."""
    from storeroon.reports.queries.album_consistency import (
        _CONSISTENCY_FIELDS,
        _check_field_consistency,
        _check_track_numbering,
    )

    album_rows = conn.execute(_ALL_ALBUM_DIRS_SQL).fetchall()
    total_albums = len(album_rows)

    # Build album_dir → file_ids map for alias checks
    album_file_ids: dict[str, list[int]] = defaultdict(list)
    for row in album_rows:
        # Get file IDs per album
        fid_rows = conn.execute(
            "SELECT id FROM files WHERE status = 'ok' AND SUBSTR(path, 1, LENGTH(path) - LENGTH(filename) - 1) = ?",
            (row["album_dir"],),
        ).fetchall()
        album_file_ids[row["album_dir"]] = [r["id"] for r in fid_rows]

    # Count albums affected per issue type
    issue_counts: Counter[str] = Counter()

    for row in album_rows:
        album_dir = row["album_dir"]
        file_count = row["file_count"]
        album_issues_found: set[str] = set()

        # Field consistency
        for field_name in _CONSISTENCY_FIELDS:
            violation = _check_field_consistency(conn, album_dir, field_name, file_count)
            if violation is not None:
                album_issues_found.add(f"Inconsistent {field_name}")

        # Track numbering
        violations = _check_track_numbering(conn, album_dir, file_count)
        for nv in violations:
            label_map = {
                "missing_track": "Missing tracks",
                "missing_disc": "Missing discs",
                "duplicate_track": "Duplicate track numbers",
                "totaltracks_mismatch": "Track count mismatch",
                "exceeds_total": "Track number exceeds total",
            }
            album_issues_found.add(label_map.get(nv.check_type, nv.check_type))

        for issue_label in album_issues_found:
            issue_counts[issue_label] += 1

    # Alias consistency: count albums where any file has a mismatch
    _aliases = aliases or {}
    _canonical = canonical_keys or frozenset()
    relevant_pairs: list[tuple[str, str]] = []
    all_alias_keys: set[str] = set()
    for alias_key, canon_key in _aliases.items():
        if canon_key in _canonical:
            relevant_pairs.append((alias_key, canon_key))
            all_alias_keys.add(alias_key)
            all_alias_keys.add(canon_key)

    if relevant_pairs:
        # Fetch all relevant tags in one query
        key_placeholders = ",".join(f"'{k}'" for k in all_alias_keys)
        alias_sql = f"""
        SELECT rt.file_id, rt.tag_key_upper, rt.tag_value
        FROM raw_tags rt
        JOIN files f ON f.id = rt.file_id
        WHERE f.status = 'ok' AND rt.tag_index = 0
          AND rt.tag_key_upper IN ({key_placeholders})
        """
        alias_rows = conn.execute(alias_sql).fetchall()
        file_tags: dict[int, dict[str, str]] = defaultdict(dict)
        for ar in alias_rows:
            file_tags[ar["file_id"]][ar["tag_key_upper"]] = ar["tag_value"]

        # Check per album
        for adir, fids in album_file_ids.items():
            has_mismatch = False
            for fid in fids:
                if has_mismatch:
                    break
                tags = file_tags.get(fid, {})
                for alias_key, canon_key in relevant_pairs:
                    canon_val = tags.get(canon_key)
                    if not canon_val or not canon_val.strip():
                        continue
                    alias_val = tags.get(alias_key)
                    if not alias_val or not alias_val.strip() or canon_val.strip() != alias_val.strip():
                        has_mismatch = True
                        break
            if has_mismatch:
                issue_counts["Alias mismatch"] += 1

    bars: list[AlbumHealthBar] = []
    for label, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
        bars.append(AlbumHealthBar(
            issue_label=label,
            albums_affected=count,
            total_albums=total_albums,
            clean_pct=safe_pct(total_albums - count, total_albums),
        ))

    # Add bars for issue types with zero occurrences (all clean)
    all_possible = (
        ["Missing tracks", "Missing discs", "Duplicate track numbers",
         "Track count mismatch", "Track number exceeds total", "Alias mismatch"]
        + [f"Inconsistent {f}" for f in _CONSISTENCY_FIELDS]
    )
    seen = {b.issue_label for b in bars}
    for label in all_possible:
        if label not in seen:
            bars.append(AlbumHealthBar(
                issue_label=label,
                albums_affected=0,
                total_albums=total_albums,
                clean_pct=100.0,
            ))

    # Sort: worst coverage first
    bars.sort(key=lambda b: b.clean_pct)

    return bars, total_albums


def _build_track_health(conn: sqlite3.Connection, total_files: int) -> list[TrackHealthBar]:
    """Count non-tag issues per file."""
    rows = conn.execute(_TRACK_ISSUES_SQL).fetchall()
    counts = {r["issue_type"]: r["file_count"] for r in rows}

    labels = {
        "file_unreadable": "File readable",
        "no_audio_md5": "Audio MD5 present",
        "tag_read_error": "Tags readable",
    }

    bars: list[TrackHealthBar] = []
    for issue_type, label in labels.items():
        affected = counts.get(issue_type, 0)
        bars.append(TrackHealthBar(
            issue_label=label,
            files_affected=affected,
            total_files=total_files,
            clean_pct=safe_pct(total_files - affected, total_files),
        ))

    bars.sort(key=lambda b: b.clean_pct)
    return bars


def _build_tag_bars(
    conn: sqlite3.Connection,
    tags: tuple[str, ...],
    total_files: int,
    encoding_suspect_counts: dict[str, int],
) -> list[TagBar]:
    """Build tag quality bars for a group of tags."""
    bars: list[TagBar] = []

    for tag_key in tags:
        # Get files with this tag
        files_with_tag = {r[0] for r in conn.execute(_FILES_WITH_TAG_SQL, (tag_key,)).fetchall()}
        missing = total_files - len(files_with_tag)
        if missing < 0:
            missing = 0

        validator = _get_validator(tag_key)

        if validator is not None:
            # Validate each file's value
            rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()
            valid = 0
            invalid = 0
            seen: set[int] = set()
            for r in rows:
                fid = r["file_id"]
                if fid in seen:
                    continue
                seen.add(fid)
                value = r["tag_value"]
                if not value or not value.strip():
                    continue
                if validator(value):
                    valid += 1
                else:
                    invalid += 1
        else:
            # No validator — all present non-empty values are "valid"
            valid = len(files_with_tag)
            invalid = 0

        misencoded = encoding_suspect_counts.get(tag_key, 0)

        bars.append(TagBar(
            tag_key=tag_key,
            valid_count=valid,
            valid_pct=safe_pct(valid, total_files),
            invalid_count=invalid,
            invalid_pct=safe_pct(invalid, total_files),
            misencoded_count=misencoded,
            misencoded_pct=safe_pct(misencoded, total_files),
            missing_count=missing,
            missing_pct=safe_pct(missing, total_files),
        ))

    # Sort: worst coverage first (by valid_pct), then by config order (stable sort)
    bars.sort(key=lambda b: b.valid_pct)

    return bars


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
) -> CollectionIssuesFullData:
    """Return the collection issues overview."""
    total_files_row = conn.execute(_TOTAL_OK_FILES_SQL).fetchone()
    total_files = total_files_row[0] if total_files_row else 0

    # Album health (including alias mismatch check)
    canonical_keys = frozenset(tags_config.required + tags_config.recommended)
    album_health, total_albums = _build_album_health(
        conn, aliases=tags_config.aliases, canonical_keys=canonical_keys,
    )

    # Track health
    track_health = _build_track_health(conn, total_files)

    # Encoding suspect counts per tag key
    encoding_rows = conn.execute(_ENCODING_SUSPECT_SQL).fetchall()
    encoding_counts: dict[str, int] = {}
    for r in encoding_rows:
        key = r["tag_key"]
        if key:
            encoding_counts[key.upper()] = r["file_count"]

    # Tag bars per config group + alias keys in recommended
    required_tags = _build_tag_bars(conn, tags_config.required, total_files, encoding_counts)

    # Add alias keys to recommended tags (aliases whose canonical is in required or recommended)
    alias_tag_keys = tuple(
        alias_key for alias_key, canon_key in tags_config.aliases.items()
        if canon_key in canonical_keys
    )
    recommended_plus_aliases = tags_config.recommended + alias_tag_keys
    recommended_tags = _build_tag_bars(conn, recommended_plus_aliases, total_files, encoding_counts)

    other_tags = _build_tag_bars(conn, tags_config.other, total_files, encoding_counts)

    # Alias consistency
    alias_consistency = _alias_usage(conn, tags_config.aliases, canonical_keys)

    return CollectionIssuesFullData(
        total_albums=total_albums,
        total_files=total_files,
        album_health=album_health,
        track_health=track_health,
        required_tags=required_tags,
        recommended_tags=recommended_tags,
        other_tags=other_tags,
        alias_consistency=alias_consistency,
    )
