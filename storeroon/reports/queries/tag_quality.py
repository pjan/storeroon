"""
storeroon.reports.queries.tag_quality — Tag quality and integrity.

Combines format validation and external ID coverage into a single report.

Sections:
    1. Date Fields (DATE, ORIGINALDATE)
    2. Track & Disc Numbers (TRACKNUMBER, DISCNUMBER, TOTALDISCS)
    3. ISRCs (ISRC)
    4. MusicBrainz IDs (coverage, partial albums, duplicates, backfill)
    5. Discogs IDs (coverage, partial albums, duplicates, backfill)

Public API:
    full_data(conn, artist_filter=None) -> TagQualityFullData
    summary_data(conn) -> TagQualitySummaryData
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from collections.abc import Callable

from storeroon.config import TagsConfig
from storeroon.reports.models import (
    BackfillCandidate,
    DateQualityRow,
    DuplicateIdEntry,
    FieldFormatSection,
    FieldValidationRow,
    IdCoverageRow,
    IdSectionData,
    InvalidValueRow,
    PartialAlbumCoverage,
    TagGroupQuality,
    TagQualityFullData,
    TagQualitySummaryData,
)
from storeroon.reports.utils import (
    RE_ISRC_HYPHEN,
    RE_POSITIVE_INT,
    RE_TRACKNUMBER_LEGACY,
    date_precision,
    is_valid_date,
    is_valid_discogs_id,
    is_valid_isrc,
    is_valid_uuid,
    safe_pct,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MB_KEYS: tuple[str, ...] = (
    "MUSICBRAINZ_TRACKID",
    "MUSICBRAINZ_RELEASETRACKID",
    "MUSICBRAINZ_ALBUMID",
    "MUSICBRAINZ_ARTISTID",
    "MUSICBRAINZ_ALBUMARTISTID",
    "MUSICBRAINZ_RELEASEGROUPID",
)

_DISCOGS_KEYS: tuple[str, ...] = (
    "DISCOGS_RELEASE_ID",
    "DISCOGS_ARTIST_ID",
    "DISCOGS_MASTER_ID",
    "DISCOGS_LABEL_ID",
)

_MB_ALBUM_KEY = "MUSICBRAINZ_ALBUMID"
_MB_BACKFILL_TARGET = "MUSICBRAINZ_RELEASEGROUPID"
_MB_TRACK_KEY = "MUSICBRAINZ_TRACKID"

_DISCOGS_ALBUM_KEY = "DISCOGS_RELEASE_ID"
_DISCOGS_BACKFILL_TARGET = "DISCOGS_MASTER_ID"

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_TOTAL_OK_FILES_SQL = """
SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'
"""

_TOTAL_OK_FILES_FILTERED_SQL = """
SELECT COUNT(DISTINCT f.id) AS cnt
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

_TAG_VALUES_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

_TAG_VALUES_FILTERED_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.file_id IN (
      SELECT DISTINCT rt2.file_id
      FROM raw_tags rt2
      WHERE rt2.tag_key_upper = 'ALBUMARTIST'
        AND LOWER(rt2.tag_value) LIKE '%' || LOWER(?) || '%'
  )
"""

_TAG_VALUES_INDEX0_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
"""

_TAG_VALUES_INDEX0_FILTERED_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
  AND rt.file_id IN (
      SELECT DISTINCT rt2.file_id
      FROM raw_tags rt2
      WHERE rt2.tag_key_upper = 'ALBUMARTIST'
        AND LOWER(rt2.tag_value) LIKE '%' || LOWER(?) || '%'
  )
"""

_FILES_WITH_TAG_SQL = """
SELECT DISTINCT rt.file_id
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

_FILES_WITH_TAG_FILTERED_SQL = """
SELECT DISTINCT rt.file_id
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.file_id IN (
      SELECT DISTINCT rt2.file_id
      FROM raw_tags rt2
      WHERE rt2.tag_key_upper = 'ALBUMARTIST'
        AND LOWER(rt2.tag_value) LIKE '%' || LOWER(?) || '%'
  )
"""

_DISC_CROSS_CHECK_SQL = """
SELECT
    rt_dn.file_id,
    rt_dn.tag_value AS discnumber,
    rt_td.tag_value AS totaldiscs
FROM raw_tags rt_dn
JOIN files f ON f.id = rt_dn.file_id
LEFT JOIN raw_tags rt_td
    ON rt_td.file_id = rt_dn.file_id
   AND rt_td.tag_key_upper = 'TOTALDISCS'
   AND rt_td.tag_index = 0
WHERE f.status = 'ok'
  AND rt_dn.tag_key_upper = 'DISCNUMBER'
  AND rt_dn.tag_index = 0
"""

_FILES_WITH_ALBUM_DIR_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
WHERE f.status = 'ok'
"""

_FILES_WITH_ALBUM_DIR_FILTERED_SQL = """
SELECT
    f.id AS file_id,
    f.path,
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

_DUPLICATE_TAG_VALUES_SQL = """
SELECT rt.tag_value, COUNT(DISTINCT rt.file_id) AS file_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
HAVING COUNT(DISTINCT rt.file_id) > 1
ORDER BY file_count DESC
"""

_FILES_FOR_TAG_VALUE_SQL = """
SELECT f.id AS file_id, f.path,
       SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
  AND rt.tag_value = ?
  AND rt.tag_index = 0
ORDER BY f.path
"""

# ---------------------------------------------------------------------------
# Helpers — shared
# ---------------------------------------------------------------------------


def _get_total_ok_files(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> int:
    if artist_filter:
        row = conn.execute(_TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
    else:
        row = conn.execute(_TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Field format validation (dates, track numbers, ISRCs)
# ---------------------------------------------------------------------------


def _validate_date(value: str) -> bool:
    return is_valid_date(value)


def _validate_tracknumber(value: str) -> bool:
    v = value.strip()
    if RE_TRACKNUMBER_LEGACY.match(v):
        m = RE_TRACKNUMBER_LEGACY.match(v)
        assert m is not None
        return int(m.group(1)) > 0
    if RE_POSITIVE_INT.match(v):
        return int(v) > 0
    return False


def _validate_positive_int(value: str) -> bool:
    v = value.strip()
    if not RE_POSITIVE_INT.match(v):
        return False
    return int(v) > 0


def _validate_isrc(value: str) -> bool:
    v = value.strip()
    if not is_valid_isrc(v):
        return False
    clean = v.replace("-", "")
    if len(clean) == 12 and clean[-5:] == "00000":
        return False
    return True


def _analyse_field(
    conn: sqlite3.Connection,
    tag_key: str,
    total_files: int,
    validator: Callable[[str], bool],
    *,
    artist_filter: str | None = None,
    max_invalid_values: int = 20,
) -> FieldFormatSection:
    if artist_filter:
        rows = conn.execute(_TAG_VALUES_FILTERED_SQL, (tag_key, artist_filter)).fetchall()
        files_with_tag_rows = conn.execute(
            _FILES_WITH_TAG_FILTERED_SQL, (tag_key, artist_filter)
        ).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()
        files_with_tag_rows = conn.execute(_FILES_WITH_TAG_SQL, (tag_key,)).fetchall()

    files_with_tag = {r[0] for r in files_with_tag_rows}
    absent_count = max(total_files - len(files_with_tag), 0)

    valid_count = 0
    invalid_count = 0
    invalid_values: Counter[str] = Counter()
    seen_file_ids: set[int] = set()
    for r in rows:
        fid = r["file_id"]
        if fid in seen_file_ids:
            continue
        seen_file_ids.add(fid)
        value = r["tag_value"]
        if validator(value):
            valid_count += 1
        else:
            invalid_count += 1
            display_val = value if len(value) <= 100 else value[:97] + "..."
            invalid_values[display_val] += 1

    invalid_total = sum(invalid_values.values())
    invalid_list = [
        InvalidValueRow(value=val, count=cnt)
        for val, cnt in invalid_values.most_common(max_invalid_values)
    ]

    summary = FieldValidationRow(
        field_name=tag_key,
        valid_count=valid_count,
        valid_pct=safe_pct(valid_count, total_files),
        invalid_count=invalid_count,
        invalid_pct=safe_pct(invalid_count, total_files),
        absent_count=absent_count,
        absent_pct=safe_pct(absent_count, total_files),
    )

    return FieldFormatSection(
        field_name=tag_key,
        summary=summary,
        invalid_values=invalid_list,
        invalid_values_total=invalid_total,
    )



def _analyse_tracknumber(
    conn: sqlite3.Connection, total_files: int, *, artist_filter: str | None = None
) -> FieldFormatSection:
    section = _analyse_field(conn, "TRACKNUMBER", total_files, _validate_tracknumber, artist_filter=artist_filter)

    if artist_filter:
        rows = conn.execute(_TAG_VALUES_FILTERED_SQL, ("TRACKNUMBER", artist_filter)).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, ("TRACKNUMBER",)).fetchall()

    legacy_count = 0
    exceeds_99_count = 0
    seen: set[int] = set()
    for r in rows:
        fid = r["file_id"]
        if fid in seen:
            continue
        seen.add(fid)
        v = r["tag_value"].strip()
        if RE_TRACKNUMBER_LEGACY.match(v):
            legacy_count += 1
        if RE_POSITIVE_INT.match(v) and int(v) > 99:
            exceeds_99_count += 1

    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if legacy_count > 0:
        extra_invalids.insert(0, InvalidValueRow(value="[Legacy N/T format]", count=legacy_count))
        extra_total += legacy_count
    if exceeds_99_count > 0:
        extra_invalids.insert(0, InvalidValueRow(value="[Track number > 99 (unusual)]", count=exceeds_99_count))
        extra_total += exceeds_99_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


def _analyse_discnumber_cross_check(
    conn: sqlite3.Connection, total_files: int, *, artist_filter: str | None = None
) -> FieldFormatSection:
    section = _analyse_field(conn, "DISCNUMBER", total_files, _validate_positive_int, artist_filter=artist_filter)

    rows = conn.execute(_DISC_CROSS_CHECK_SQL).fetchall()
    exceeds_count = 0
    for r in rows:
        dn_str, td_str = r["discnumber"], r["totaldiscs"]
        if dn_str and td_str:
            dn_s, td_s = dn_str.strip(), td_str.strip()
            if RE_POSITIVE_INT.match(dn_s) and RE_POSITIVE_INT.match(td_s) and int(dn_s) > int(td_s):
                exceeds_count += 1

    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if exceeds_count > 0:
        extra_invalids.insert(0, InvalidValueRow(value="[DISCNUMBER > TOTALDISCS]", count=exceeds_count))
        extra_total += exceeds_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


def _analyse_isrc(
    conn: sqlite3.Connection, total_files: int, *, artist_filter: str | None = None
) -> FieldFormatSection:
    section = _analyse_field(conn, "ISRC", total_files, _validate_isrc, artist_filter=artist_filter)

    if artist_filter:
        rows = conn.execute(_TAG_VALUES_FILTERED_SQL, ("ISRC", artist_filter)).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, ("ISRC",)).fetchall()

    hyphenated_count = 0
    seen: set[int] = set()
    for r in rows:
        fid = r["file_id"]
        if fid in seen:
            continue
        seen.add(fid)
        if RE_ISRC_HYPHEN.match(r["tag_value"].strip()):
            hyphenated_count += 1

    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if hyphenated_count > 0:
        extra_invalids.insert(0, InvalidValueRow(value="[Hyphenated ISRC — needs normalisation]", count=hyphenated_count))
        extra_total += hyphenated_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


# ---------------------------------------------------------------------------
# ID coverage, partial albums, duplicates, backfill
# ---------------------------------------------------------------------------


def _build_file_tag_map(
    conn: sqlite3.Connection, tag_key: str, artist_filter: str | None = None
) -> dict[int, str]:
    if artist_filter:
        rows = conn.execute(_TAG_VALUES_INDEX0_FILTERED_SQL, (tag_key, artist_filter)).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_INDEX0_SQL, (tag_key,)).fetchall()
    return {r["file_id"]: r["tag_value"] for r in rows}


def _build_file_album_dir_map(
    conn: sqlite3.Connection, artist_filter: str | None = None
) -> tuple[dict[int, str], dict[int, str]]:
    if artist_filter:
        rows = conn.execute(_FILES_WITH_ALBUM_DIR_FILTERED_SQL, (artist_filter,)).fetchall()
    else:
        rows = conn.execute(_FILES_WITH_ALBUM_DIR_SQL).fetchall()
    dir_map: dict[int, str] = {}
    path_map: dict[int, str] = {}
    for r in rows:
        dir_map[r["file_id"]] = r["album_dir"]
        path_map[r["file_id"]] = r["path"]
    return dir_map, path_map


def _id_coverage_row(
    tag_key: str, tag_values: dict[int, str], total_files: int, validator: Callable[[str], bool]
) -> IdCoverageRow:
    valid = 0
    malformed = 0
    for value in tag_values.values():
        v = value.strip()
        if not v:
            continue
        if validator(v):
            valid += 1
        else:
            malformed += 1
    absent = max(total_files - valid - malformed, 0)
    return IdCoverageRow(
        tag_key=tag_key,
        valid_count=valid, valid_pct=safe_pct(valid, total_files),
        malformed_count=malformed, malformed_pct=safe_pct(malformed, total_files),
        absent_count=absent, absent_pct=safe_pct(absent, total_files),
    )


def _partial_album_coverage(
    album_key_values: dict[int, str],
    file_album_dirs: dict[int, str],
    all_file_ids: set[int],
    validator: Callable[[str], bool],
) -> list[PartialAlbumCoverage]:
    album_files: dict[str, list[int]] = defaultdict(list)
    for fid in all_file_ids:
        adir = file_album_dirs.get(fid)
        if adir is not None:
            album_files[adir].append(fid)

    results: list[PartialAlbumCoverage] = []
    for album_dir, fids in sorted(album_files.items()):
        with_id = sum(1 for fid in fids if (v := album_key_values.get(fid)) and v.strip() and validator(v.strip()))
        without_id = len(fids) - with_id
        if with_id > 0 and without_id > 0:
            results.append(PartialAlbumCoverage(
                album_dir=album_dir, tracks_with_id=with_id,
                tracks_without_id=without_id, total_tracks=len(fids),
            ))
    results.sort(key=lambda r: r.tracks_without_id, reverse=True)
    return results


def _find_duplicate_ids(conn: sqlite3.Connection, tag_key: str) -> list[DuplicateIdEntry]:
    dup_rows = conn.execute(_DUPLICATE_TAG_VALUES_SQL, (tag_key,)).fetchall()
    results: list[DuplicateIdEntry] = []
    for dr in dup_rows:
        id_value = dr["tag_value"]
        file_rows = conn.execute(_FILES_FOR_TAG_VALUE_SQL, (tag_key, id_value)).fetchall()
        paths = [r["path"] for r in file_rows]
        dirs = {r["album_dir"] for r in file_rows}
        results.append(DuplicateIdEntry(
            id_value=id_value, file_count=dr["file_count"],
            file_paths=paths, same_directory=len(dirs) == 1,
        ))
    return results


def _backfill_candidates(
    source_values: dict[int, str], target_values: dict[int, str],
    source_validator: Callable[[str], bool], description: str,
) -> BackfillCandidate | None:
    affected_tracks = 0
    distinct_source_ids: set[str] = set()
    for fid, source_val in source_values.items():
        sv = source_val.strip()
        if not sv or not source_validator(sv):
            continue
        target_val = target_values.get(fid)
        if not target_val or not target_val.strip():
            affected_tracks += 1
            distinct_source_ids.add(sv.lower())
    if affected_tracks == 0:
        return None
    return BackfillCandidate(
        description=description, affected_tracks=affected_tracks,
        distinct_source_ids=len(distinct_source_ids),
    )


def _build_mb_section(
    conn: sqlite3.Connection, total_files: int,
    file_album_dirs: dict[int, str], all_file_ids: set[int],
    artist_filter: str | None = None,
) -> IdSectionData:
    coverage: list[IdCoverageRow] = []
    tag_maps: dict[str, dict[int, str]] = {}
    for key in _MB_KEYS:
        tag_map = _build_file_tag_map(conn, key, artist_filter)
        tag_maps[key] = tag_map
        coverage.append(_id_coverage_row(key, tag_map, total_files, is_valid_uuid))
    coverage.sort(key=lambda r: r.valid_pct)

    partial = _partial_album_coverage(
        tag_maps.get(_MB_ALBUM_KEY, {}), file_album_dirs, all_file_ids, is_valid_uuid
    )
    duplicates = _find_duplicate_ids(conn, _MB_TRACK_KEY)
    backfill = _backfill_candidates(
        tag_maps.get(_MB_ALBUM_KEY, {}), tag_maps.get(_MB_BACKFILL_TARGET, {}),
        is_valid_uuid,
        f"Tracks with valid {_MB_ALBUM_KEY} but missing {_MB_BACKFILL_TARGET} "
        f"— backfillable via MusicBrainz API with one lookup per unique album ID",
    )
    return IdSectionData(
        source_name="MusicBrainz", coverage=coverage,
        partial_albums=partial, duplicate_ids=duplicates, backfill=backfill,
    )


def _build_discogs_section(
    conn: sqlite3.Connection, total_files: int,
    file_album_dirs: dict[int, str], all_file_ids: set[int],
    artist_filter: str | None = None,
) -> IdSectionData:
    coverage: list[IdCoverageRow] = []
    tag_maps: dict[str, dict[int, str]] = {}
    for key in _DISCOGS_KEYS:
        tag_map = _build_file_tag_map(conn, key, artist_filter)
        tag_maps[key] = tag_map
        coverage.append(_id_coverage_row(key, tag_map, total_files, is_valid_discogs_id))
    coverage.sort(key=lambda r: r.valid_pct)

    partial = _partial_album_coverage(
        tag_maps.get(_DISCOGS_ALBUM_KEY, {}), file_album_dirs, all_file_ids, is_valid_discogs_id
    )
    duplicates = _find_duplicate_ids(conn, _DISCOGS_ALBUM_KEY)
    backfill = _backfill_candidates(
        tag_maps.get(_DISCOGS_ALBUM_KEY, {}), tag_maps.get(_DISCOGS_BACKFILL_TARGET, {}),
        is_valid_discogs_id,
        f"Tracks with valid {_DISCOGS_ALBUM_KEY} but missing {_DISCOGS_BACKFILL_TARGET} "
        f"— backfillable via Discogs API with one lookup per unique release ID",
    )
    return IdSectionData(
        source_name="Discogs", coverage=coverage,
        partial_albums=partial, duplicate_ids=duplicates, backfill=backfill,
    )


def _overall_coverage_pct(
    tag_maps: dict[str, dict[int, str]], keys: tuple[str, ...],
    total_files: int, all_file_ids: set[int], validator: Callable[[str], bool],
) -> float:
    if total_files == 0:
        return 0.0
    fully_covered = 0
    for fid in all_file_ids:
        if all(
            (val := tag_maps.get(key, {}).get(fid)) and val.strip() and validator(val.strip())
            for key in keys
        ):
            fully_covered += 1
    return safe_pct(fully_covered, total_files)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Specialised analysers per tag type
# ---------------------------------------------------------------------------

# Tags with specific format validators — only these appear in the quality report.
_DATE_TAGS = frozenset({"DATE", "ORIGINALDATE"})
_POSITIVE_INT_TAGS = frozenset({"DISCNUMBER", "DISCTOTAL", "TOTALDISCS"})
_VALIDATED_TAGS: frozenset[str] = (
    _DATE_TAGS
    | _POSITIVE_INT_TAGS
    | {"TRACKNUMBER", "ISRC"}
)


def _analyse_tag(
    conn: sqlite3.Connection,
    tag_key: str,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> FieldFormatSection | None:
    """Run format validation for a tag key, or return None if no validator exists.

    Date tags are excluded — they have their own dedicated table.
    """
    key = tag_key.upper()
    if key in _DATE_TAGS:
        return None  # handled by _build_date_quality
    if key == "TRACKNUMBER":
        return _analyse_tracknumber(conn, total_files, artist_filter=artist_filter)
    if key == "DISCNUMBER":
        return _analyse_discnumber_cross_check(conn, total_files, artist_filter=artist_filter)
    if key in _POSITIVE_INT_TAGS:
        return _analyse_field(conn, key, total_files, _validate_positive_int, artist_filter=artist_filter)
    if key == "ISRC":
        return _analyse_isrc(conn, total_files, artist_filter=artist_filter)
    return None


def _build_date_quality(
    conn: sqlite3.Connection,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> list[DateQualityRow]:
    """Build date precision rows for DATE and ORIGINALDATE."""
    result: list[DateQualityRow] = []
    for tag_key in ("DATE", "ORIGINALDATE"):
        if artist_filter:
            rows = conn.execute(_TAG_VALUES_FILTERED_SQL, (tag_key, artist_filter)).fetchall()
            files_with_tag_rows = conn.execute(
                _FILES_WITH_TAG_FILTERED_SQL, (tag_key, artist_filter)
            ).fetchall()
        else:
            rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()
            files_with_tag_rows = conn.execute(_FILES_WITH_TAG_SQL, (tag_key,)).fetchall()

        files_with_tag = {r[0] for r in files_with_tag_rows}
        missing = max(total_files - len(files_with_tag), 0)

        precision_counts: Counter[str] = Counter()
        seen: set[int] = set()
        for r in rows:
            fid = r["file_id"]
            if fid in seen:
                continue
            seen.add(fid)
            precision_counts[date_precision(r["tag_value"])] += 1

        result.append(DateQualityRow(
            field_name=tag_key,
            full_date_count=precision_counts.get("full_date", 0),
            year_only_count=precision_counts.get("year_only", 0) + precision_counts.get("year_month", 0),
            invalid_count=precision_counts.get("invalid", 0),
            missing_count=missing,
        ))
    return result


def _analyse_group(
    conn: sqlite3.Connection,
    group_name: str,
    keys: tuple[str, ...],
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> TagGroupQuality:
    """Analyse tags in a config group that have format validators."""
    fields: list[FieldFormatSection] = []
    for key in keys:
        section = _analyse_tag(conn, key, total_files, artist_filter=artist_filter)
        if section is not None:
            fields.append(section)
    return TagGroupQuality(group_name=group_name, fields=fields)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
    *,
    artist_filter: str | None = None,
) -> TagQualityFullData:
    """Return the complete dataset for ``report tag-quality``."""
    total_files = _get_total_ok_files(conn, artist_filter)

    # Date precision table.
    date_quality = _build_date_quality(conn, total_files, artist_filter=artist_filter)

    # Field format validation grouped by config section (only validated tags, excludes dates).
    groups: list[TagGroupQuality] = [
        _analyse_group(conn, "Required Tags", tags_config.required, total_files, artist_filter=artist_filter),
        _analyse_group(conn, "Recommended Tags", tags_config.recommended, total_files, artist_filter=artist_filter),
        _analyse_group(conn, "Other Tracked Tags", tags_config.other, total_files, artist_filter=artist_filter),
    ]

    # External ID sections.
    file_album_dirs, _ = _build_file_album_dir_map(conn, artist_filter)
    all_file_ids = set(file_album_dirs.keys())

    mb = _build_mb_section(conn, total_files, file_album_dirs, all_file_ids, artist_filter)
    discogs = _build_discogs_section(conn, total_files, file_album_dirs, all_file_ids, artist_filter)

    return TagQualityFullData(
        total_files=total_files,
        date_quality=date_quality,
        groups=groups,
        musicbrainz=mb,
        discogs=discogs,
    )


def summary_data(conn: sqlite3.Connection, tags_config: TagsConfig) -> TagQualitySummaryData:
    """Return headline metrics only for the ``summary`` command."""
    total_files = _get_total_ok_files(conn)
    file_album_dirs, _ = _build_file_album_dir_map(conn)
    all_file_ids = set(file_album_dirs.keys())

    # Field validation summary.
    data = full_data(conn, tags_config)
    fields_with_invalid: list[FieldValidationRow] = []
    for group in data.groups:
        fields_with_invalid.extend(s.summary for s in group.fields if s.summary.invalid_count > 0)

    # MB summary.
    mb_tag_maps: dict[str, dict[int, str]] = {}
    mb_malformed = 0
    for key in _MB_KEYS:
        tag_map = _build_file_tag_map(conn, key)
        mb_tag_maps[key] = tag_map
        row = _id_coverage_row(key, tag_map, total_files, is_valid_uuid)
        mb_malformed += row.malformed_count
    mb_pct = _overall_coverage_pct(mb_tag_maps, _MB_KEYS, total_files, all_file_ids, is_valid_uuid)

    # Discogs summary.
    discogs_tag_maps: dict[str, dict[int, str]] = {}
    discogs_malformed = 0
    for key in _DISCOGS_KEYS:
        tag_map = _build_file_tag_map(conn, key)
        discogs_tag_maps[key] = tag_map
        row = _id_coverage_row(key, tag_map, total_files, is_valid_discogs_id)
        discogs_malformed += row.malformed_count
    discogs_pct = _overall_coverage_pct(discogs_tag_maps, _DISCOGS_KEYS, total_files, all_file_ids, is_valid_discogs_id)

    return TagQualitySummaryData(
        fields_with_invalid=fields_with_invalid,
        mb_overall_coverage_pct=mb_pct,
        mb_malformed_count=mb_malformed,
        discogs_overall_coverage_pct=discogs_pct,
        discogs_malformed_count=discogs_malformed,
    )
