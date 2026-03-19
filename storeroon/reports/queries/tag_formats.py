"""
storeroon.reports.queries.tag_formats — Report 5: Tag format quality.

Fetches raw values from ``raw_tags`` for each validated field, applies
format checks in Python, and groups results into valid / invalid / absent.

Validated fields:
    - DATE, ORIGINALDATE — date format (YYYY, YYYY-MM, YYYY-MM-DD)
    - TRACKNUMBER — positive integer, optional zero-padding, legacy N/T
    - DISCNUMBER, TOTALDISCS — positive integer
    - ISRC — 12-char alphanumeric (with or without hyphens)
    - MUSICBRAINZ_TRACKID, MUSICBRAINZ_RELEASETRACKID, MUSICBRAINZ_ALBUMID,
      MUSICBRAINZ_ARTISTID, MUSICBRAINZ_ALBUMARTISTID, MUSICBRAINZ_RELEASEGROUPID
      — UUID v4 format

Public API:
    full_data(conn, artist_filter=None) -> TagFormatsFullData
    summary_data(conn) -> TagFormatsSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Callable

from storeroon.reports.models import (
    DatePrecisionRow,
    FieldFormatSection,
    FieldValidationRow,
    InvalidValueRow,
    TagFormatsFullData,
    TagFormatsSummaryData,
)
from storeroon.reports.utils import (
    RE_ISRC_HYPHEN,
    RE_POSITIVE_INT,
    RE_TRACKNUMBER_LEGACY,
    date_precision,
    is_valid_date,
    is_valid_isrc,
    is_valid_uuid,
    safe_pct,
)

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_TOTAL_OK_FILES_SQL = """
SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'
"""

# Fetch all values for a given tag_key_upper across ok files.
# Returns (file_id, tag_value) pairs.
_TAG_VALUES_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

# Fetch all values for a given tag_key_upper, filtered by ALBUMARTIST substring.
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

# Count of total ok files (optionally filtered by artist).
_TOTAL_OK_FILES_FILTERED_SQL = """
SELECT COUNT(DISTINCT f.id) AS cnt
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""

# File IDs that have a given tag_key_upper (ok files only).
_FILES_WITH_TAG_SQL = """
SELECT DISTINCT rt.file_id
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = ?
"""

# File IDs that have a given tag_key_upper, filtered by artist.
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

# For disc number cross-check: fetch DISCNUMBER and TOTALDISCS for the same file.
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


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------


def _validate_date(value: str) -> bool:
    """Validate DATE / ORIGINALDATE format."""
    return is_valid_date(value)


def _validate_tracknumber(value: str) -> bool:
    """Validate TRACKNUMBER: positive integer (optionally zero-padded)
    or legacy N/T format."""
    v = value.strip()
    # Legacy N/T format is valid (but flagged separately).
    if RE_TRACKNUMBER_LEGACY.match(v):
        m = RE_TRACKNUMBER_LEGACY.match(v)
        assert m is not None
        track = int(m.group(1))
        return track > 0
    # Positive integer (possibly zero-padded).
    if RE_POSITIVE_INT.match(v):
        n = int(v)
        return n > 0
    return False


def _validate_positive_int(value: str) -> bool:
    """Validate DISCNUMBER / TOTALDISCS: positive integer string."""
    v = value.strip()
    if not RE_POSITIVE_INT.match(v):
        return False
    return int(v) > 0


def _validate_isrc(value: str) -> bool:
    """Validate ISRC format."""
    v = value.strip()
    if not is_valid_isrc(v):
        return False
    # Check for placeholder designation code '00000'.
    # Remove hyphens for uniform check.
    clean = v.replace("-", "")
    if len(clean) == 12 and clean[-5:] == "00000":
        return False
    return True


def _validate_uuid(value: str) -> bool:
    """Validate UUID v4 format for MusicBrainz IDs."""
    return is_valid_uuid(value)


# ---------------------------------------------------------------------------
# Per-field analysis
# ---------------------------------------------------------------------------


def _analyse_field(
    conn: sqlite3.Connection,
    tag_key: str,
    total_files: int,
    validator: Callable[[str], bool],
    *,
    artist_filter: str | None = None,
    max_invalid_values: int = 20,
) -> FieldFormatSection:
    """Run format validation for a single field and build the result section.

    Parameters
    ----------
    conn:
        Database connection.
    tag_key:
        Upper-cased tag key to validate.
    total_files:
        Total number of ok files (for absent calculation).
    validator:
        Function that returns True if a value is valid.
    artist_filter:
        Optional case-insensitive ALBUMARTIST substring filter.
    max_invalid_values:
        Maximum number of distinct invalid values to include in the output.
    """
    # Fetch all values for this tag.
    if artist_filter:
        rows = conn.execute(
            _TAG_VALUES_FILTERED_SQL, (tag_key, artist_filter)
        ).fetchall()
        files_with_tag_rows = conn.execute(
            _FILES_WITH_TAG_FILTERED_SQL, (tag_key, artist_filter)
        ).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()
        files_with_tag_rows = conn.execute(_FILES_WITH_TAG_SQL, (tag_key,)).fetchall()

    files_with_tag = {r[0] for r in files_with_tag_rows}
    absent_count = total_files - len(files_with_tag)
    if absent_count < 0:
        absent_count = 0

    valid_count = 0
    invalid_count = 0
    invalid_values: Counter[str] = Counter()

    # De-duplicate: we check one value per file (tag_index=0 is fetched first
    # by default, but raw_tags may have multiple rows; we count per file_id).
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
            # Truncate very long values for display.
            display_val = value if len(value) <= 100 else value[:97] + "..."
            invalid_values[display_val] += 1

    # Build invalid values list (capped).
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


def _analyse_date_field(
    conn: sqlite3.Connection,
    tag_key: str,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> FieldFormatSection:
    """Specialised analysis for DATE / ORIGINALDATE that also computes
    the precision distribution."""
    section = _analyse_field(
        conn,
        tag_key,
        total_files,
        _validate_date,
        artist_filter=artist_filter,
    )

    # Compute date precision distribution from all values.
    if artist_filter:
        rows = conn.execute(
            _TAG_VALUES_FILTERED_SQL, (tag_key, artist_filter)
        ).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, (tag_key,)).fetchall()

    precision_counter: Counter[str] = Counter()
    seen: set[int] = set()
    for r in rows:
        fid = r["file_id"]
        if fid in seen:
            continue
        seen.add(fid)
        prec = date_precision(r["tag_value"])
        precision_counter[prec] += 1

    total_with_tag = sum(precision_counter.values())
    precision_rows = [
        DatePrecisionRow(
            precision=prec,
            count=cnt,
            percentage=safe_pct(cnt, total_with_tag),
        )
        for prec, cnt in sorted(
            precision_counter.items(),
            key=lambda x: {
                "full_date": 0,
                "year_month": 1,
                "year_only": 2,
                "invalid": 3,
            }.get(x[0], 4),
        )
    ]

    extra = {"date_precision": precision_rows} if precision_rows else {}

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=section.invalid_values,
        invalid_values_total=section.invalid_values_total,
        extra=extra,
    )


def _analyse_tracknumber(
    conn: sqlite3.Connection,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> FieldFormatSection:
    """Specialised analysis for TRACKNUMBER that flags legacy N/T format."""
    section = _analyse_field(
        conn,
        "TRACKNUMBER",
        total_files,
        _validate_tracknumber,
        artist_filter=artist_filter,
    )

    # Count legacy N/T format values among the valid ones.
    if artist_filter:
        rows = conn.execute(
            _TAG_VALUES_FILTERED_SQL, ("TRACKNUMBER", artist_filter)
        ).fetchall()
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
        if RE_POSITIVE_INT.match(v):
            n = int(v)
            if n > 99:
                exceeds_99_count += 1

    # Append legacy and exceeds-99 info as extra invalid values if present.
    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if legacy_count > 0:
        extra_invalids.insert(
            0, InvalidValueRow(value="[Legacy N/T format]", count=legacy_count)
        )
        extra_total += legacy_count
    if exceeds_99_count > 0:
        extra_invalids.insert(
            0,
            InvalidValueRow(
                value="[Track number > 99 (unusual)]", count=exceeds_99_count
            ),
        )
        extra_total += exceeds_99_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


def _analyse_discnumber_cross_check(
    conn: sqlite3.Connection,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> FieldFormatSection:
    """Analyse DISCNUMBER with cross-check against TOTALDISCS."""
    section = _analyse_field(
        conn,
        "DISCNUMBER",
        total_files,
        _validate_positive_int,
        artist_filter=artist_filter,
    )

    # Cross-check: disc number exceeding TOTALDISCS.
    rows = conn.execute(_DISC_CROSS_CHECK_SQL).fetchall()
    exceeds_count = 0
    for r in rows:
        dn_str = r["discnumber"]
        td_str = r["totaldiscs"]
        if dn_str and td_str:
            dn_stripped = dn_str.strip()
            td_stripped = td_str.strip()
            if RE_POSITIVE_INT.match(dn_stripped) and RE_POSITIVE_INT.match(
                td_stripped
            ):
                dn = int(dn_stripped)
                td = int(td_stripped)
                if dn > td:
                    exceeds_count += 1

    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if exceeds_count > 0:
        extra_invalids.insert(
            0,
            InvalidValueRow(value="[DISCNUMBER > TOTALDISCS]", count=exceeds_count),
        )
        extra_total += exceeds_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


def _analyse_isrc(
    conn: sqlite3.Connection,
    total_files: int,
    *,
    artist_filter: str | None = None,
) -> FieldFormatSection:
    """Specialised analysis for ISRC that also counts hyphenated forms."""
    section = _analyse_field(
        conn,
        "ISRC",
        total_files,
        _validate_isrc,
        artist_filter=artist_filter,
    )

    # Count hyphenated ISRCs among valid ones.
    if artist_filter:
        rows = conn.execute(
            _TAG_VALUES_FILTERED_SQL, ("ISRC", artist_filter)
        ).fetchall()
    else:
        rows = conn.execute(_TAG_VALUES_SQL, ("ISRC",)).fetchall()

    hyphenated_count = 0
    seen: set[int] = set()
    for r in rows:
        fid = r["file_id"]
        if fid in seen:
            continue
        seen.add(fid)
        v = r["tag_value"].strip()
        if RE_ISRC_HYPHEN.match(v):
            hyphenated_count += 1

    extra_invalids = list(section.invalid_values)
    extra_total = section.invalid_values_total
    if hyphenated_count > 0:
        extra_invalids.insert(
            0,
            InvalidValueRow(
                value="[Hyphenated ISRC — needs normalisation]",
                count=hyphenated_count,
            ),
        )
        extra_total += hyphenated_count

    return FieldFormatSection(
        field_name=section.field_name,
        summary=section.summary,
        invalid_values=extra_invalids,
        invalid_values_total=extra_total,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    artist_filter: str | None = None,
) -> TagFormatsFullData:
    """Return the complete dataset for ``report tag-formats``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    artist_filter:
        Optional case-insensitive ALBUMARTIST substring filter.
    """
    if artist_filter:
        row = conn.execute(_TOTAL_OK_FILES_FILTERED_SQL, (artist_filter,)).fetchone()
        total_files = row[0] if row else 0
    else:
        row = conn.execute(_TOTAL_OK_FILES_SQL).fetchone()
        total_files = row[0] if row else 0

    sections: list[FieldFormatSection] = []

    # DATE and ORIGINALDATE — specialised date analysis.
    sections.append(
        _analyse_date_field(conn, "DATE", total_files, artist_filter=artist_filter)
    )
    sections.append(
        _analyse_date_field(
            conn, "ORIGINALDATE", total_files, artist_filter=artist_filter
        )
    )

    # TRACKNUMBER — specialised with legacy N/T detection.
    sections.append(
        _analyse_tracknumber(conn, total_files, artist_filter=artist_filter)
    )

    # DISCNUMBER — with cross-check against TOTALDISCS.
    sections.append(
        _analyse_discnumber_cross_check(conn, total_files, artist_filter=artist_filter)
    )

    # TOTALDISCS — plain positive int.
    sections.append(
        _analyse_field(
            conn,
            "TOTALDISCS",
            total_files,
            _validate_positive_int,
            artist_filter=artist_filter,
        )
    )

    # ISRC — specialised with hyphenated form counting.
    sections.append(_analyse_isrc(conn, total_files, artist_filter=artist_filter))

    # MusicBrainz IDs — UUID v4 validation.
    mb_keys = [
        "MUSICBRAINZ_TRACKID",
        "MUSICBRAINZ_RELEASETRACKID",
        "MUSICBRAINZ_ALBUMID",
        "MUSICBRAINZ_ARTISTID",
        "MUSICBRAINZ_ALBUMARTISTID",
        "MUSICBRAINZ_RELEASEGROUPID",
    ]
    for key in mb_keys:
        sections.append(
            _analyse_field(
                conn,
                key,
                total_files,
                _validate_uuid,
                artist_filter=artist_filter,
            )
        )

    return TagFormatsFullData(
        total_files=total_files,
        sections=sections,
    )


def summary_data(conn: sqlite3.Connection) -> TagFormatsSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Fields with any invalid values, count of invalid instances per field.
    """
    data = full_data(conn)
    fields_with_invalid = [
        s.summary for s in data.sections if s.summary.invalid_count > 0
    ]
    return TagFormatsSummaryData(fields_with_invalid=fields_with_invalid)
