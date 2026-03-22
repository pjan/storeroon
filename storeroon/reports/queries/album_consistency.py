"""
storeroon.reports.queries.album_consistency — Intra-album consistency helpers.

Album boundaries are defined by the parent directory path of the FLAC file,
NOT by ALBUMARTIST + ALBUM tag values. Two different releases (e.g. original
and remaster) may share those values but are distinct directories.

Private helpers used by overview, issues, and collection_issues queries:
    _check_field_consistency(conn, album_dir, field, total_tracks)
    _check_track_numbering(conn, album_dir, total_tracks)
    _CONSISTENCY_FIELDS
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict

from storeroon.reports.models import (
    FieldConsistencyViolation,
    TrackNumberingViolation,
)

# ---------------------------------------------------------------------------
# Fields checked for cross-track consistency within an album directory
# ---------------------------------------------------------------------------

_CONSISTENCY_FIELDS: tuple[str, ...] = (
    "ALBUMARTIST",
    "ALBUM",
    "DATE",
    "ORIGINALDATE",
    "TRACKTOTAL",
    "DISCTOTAL",
    "LABEL",
    "CATALOGNUMBER",
    "RELEASETYPE",
    "RELEASESTATUS",
    "MUSICBRAINZ_ALBUMID",
    "MUSICBRAINZ_ALBUMARTISTID",
    "MUSICBRAINZ_RELEASEGROUPID",
)

# Reverse alias map: canonical → list of alias keys.
# Used to merge alias values with canonical values in consistency checks.
_CANONICAL_TO_ALIASES: dict[str, list[str]] = {
    "DATE": ["YEAR"],
    "ORIGINALDATE": ["ORIGINALYEAR"],
    "ALBUMARTIST": ["ALBUM ARTIST"],
    "TRACKTOTAL": ["TOTALTRACKS"],
    "DISCTOTAL": ["TOTALDISCS", "DISCS"],
}

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# All tag values for a given key across files in an album directory.
# Returns (file_id, tag_value) pairs. Only tag_index=0 to avoid
# double-counting multi-value tags for consistency purposes.
_TAG_VALUES_IN_ALBUM_SQL = """
SELECT rt.file_id, rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) = ?
  AND rt.tag_key_upper = ?
  AND rt.tag_index = 0
"""

# Track numbering data for an album directory.
# Fetches all aliases for totaltracks (TRACKTOTAL, TOTALTRACKS) and
# totaldiscs (DISCTOTAL, TOTALDISCS, DISCS) so the resolver can pick
# the canonical value with the right priority.
_TRACK_NUMBERING_SQL = """
SELECT
    rt_tn.file_id,
    rt_tn.tag_value AS tracknumber,
    rt_dn.tag_value AS discnumber,
    rt_tracktotal.tag_value  AS tracktotal,
    rt_totaltracks.tag_value AS totaltracks,
    rt_disctotal.tag_value   AS disctotal,
    rt_totaldiscs.tag_value  AS totaldiscs,
    rt_discs.tag_value       AS discs
FROM raw_tags rt_tn
JOIN files f ON f.id = rt_tn.file_id
LEFT JOIN raw_tags rt_dn
    ON rt_dn.file_id = rt_tn.file_id
   AND rt_dn.tag_key_upper = 'DISCNUMBER'
   AND rt_dn.tag_index = 0
LEFT JOIN raw_tags rt_tracktotal
    ON rt_tracktotal.file_id = rt_tn.file_id
   AND rt_tracktotal.tag_key_upper = 'TRACKTOTAL'
   AND rt_tracktotal.tag_index = 0
LEFT JOIN raw_tags rt_totaltracks
    ON rt_totaltracks.file_id = rt_tn.file_id
   AND rt_totaltracks.tag_key_upper = 'TOTALTRACKS'
   AND rt_totaltracks.tag_index = 0
LEFT JOIN raw_tags rt_disctotal
    ON rt_disctotal.file_id = rt_tn.file_id
   AND rt_disctotal.tag_key_upper = 'DISCTOTAL'
   AND rt_disctotal.tag_index = 0
LEFT JOIN raw_tags rt_totaldiscs
    ON rt_totaldiscs.file_id = rt_tn.file_id
   AND rt_totaldiscs.tag_key_upper = 'TOTALDISCS'
   AND rt_totaldiscs.tag_index = 0
LEFT JOIN raw_tags rt_discs
    ON rt_discs.file_id = rt_tn.file_id
   AND rt_discs.tag_key_upper = 'DISCS'
   AND rt_discs.tag_index = 0
WHERE f.status = 'ok'
  AND SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) = ?
  AND rt_tn.tag_key_upper = 'TRACKNUMBER'
  AND rt_tn.tag_index = 0
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_int(value: str | None) -> int | None:
    """Parse a string as an integer, handling legacy N/T format and None."""
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    # Handle legacy "N/T" tracknumber format — extract N.
    if "/" in v:
        parts = v.split("/", 1)
        v = parts[0].strip()
    try:
        return int(v)
    except (ValueError, OverflowError):
        return None


def _resolve_totaltracks(tracktotal: str | None, totaltracks: str | None) -> int | None:
    """Resolve the declared total tracks from aliases.

    Priority: TRACKTOTAL (canonical) > TOTALTRACKS (alias).
    """
    val = _safe_int(tracktotal)
    if val is not None:
        return val
    return _safe_int(totaltracks)


def _resolve_totaldiscs(
    disctotal: str | None, totaldiscs: str | None, discs: str | None
) -> int | None:
    """Resolve the declared total discs from aliases.

    Priority: DISCTOTAL (canonical) > TOTALDISCS (alias) > DISCS (alias).
    """
    val = _safe_int(disctotal)
    if val is not None:
        return val
    val = _safe_int(totaldiscs)
    if val is not None:
        return val
    return _safe_int(discs)


def _check_field_consistency(
    conn: sqlite3.Connection,
    album_dir: str,
    field: str,
    total_tracks_in_dir: int,
) -> FieldConsistencyViolation | None:
    """Check a single field for cross-track consistency in an album directory.

    Also queries alias tag keys for the field and merges values, so that
    e.g. TRACKTOTAL and TOTALTRACKS are treated as the same concept.

    Returns a violation if inconsistency is found, or None.
    """
    # Query canonical field + all its aliases, take first non-null per file
    keys_to_check = [field] + _CANONICAL_TO_ALIASES.get(field, [])

    # Collect per-file: use canonical value if present, else first alias value
    file_values: dict[int, str] = {}  # file_id → value
    for key in keys_to_check:
        rows = conn.execute(_TAG_VALUES_IN_ALBUM_SQL, (album_dir, key)).fetchall()
        for r in rows:
            fid = r["file_id"]
            if fid not in file_values:  # first key wins (canonical first)
                val = r["tag_value"]
                if val and val.strip():
                    file_values[fid] = val.strip()

    # Group by normalised value.
    value_counts: Counter[str] = Counter()
    raw_values: dict[str, set[str]] = defaultdict(set)  # normalised → originals

    for val in file_values.values():
        normalised = val.lower()
        value_counts[normalised] += 1
        raw_values[normalised].add(val)

    null_count = total_tracks_in_dir - len(file_values)

    # Check for inconsistency: more than one distinct normalised value,
    # OR a mix of NULL and non-NULL.
    has_value_inconsistency = len(value_counts) > 1
    has_null_mix = null_count > 0 and len(file_values) > 0

    if not has_value_inconsistency and not has_null_mix:
        return None

    # Build the distinct values list (use originals for display).
    distinct_values: list[str] = []
    track_counts_per_value: dict[str, int] = {}
    for normalised, count in value_counts.most_common():
        # Pick one representative original value.
        originals = sorted(raw_values[normalised])
        representative = originals[0]
        distinct_values.append(representative)
        track_counts_per_value[representative] = count

    return FieldConsistencyViolation(
        album_dir=album_dir,
        field_name=field,
        distinct_values=distinct_values,
        track_counts_per_value=track_counts_per_value,
        null_track_count=null_count,
    )


def _check_track_numbering(
    conn: sqlite3.Connection,
    album_dir: str,
    total_tracks_in_dir: int,
) -> list[TrackNumberingViolation]:
    """Run all track numbering checks for a single album directory.

    Returns a list of violations found (may be empty).
    """
    violations: list[TrackNumberingViolation] = []

    rows = conn.execute(_TRACK_NUMBERING_SQL, (album_dir,)).fetchall()
    if not rows:
        return violations

    # Collect per-disc data.
    # disc_tracks: disc_number -> list of track numbers
    disc_tracks: dict[int, list[int]] = defaultdict(list)
    declared_totaltracks: set[int | None] = set()
    declared_totaldiscs: set[int | None] = set()

    for r in rows:
        tn = _safe_int(r["tracknumber"])
        dn = _safe_int(r["discnumber"])

        # Resolve totaltracks: TRACKTOTAL > TOTALTRACKS
        tt = _resolve_totaltracks(r["tracktotal"], r["totaltracks"])
        # Resolve totaldiscs: DISCTOTAL > TOTALDISCS > DISCS
        td = _resolve_totaldiscs(r["disctotal"], r["totaldiscs"], r["discs"])

        # Default disc number to 1 if not specified.
        disc = dn if dn is not None else 1

        if tn is not None:
            disc_tracks[disc].append(tn)

        declared_totaltracks.add(tt)
        declared_totaldiscs.add(td)

    # Get a single declared TOTALTRACKS (if consistent).
    tt_values = {v for v in declared_totaltracks if v is not None}
    td_values = {v for v in declared_totaldiscs if v is not None}

    # Determine if multi-disc.
    is_multi_disc = len(disc_tracks) > 1 or any(d > 1 for d in disc_tracks)

    # --- Check: TOTALTRACKS vs actual file count ---
    # For single-disc albums, compare against total files in the directory.
    # For multi-disc, compare per disc.
    if not is_multi_disc:
        # Single disc: TOTALTRACKS should equal total files in dir.
        if len(tt_values) == 1:
            declared_tt = tt_values.pop()
            tt_values.add(declared_tt)  # put it back
            if declared_tt != total_tracks_in_dir:
                violations.append(
                    TrackNumberingViolation(
                        album_dir=album_dir,
                        check_type="totaltracks_mismatch",
                        description=(
                            f"Total tracks declares {declared_tt} but directory "
                            f"contains {total_tracks_in_dir} FLAC files"
                        ),
                    )
                )
    else:
        # Multi-disc: check per disc if a consistent TOTALTRACKS exists.
        if len(tt_values) == 1:
            declared_tt = next(iter(tt_values))
            for disc_num, tracks in sorted(disc_tracks.items()):
                if len(tracks) != declared_tt:
                    violations.append(
                        TrackNumberingViolation(
                            album_dir=album_dir,
                            check_type="totaltracks_mismatch",
                            description=(
                                f"Disc {disc_num}: Total tracks declares "
                                f"{declared_tt} but disc has {len(tracks)} tracks"
                            ),
                        )
                    )

    # --- Check per-disc: gaps, duplicates, exceeds total tracks ---
    for disc_num, tracks in sorted(disc_tracks.items()):
        if not tracks:
            continue

        track_set = set(tracks)
        max_track = max(tracks)

        # Track number duplicates.
        track_counter = Counter(tracks)
        for tn_val, cnt in track_counter.items():
            if cnt > 1:
                violations.append(
                    TrackNumberingViolation(
                        album_dir=album_dir,
                        check_type="duplicate_track",
                        description=(
                            f"{'Disc ' + str(disc_num) + ': ' if is_multi_disc else ''}"
                            f"{cnt} files claim track number {tn_val}"
                        ),
                    )
                )

        # Track number gaps: check for missing integers in [1, max].
        expected = set(range(1, max_track + 1))
        missing = sorted(expected - track_set)
        for m in missing:
            violations.append(
                TrackNumberingViolation(
                    album_dir=album_dir,
                    check_type="missing_track",
                    description=(
                        f"{'Disc ' + str(disc_num) + ': ' if is_multi_disc else ''}"
                        f"Track {m} missing (range 1–{max_track})"
                    ),
                )
            )

        # Track number exceeding declared TOTALTRACKS.
        if len(tt_values) == 1:
            declared_tt = next(iter(tt_values))
            for tn_val in sorted(track_set):
                if tn_val > declared_tt:
                    violations.append(
                        TrackNumberingViolation(
                            album_dir=album_dir,
                            check_type="exceeds_total",
                            description=(
                                f"{'Disc ' + str(disc_num) + ': ' if is_multi_disc else ''}"
                                f"Track number {tn_val} exceeds total tracks ({declared_tt})"
                            ),
                        )
                    )

    # --- Check: disc number gaps (multi-disc only) ---
    if is_multi_disc and len(td_values) == 1:
        declared_td = next(iter(td_values))
        expected_discs = set(range(1, declared_td + 1))
        actual_discs = set(disc_tracks.keys())
        missing_discs = sorted(expected_discs - actual_discs)
        for md in missing_discs:
            violations.append(
                TrackNumberingViolation(
                    album_dir=album_dir,
                    check_type="missing_disc",
                    description=(
                        f"Disc {md} missing (Total discs declares {declared_td})"
                    ),
                )
            )

    return violations