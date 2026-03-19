"""
storeroon.reports.queries.artists — Report 10: Artist name consistency.

Fetch all distinct ALBUMARTIST and ARTIST values with file counts from
``raw_tags``.

Fuzzy matching runs in two passes:
    Pass 1 — Case/whitespace variants (fast, always runs including in summary):
        Group by LOWER(TRIM(tag_value)). Any group with more than one distinct
        original value is a case inconsistency.
    Pass 2 — Token-sort fuzzy matching (slow, excluded from summary):
        Pre-filter by length ratio, normalise via token-sort, compute
        difflib.SequenceMatcher ratio, flag pairs above threshold.

Public API:
    full_data(conn, fuzzy_threshold=0.85) -> ArtistsFullData
    summary_data(conn, fuzzy_threshold=0.85) -> ArtistsSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from difflib import SequenceMatcher

from storeroon.reports.models import (
    ArtistsFullData,
    ArtistsSummaryData,
    ArtistValueRow,
    CaseVariantGroup,
    FuzzyPairRow,
)
from storeroon.reports.utils import token_sort_normalise

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# All distinct ALBUMARTIST values with track count and album count.
_ALBUMARTIST_VALUES_SQL = """
SELECT
    rt.tag_value AS value,
    COUNT(DISTINCT rt.file_id) AS track_count,
    COUNT(DISTINCT SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1)) AS album_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND rt.tag_index = 0
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
ORDER BY track_count DESC
"""

# All distinct ARTIST values with track count and album count.
_ARTIST_VALUES_SQL = """
SELECT
    rt.tag_value AS value,
    COUNT(DISTINCT rt.file_id) AS track_count,
    COUNT(DISTINCT SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1)) AS album_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ARTIST'
  AND rt.tag_index = 0
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
ORDER BY track_count DESC
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_artist_values(
    conn: sqlite3.Connection,
    sql: str,
) -> list[ArtistValueRow]:
    """Fetch all distinct values for an artist-type tag key."""
    rows = conn.execute(sql).fetchall()
    return [
        ArtistValueRow(
            value=r["value"],
            track_count=r["track_count"],
            album_count=r["album_count"],
        )
        for r in rows
    ]


def _find_case_variants(
    values: list[ArtistValueRow],
) -> list[CaseVariantGroup]:
    """Pass 1: find groups of values that differ only in case/whitespace.

    Groups all distinct values by ``LOWER(TRIM(value))``. Any group with
    more than one distinct original value is a case inconsistency.

    Returns groups sorted by total track count descending.
    """
    groups: dict[str, list[ArtistValueRow]] = defaultdict(list)
    for v in values:
        normalised = v.value.strip().lower()
        groups[normalised].append(v)

    result: list[CaseVariantGroup] = []
    for normalised, entries in groups.items():
        if len(entries) < 2:
            continue
        # Distinct original values (after strip, preserving case).
        distinct_originals = sorted({e.value.strip() for e in entries})
        if len(distinct_originals) < 2:
            continue
        total_tracks = sum(e.track_count for e in entries)
        result.append(
            CaseVariantGroup(
                normalised=normalised,
                variants=distinct_originals,
                total_track_count=total_tracks,
            )
        )

    result.sort(key=lambda g: g.total_track_count, reverse=True)
    return result


def _find_fuzzy_pairs(
    values: list[ArtistValueRow],
    case_variant_groups: list[CaseVariantGroup],
    threshold: float,
    max_pairs: int = 100,
) -> list[FuzzyPairRow]:
    """Pass 2: token-sort fuzzy matching.

    Pre-filter: skip pairs where max(len(a), len(b)) / min(len(a), len(b)) > 2.0.
    Normalise via token-sort. Compute difflib.SequenceMatcher ratio.
    Flag pairs above threshold.

    Excludes pairs already caught by Pass 1 (case variants).

    Returns at most *max_pairs* results sorted by similarity descending.
    """
    # Build a set of normalised values that are in the same case-variant group
    # so we can skip them.
    case_variant_pairs: set[frozenset[str]] = set()
    for g in case_variant_groups:
        for i, a in enumerate(g.variants):
            for b in g.variants[i + 1 :]:
                case_variant_pairs.add(frozenset({a, b}))

    # Pre-compute token-sort normalised forms and track counts.
    entries: list[tuple[str, str, int]] = []  # (original, normalised, track_count)
    for v in values:
        original = v.value.strip()
        normalised = token_sort_normalise(original)
        entries.append((original, normalised, v.track_count))

    pairs: list[FuzzyPairRow] = []

    for i in range(len(entries)):
        orig_a, norm_a, count_a = entries[i]
        len_a = len(orig_a)

        for j in range(i + 1, len(entries)):
            orig_b, norm_b, count_b = entries[j]
            len_b = len(orig_b)

            # Skip pairs already caught by case variants.
            if frozenset({orig_a, orig_b}) in case_variant_pairs:
                continue

            # Pre-filter: skip clearly different-length names.
            min_len = min(len_a, len_b)
            max_len = max(len_a, len_b)
            if min_len > 0 and max_len / min_len > 2.0:
                continue

            # Skip identical normalised forms (these would be case variants
            # that somehow weren't caught — shouldn't happen, but be safe).
            if norm_a == norm_b:
                continue

            # Compute similarity.
            ratio = SequenceMatcher(None, norm_a, norm_b).ratio()
            if ratio >= threshold:
                pairs.append(
                    FuzzyPairRow(
                        name_a=orig_a,
                        name_b=orig_b,
                        similarity=round(ratio, 4),
                        count_a=count_a,
                        count_b=count_b,
                    )
                )

    # Sort by similarity descending and cap.
    pairs.sort(key=lambda p: p.similarity, reverse=True)
    return pairs[:max_pairs]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    fuzzy_threshold: float = 0.85,
) -> ArtistsFullData:
    """Return the complete dataset for ``report artists``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    fuzzy_threshold:
        Minimum similarity ratio (0.0–1.0) for fuzzy pair detection.
    """
    albumartist_values = _fetch_artist_values(conn, _ALBUMARTIST_VALUES_SQL)
    artist_values = _fetch_artist_values(conn, _ARTIST_VALUES_SQL)

    aa_case_variants = _find_case_variants(albumartist_values)
    aa_fuzzy_pairs = _find_fuzzy_pairs(
        albumartist_values, aa_case_variants, fuzzy_threshold
    )

    a_case_variants = _find_case_variants(artist_values)
    a_fuzzy_pairs = _find_fuzzy_pairs(artist_values, a_case_variants, fuzzy_threshold)

    return ArtistsFullData(
        albumartist_values=albumartist_values,
        albumartist_case_variants=aa_case_variants,
        albumartist_fuzzy_pairs=aa_fuzzy_pairs,
        artist_values=artist_values,
        artist_case_variants=a_case_variants,
        artist_fuzzy_pairs=a_fuzzy_pairs,
    )


def summary_data(
    conn: sqlite3.Connection,
    *,
    fuzzy_threshold: float = 0.85,
) -> ArtistsSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Only Pass 1 (case/whitespace variants) is run for summary mode —
    Pass 2 (fuzzy matching) is excluded because it is O(n²).

    Count of distinct ALBUMARTIST values, count of case variant groups
    (as a proxy for fuzzy pair count), count of distinct ARTIST values.
    """
    albumartist_values = _fetch_artist_values(conn, _ALBUMARTIST_VALUES_SQL)
    artist_values = _fetch_artist_values(conn, _ARTIST_VALUES_SQL)

    aa_case_variants = _find_case_variants(albumartist_values)

    return ArtistsSummaryData(
        distinct_albumartist_count=len(albumartist_values),
        albumartist_case_variant_count=len(aa_case_variants),
        albumartist_fuzzy_pair_count=len(aa_case_variants),  # Pass 1 only in summary
        distinct_artist_count=len(artist_values),
    )
