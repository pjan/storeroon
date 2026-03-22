"""
storeroon.reports.queries.genres — Report 11: Genre analysis.

Fetch all GENRE tag values from ``raw_tags`` where
``tag_key_upper = 'GENRE'``. Because GENRE can be multi-value (multiple
rows for the same file), count both distinct file occurrences and distinct
value occurrences.

Apply same fuzzy matching logic as Report 10 (same thresholds).

Full report tables:
    1. All distinct GENRE values with file count and % of collection.
    2. Genre fuzzy similarity pairs.
    3. Files with no GENRE tag — count and %, grouped by ALBUMARTIST.
    4. Files with multiple GENRE tag instances — count, distinct combos.

Public API:
    full_data(conn, fuzzy_threshold=0.85) -> GenresFullData
    summary_data(conn, fuzzy_threshold=0.85) -> GenresSummaryData
"""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from difflib import SequenceMatcher

from storeroon.reports.models import (
    FuzzyPairRow,
    GenreMissingByArtist,
    GenresFullData,
    GenresSummaryData,
    GenreValueRow,
    MultiGenreCombo,
)
from storeroon.reports.utils import TOTAL_OK_FILES_SQL, safe_pct, token_sort_normalise

# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# All distinct GENRE values with the count of distinct files that have each value.
_GENRE_VALUES_SQL = """
SELECT
    rt.tag_value AS value,
    COUNT(DISTINCT rt.file_id) AS file_count
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'GENRE'
  AND TRIM(rt.tag_value) != ''
GROUP BY rt.tag_value
ORDER BY file_count DESC
"""

# File IDs that have at least one GENRE tag (non-empty).
_FILES_WITH_GENRE_SQL = """
SELECT DISTINCT rt.file_id
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'GENRE'
  AND TRIM(rt.tag_value) != ''
"""

# File IDs with NO genre tag at all (or only empty genre tags).
# We find all ok files and subtract those with a non-empty GENRE.
_FILES_WITHOUT_GENRE_SQL = """
SELECT f.id AS file_id
FROM files f
WHERE f.status = 'ok'
  AND f.id NOT IN (
      SELECT DISTINCT rt.file_id
      FROM raw_tags rt
      WHERE rt.tag_key_upper = 'GENRE'
        AND TRIM(rt.tag_value) != ''
  )
"""

# ALBUMARTIST for files without genre, grouped by artist.
_MISSING_GENRE_BY_ARTIST_SQL = """
SELECT
    COALESCE(
        (SELECT rt2.tag_value
         FROM raw_tags rt2
         WHERE rt2.file_id = f.id
           AND rt2.tag_key_upper = 'ALBUMARTIST'
           AND rt2.tag_index = 0
         LIMIT 1),
        '(unknown)'
    ) AS artist,
    COUNT(*) AS missing_count
FROM files f
WHERE f.status = 'ok'
  AND f.id NOT IN (
      SELECT DISTINCT rt.file_id
      FROM raw_tags rt
      WHERE rt.tag_key_upper = 'GENRE'
        AND TRIM(rt.tag_value) != ''
  )
GROUP BY artist
ORDER BY missing_count DESC
"""

# Files with multiple GENRE tag instances (tag_index > 0 means multi-value).
_MULTI_GENRE_FILES_SQL = """
SELECT
    rt.file_id,
    rt.tag_value
FROM raw_tags rt
JOIN files f ON f.id = rt.file_id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'GENRE'
  AND TRIM(rt.tag_value) != ''
  AND rt.file_id IN (
      SELECT rt2.file_id
      FROM raw_tags rt2
      WHERE rt2.tag_key_upper = 'GENRE'
        AND TRIM(rt2.tag_value) != ''
      GROUP BY rt2.file_id
      HAVING COUNT(*) > 1
  )
ORDER BY rt.file_id, rt.tag_index
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_total_ok_files(conn: sqlite3.Connection) -> int:
    """Return count of files with status='ok'."""
    row = conn.execute(TOTAL_OK_FILES_SQL).fetchone()
    return row[0] if row else 0


def _fetch_genre_values(
    conn: sqlite3.Connection,
    total_files: int,
) -> list[GenreValueRow]:
    """Fetch all distinct GENRE values with file counts."""
    rows = conn.execute(_GENRE_VALUES_SQL).fetchall()
    return [
        GenreValueRow(
            value=r["value"],
            file_count=r["file_count"],
            file_pct=safe_pct(r["file_count"], total_files),
        )
        for r in rows
    ]


def _find_case_variants(
    values: list[GenreValueRow],
) -> list[tuple[str, list[GenreValueRow]]]:
    """Find groups of genre values that differ only in case/whitespace.

    Returns a list of (normalised_form, [entries]) for groups with 2+ members.
    """
    groups: dict[str, list[GenreValueRow]] = defaultdict(list)
    for v in values:
        normalised = v.value.strip().lower()
        groups[normalised].append(v)

    result: list[tuple[str, list[GenreValueRow]]] = []
    for normalised, entries in groups.items():
        if len(entries) >= 2:
            result.append((normalised, entries))

    return result


def _find_fuzzy_pairs(
    values: list[GenreValueRow],
    threshold: float,
    max_pairs: int = 100,
) -> list[FuzzyPairRow]:
    """Token-sort fuzzy matching on genre values.

    Same two-pass logic as Report 10 (artists):
        Pass 1 — Case/whitespace variants are identified and excluded.
        Pass 2 — Token-sort + SequenceMatcher on remaining pairs.

    Pre-filter: skip pairs where max(len(a), len(b)) / min(len(a), len(b)) > 2.0.

    Returns at most *max_pairs* results sorted by similarity descending.
    """
    # Build a set of normalised values that are case variants of each other
    # so we can skip them in fuzzy matching.
    case_variant_groups = _find_case_variants(values)
    case_variant_pairs: set[frozenset[str]] = set()
    for _, entries in case_variant_groups:
        originals = [e.value.strip() for e in entries]
        for i, a in enumerate(originals):
            for b in originals[i + 1 :]:
                case_variant_pairs.add(frozenset({a, b}))

    # Pre-compute token-sort normalised forms.
    normalised_entries: list[
        tuple[str, str, int]
    ] = []  # (original, normalised, file_count)
    for v in values:
        original = v.value.strip()
        normalised = token_sort_normalise(original)
        normalised_entries.append((original, normalised, v.file_count))

    pairs: list[FuzzyPairRow] = []

    for i in range(len(normalised_entries)):
        orig_a, norm_a, count_a = normalised_entries[i]
        len_a = len(orig_a)

        for j in range(i + 1, len(normalised_entries)):
            orig_b, norm_b, count_b = normalised_entries[j]
            len_b = len(orig_b)

            # Skip pairs already caught by case variants.
            if frozenset({orig_a, orig_b}) in case_variant_pairs:
                continue

            # Pre-filter: skip clearly different-length names.
            min_len = min(len_a, len_b)
            max_len = max(len_a, len_b)
            if min_len > 0 and max_len / min_len > 2.0:
                continue

            # Skip identical normalised forms (these are case variants
            # that should have been caught above).
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


def _count_files_without_genre(conn: sqlite3.Connection) -> int:
    """Count files with no GENRE tag (or only empty GENRE tags)."""
    rows = conn.execute(_FILES_WITHOUT_GENRE_SQL).fetchall()
    return len(rows)


def _missing_genre_by_artist(
    conn: sqlite3.Connection,
) -> list[GenreMissingByArtist]:
    """Group files with no genre by ALBUMARTIST, sorted by missing count desc."""
    rows = conn.execute(_MISSING_GENRE_BY_ARTIST_SQL).fetchall()
    return [
        GenreMissingByArtist(
            artist=r["artist"],
            missing_count=r["missing_count"],
        )
        for r in rows
    ]


def _multi_genre_files(
    conn: sqlite3.Connection,
) -> tuple[int, list[MultiGenreCombo]]:
    """Find files with multiple GENRE tag instances.

    Returns (count_of_multi_genre_files, list_of_distinct_value_combinations).
    """
    rows = conn.execute(_MULTI_GENRE_FILES_SQL).fetchall()

    # Group genre values by file_id.
    file_genres: dict[int, list[str]] = defaultdict(list)
    for r in rows:
        file_genres[r["file_id"]].append(r["tag_value"])

    multi_count = len(file_genres)

    # Count distinct combinations.
    combo_counter: Counter[tuple[str, ...]] = Counter()
    for genres in file_genres.values():
        # Sort for consistent combination keys.
        combo = tuple(sorted(genres))
        combo_counter[combo] += 1

    combos = [
        MultiGenreCombo(
            values=list(combo),
            file_count=count,
        )
        for combo, count in combo_counter.most_common()
    ]

    return multi_count, combos


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(
    conn: sqlite3.Connection,
    *,
    fuzzy_threshold: float = 0.85,
) -> GenresFullData:
    """Return the complete dataset for ``report genres``.

    Parameters
    ----------
    conn:
        An open SQLite connection to the storeroon database.
    fuzzy_threshold:
        Minimum similarity ratio (0.0–1.0) for fuzzy pair detection.
        Read from the TOML config under ``[reports] fuzzy_threshold``.
    """
    total_files = _get_total_ok_files(conn)
    genre_values = _fetch_genre_values(conn, total_files)

    fuzzy_pairs = _find_fuzzy_pairs(genre_values, fuzzy_threshold)

    no_genre_count = _count_files_without_genre(conn)
    no_genre_pct = safe_pct(no_genre_count, total_files)
    no_genre_by_artist = _missing_genre_by_artist(conn)

    multi_genre_count, multi_genre_combos = _multi_genre_files(conn)

    return GenresFullData(
        total_files=total_files,
        genre_values=genre_values,
        fuzzy_pairs=fuzzy_pairs,
        no_genre_count=no_genre_count,
        no_genre_pct=no_genre_pct,
        no_genre_by_artist=no_genre_by_artist,
        multi_genre_count=multi_genre_count,
        multi_genre_combos=multi_genre_combos,
    )


def summary_data(
    conn: sqlite3.Connection,
    *,
    fuzzy_threshold: float = 0.85,
) -> GenresSummaryData:
    """Return headline metrics only for the ``summary`` command.

    Total distinct genres, count above threshold for fuzzy pairs (using
    case-variant count as a fast proxy since fuzzy matching is O(n²)),
    % of files with no genre.

    Note: summary mode does NOT run full fuzzy matching — only case-variant
    detection is performed for speed. The ``fuzzy_pair_count`` in summary
    mode reflects case-variant groups only.
    """
    total_files = _get_total_ok_files(conn)
    genre_values = _fetch_genre_values(conn, total_files)

    # For summary mode, use case-variant count as a fast proxy.
    case_variant_groups = _find_case_variants(genre_values)

    no_genre_count = _count_files_without_genre(conn)
    no_genre_pct = safe_pct(no_genre_count, total_files)

    return GenresSummaryData(
        distinct_genre_count=len(genre_values),
        fuzzy_pair_count=len(case_variant_groups),
        no_genre_pct=no_genre_pct,
    )
