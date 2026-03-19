"""
storeroon.reports.queries.technical — Report 2: Audio technical quality.

All data comes from ``flac_properties`` joined to ``files`` (and ``raw_tags``
for album/artist context on outliers and missing-MD5 listings).

Bucketing is done in Python after fetching raw values, not in SQL.

Public API:
    full_data(conn) -> TechnicalFullData
    summary_data(conn) -> TechnicalSummaryData
"""

from __future__ import annotations

import re
import sqlite3
from collections import Counter
from collections.abc import Callable, Sequence

from storeroon.reports.models import (
    BucketCount,
    DurationOutlier,
    MissingMd5Album,
    TechnicalFullData,
    TechnicalSummaryData,
    VendorInfo,
)
from storeroon.reports.utils import safe_pct

# ---------------------------------------------------------------------------
# Suspicious vendor string patterns (case-insensitive substrings)
# ---------------------------------------------------------------------------

_SUSPICIOUS_VENDOR_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pat, re.IGNORECASE)
    for pat in (
        r"iTunes",
        r"LAME",
        r"Fraunhofer",
        r"Windows Media",
        r"AAC",
        r"MP3",
        r"Nero",
        r"QuickTime",
        r"RealAudio",
    )
)


def _is_suspicious_vendor(vendor: str) -> bool:
    """Return True if the vendor string matches any suspicious pattern."""
    for pat in _SUSPICIOUS_VENDOR_PATTERNS:
        if pat.search(vendor):
            return True
    return False


# ---------------------------------------------------------------------------
# Bucketing helpers
# ---------------------------------------------------------------------------


def _bucket_sample_rate(values: list[int]) -> list[BucketCount]:
    """Bucket sample rates into standard categories."""
    labels = [
        ("44100 Hz", lambda v: v == 44100),
        ("48000 Hz", lambda v: v == 48000),
        ("88200 Hz", lambda v: v == 88200),
        ("96000 Hz", lambda v: v == 96000),
        ("176400 Hz", lambda v: v == 176400),
        ("192000 Hz", lambda v: v == 192000),
    ]
    return _bucket_with_other(values, labels)


def _bucket_bit_depth(values: list[int]) -> list[BucketCount]:
    """Bucket bit depths into standard categories."""
    labels = [
        ("16-bit", lambda v: v == 16),
        ("24-bit", lambda v: v == 24),
        ("32-bit", lambda v: v == 32),
    ]
    return _bucket_with_other(values, labels)


def _bucket_channels(values: list[int]) -> list[BucketCount]:
    """Bucket channel counts."""
    labels = [
        ("Mono (1)", lambda v: v == 1),
        ("Stereo (2)", lambda v: v == 2),
    ]
    return _bucket_with_other(values, labels)


def _bucket_bitrate(values: list[int]) -> list[BucketCount]:
    """Bucket approximate bitrate (kbps)."""
    labels = [
        ("<400", lambda v: v < 400),
        ("400–600", lambda v: 400 <= v < 600),
        ("600–800", lambda v: 600 <= v < 800),
        ("800–1000", lambda v: 800 <= v < 1000),
        ("1000–1200", lambda v: 1000 <= v < 1200),
        ("1200–1500", lambda v: 1200 <= v < 1500),
        (">1500", lambda v: v >= 1500),
    ]
    return _bucket_exact(values, labels)


def _bucket_file_size(values: list[int]) -> list[BucketCount]:
    """Bucket file sizes (bytes → MB buckets)."""
    mb = 1024 * 1024
    labels = [
        ("<10 MB", lambda v: v < 10 * mb),
        ("10–20 MB", lambda v: 10 * mb <= v < 20 * mb),
        ("20–30 MB", lambda v: 20 * mb <= v < 30 * mb),
        ("30–50 MB", lambda v: 30 * mb <= v < 50 * mb),
        (">50 MB", lambda v: v >= 50 * mb),
    ]
    return _bucket_exact(values, labels)


def _bucket_duration(values: list[float]) -> list[BucketCount]:
    """Bucket track durations (seconds)."""
    labels = [
        ("<1 min", lambda v: v < 60),
        ("1–3 min", lambda v: 60 <= v < 180),
        ("3–5 min", lambda v: 180 <= v < 300),
        ("5–8 min", lambda v: 300 <= v < 480),
        ("8–15 min", lambda v: 480 <= v < 900),
        (">15 min", lambda v: v >= 900),
    ]
    return _bucket_exact(values, labels)


def _bucket_with_other(
    values: Sequence[int | float],
    labels: Sequence[tuple[str, Callable[[int | float], bool]]],
) -> list[BucketCount]:
    """Bucket values; anything not matched goes into 'Other'."""
    total = len(values)
    counts: dict[str, int] = {label: 0 for label, _ in labels}
    other = 0
    for v in values:
        matched = False
        for label, pred in labels:
            if pred(v):
                counts[label] += 1
                matched = True
                break
        if not matched:
            other += 1
    result = [
        BucketCount(
            label=label, count=counts[label], percentage=safe_pct(counts[label], total)
        )
        for label, _ in labels
    ]
    if other > 0:
        result.append(
            BucketCount(label="Other", count=other, percentage=safe_pct(other, total))
        )
    return result


def _bucket_exact(
    values: Sequence[int | float],
    labels: Sequence[tuple[str, Callable[[int | float], bool]]],
) -> list[BucketCount]:
    """Bucket values into mutually exclusive ordered categories (no 'Other')."""
    total = len(values)
    counts: dict[str, int] = {label: 0 for label, _ in labels}
    for v in values:
        for label, pred in labels:
            if pred(v):
                counts[label] += 1
                break
    return [
        BucketCount(
            label=label, count=counts[label], percentage=safe_pct(counts[label], total)
        )
        for label, _ in labels
    ]


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

_RAW_PROPERTIES_SQL = """
SELECT
    fp.sample_rate_hz,
    fp.bits_per_sample,
    fp.channels,
    fp.approx_bitrate_kbps,
    fp.duration_seconds,
    fp.audio_md5,
    fp.vendor_string,
    f.size_bytes,
    f.path,
    f.filename,
    f.id AS file_id
FROM flac_properties fp
JOIN files f ON f.id = fp.file_id
WHERE f.status = 'ok'
"""

_TOTAL_FILES_SQL = """
SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'
"""

# For duration outliers: join to raw_tags to get artist/album/title context.
_TAG_CONTEXT_SQL = """
SELECT
    MAX(CASE WHEN tag_key_upper = 'ALBUMARTIST' THEN tag_value END) AS albumartist,
    MAX(CASE WHEN tag_key_upper = 'ALBUM'       THEN tag_value END) AS album,
    MAX(CASE WHEN tag_key_upper = 'TITLE'       THEN tag_value END) AS title
FROM raw_tags
WHERE file_id = ?
"""

# For missing MD5: group by album directory, join to raw_tags for context.
_MISSING_MD5_SQL = """
SELECT
    SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) AS album_dir,
    COUNT(*) AS missing_count,
    (SELECT COUNT(*) FROM files f2
     WHERE SUBSTR(f2.path, 1, LENGTH(f2.path) - LENGTH(f2.filename) - 1)
           = SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1)
       AND f2.status = 'ok') AS total_count
FROM files f
JOIN flac_properties fp ON fp.file_id = f.id
WHERE f.status = 'ok'
  AND fp.audio_md5 IS NULL
GROUP BY album_dir
ORDER BY missing_count DESC
"""

_ALBUM_DIR_CONTEXT_SQL = """
SELECT
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUMARTIST' THEN rt.tag_value END) AS albumartist,
    MAX(CASE WHEN rt.tag_key_upper = 'ALBUM'       THEN rt.tag_value END) AS album
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE SUBSTR(f.path, 1, LENGTH(f.path) - LENGTH(f.filename) - 1) = ?
  AND rt.tag_key_upper IN ('ALBUMARTIST', 'ALBUM')
LIMIT 1
"""


# ---------------------------------------------------------------------------
# Internal data fetching
# ---------------------------------------------------------------------------


def _fetch_raw(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Fetch all raw property rows."""
    return conn.execute(_RAW_PROPERTIES_SQL).fetchall()


def _fetch_total_files(conn: sqlite3.Connection) -> int:
    row = conn.execute(_TOTAL_FILES_SQL).fetchone()
    return row[0] if row else 0


def _find_duration_outliers(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> list[DurationOutlier]:
    """Find tracks under 30 seconds or over 30 minutes."""
    outliers: list[DurationOutlier] = []
    for r in rows:
        dur = r["duration_seconds"]
        if dur is None:
            continue
        if dur < 30 or dur > 1800:
            ctx = conn.execute(_TAG_CONTEXT_SQL, (r["file_id"],)).fetchone()
            outlier_type = "short" if dur < 30 else "long"
            outliers.append(
                DurationOutlier(
                    path=r["path"],
                    duration_seconds=dur,
                    albumartist=ctx["albumartist"] or "" if ctx else "",
                    album=ctx["album"] or "" if ctx else "",
                    title=ctx["title"] or "" if ctx else "",
                    outlier_type=outlier_type,
                )
            )
    # Sort: short first, then long; within each group by duration.
    outliers.sort(
        key=lambda o: (0 if o.outlier_type == "short" else 1, o.duration_seconds)
    )
    return outliers


def _collect_vendors(rows: list[sqlite3.Row]) -> list[VendorInfo]:
    """Collect distinct vendor strings with counts and suspicion flags."""
    counter: Counter[str] = Counter()
    for r in rows:
        vs = r["vendor_string"]
        if vs:
            counter[vs] += 1
        else:
            counter["(none)"] += 1
    result = [
        VendorInfo(
            vendor_string=vendor,
            count=count,
            is_suspicious=_is_suspicious_vendor(vendor)
            if vendor != "(none)"
            else False,
        )
        for vendor, count in counter.most_common()
    ]
    return result


def _collect_missing_md5(
    conn: sqlite3.Connection,
    total_files: int,
) -> tuple[int, float, list[MissingMd5Album]]:
    """Count and list files/albums with missing audio_md5."""
    md5_rows = conn.execute(_MISSING_MD5_SQL).fetchall()
    total_missing = sum(r["missing_count"] for r in md5_rows)
    pct = safe_pct(total_missing, total_files)
    albums: list[MissingMd5Album] = []
    for r in md5_rows:
        album_dir = r["album_dir"] or ""
        ctx = conn.execute(_ALBUM_DIR_CONTEXT_SQL, (album_dir,)).fetchone()
        albums.append(
            MissingMd5Album(
                albumartist=ctx["albumartist"] or "" if ctx else "",
                album=ctx["album"] or "" if ctx else "",
                album_dir=album_dir,
                missing_count=r["missing_count"],
                total_count=r["total_count"],
            )
        )
    return total_missing, pct, albums


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def full_data(conn: sqlite3.Connection) -> TechnicalFullData:
    """Return the complete dataset for ``report technical``."""
    total_files = _fetch_total_files(conn)
    rows = _fetch_raw(conn)

    sample_rates = [
        r["sample_rate_hz"] for r in rows if r["sample_rate_hz"] is not None
    ]
    bit_depths = [
        r["bits_per_sample"] for r in rows if r["bits_per_sample"] is not None
    ]
    channels = [r["channels"] for r in rows if r["channels"] is not None]
    bitrates = [
        r["approx_bitrate_kbps"] for r in rows if r["approx_bitrate_kbps"] is not None
    ]
    durations = [
        r["duration_seconds"] for r in rows if r["duration_seconds"] is not None
    ]
    sizes = [r["size_bytes"] for r in rows if r["size_bytes"] is not None]

    outliers = _find_duration_outliers(conn, rows)
    vendors = _collect_vendors(rows)
    missing_md5_count, missing_md5_pct, missing_md5_albums = _collect_missing_md5(
        conn, total_files
    )

    return TechnicalFullData(
        total_files=total_files,
        sample_rate_distribution=_bucket_sample_rate(sample_rates),
        bit_depth_distribution=_bucket_bit_depth(bit_depths),
        channel_distribution=_bucket_channels(channels),
        bitrate_distribution=_bucket_bitrate(bitrates),
        file_size_distribution=_bucket_file_size(sizes),
        duration_distribution=_bucket_duration(durations),
        duration_outliers=outliers,
        vendors=vendors,
        missing_md5_count=missing_md5_count,
        missing_md5_pct=missing_md5_pct,
        missing_md5_albums=missing_md5_albums,
    )


def summary_data(conn: sqlite3.Connection) -> TechnicalSummaryData:
    """Return headline metrics only for the ``summary`` command."""
    total_files = _fetch_total_files(conn)
    rows = _fetch_raw(conn)

    sample_rates = [
        r["sample_rate_hz"] for r in rows if r["sample_rate_hz"] is not None
    ]
    bit_depths = [
        r["bits_per_sample"] for r in rows if r["bits_per_sample"] is not None
    ]
    bitrates = [
        r["approx_bitrate_kbps"] for r in rows if r["approx_bitrate_kbps"] is not None
    ]
    durations = [
        r["duration_seconds"] for r in rows if r["duration_seconds"] is not None
    ]

    outlier_count = sum(1 for d in durations if d < 30 or d > 1800)
    vendors = _collect_vendors(rows)
    suspicious_vendor_count = sum(v.count for v in vendors if v.is_suspicious)
    missing_md5_count, missing_md5_pct, _ = _collect_missing_md5(conn, total_files)

    return TechnicalSummaryData(
        total_files=total_files,
        sample_rate_distribution=_bucket_sample_rate(sample_rates),
        bit_depth_distribution=_bucket_bit_depth(bit_depths),
        bitrate_distribution=_bucket_bitrate(bitrates),
        duration_outlier_count=outlier_count,
        suspicious_vendor_count=suspicious_vendor_count,
        missing_md5_count=missing_md5_count,
        missing_md5_pct=missing_md5_pct,
    )
