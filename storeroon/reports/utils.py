"""
storeroon.reports.utils — shared helpers for reports.

Provides number/percentage/duration formatting, bar chart rendering for
terminal output, severity colour mapping for Rich, and common query helpers.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Report name registry
# ---------------------------------------------------------------------------

REPORT_NAMES: tuple[str, ...] = (
    "overview",
    "collection_issues",
    "technical",
    "key_inventory",
    "artists",
    "genres",
    "lyrics",
    "replaygain",
)


# ---------------------------------------------------------------------------
# Filter string builder (shared by CLI and server)
# ---------------------------------------------------------------------------


def build_filter_string(
    artist: str | None = None,
    album: str | None = None,
    min_severity: str | None = None,
) -> str | None:
    """Build a human-readable filter description for report output."""
    parts: list[str] = []
    if artist:
        parts.append(f"artist={artist!r}")
    if album:
        parts.append(f"album={album!r}")
    if min_severity and min_severity != "info":
        parts.append(f"min_severity={min_severity}")
    return ", ".join(parts) if parts else None

# ---------------------------------------------------------------------------
# Number formatting
# ---------------------------------------------------------------------------


def fmt_count(n: int) -> str:
    """Format an integer with thousands separators: ``1234567`` → ``1,234,567``."""
    return f"{n:,}"


def fmt_pct(value: float, decimals: int = 1) -> str:
    """Format a percentage value: ``85.1234`` → ``85.1%``."""
    return f"{value:.{decimals}f}%"


def fmt_bytes(n: int) -> str:
    """Human-friendly byte size: ``1073741824`` → ``1.00 GB``.

    Uses binary (1024-based) units.
    """
    if n < 0:
        return f"-{fmt_bytes(-n)}"
    if n < 1024:
        return f"{n} B"
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        n_f = n / 1024
        if n_f < 1024 or unit == "TiB":
            return f"{n_f:,.2f} {unit}"
        n = int(n_f)
    return f"{n:,} B"  # pragma: no cover — unreachable


def fmt_size_gb(n: int) -> str:
    """Format bytes as a simple GB value: ``1073741824`` → ``1.00 GB``."""
    gb = n / (1024**3)
    return f"{gb:,.2f} GB"


def fmt_size_mb(n: int) -> str:
    """Format bytes as MB: ``10485760`` → ``10.00 MB``."""
    mb = n / (1024**2)
    return f"{mb:,.2f} MB"


def fmt_duration_hms(seconds: float) -> str:
    """Format seconds as ``Xd Xh Xm Xs`` with sensible truncation."""
    if seconds < 0:
        return f"-{fmt_duration_hms(-seconds)}"
    total = int(round(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def fmt_duration_short(seconds: float) -> str:
    """Format seconds as ``M:SS`` or ``H:MM:SS``."""
    total = int(round(seconds))
    if total < 0:
        return f"-{fmt_duration_short(-total)}"
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# ---------------------------------------------------------------------------
# Percentage / safe division
# ---------------------------------------------------------------------------


def safe_pct(part: int, total: int) -> float:
    """Compute percentage, returning 0.0 if *total* is zero."""
    if total == 0:
        return 0.0
    return (part / total) * 100.0


def safe_div(numerator: float, denominator: float) -> float:
    """Safe division returning 0.0 on zero denominator."""
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


# ---------------------------------------------------------------------------
# Median helper
# ---------------------------------------------------------------------------


def median(values: list[float | int]) -> float:
    """Return the median of a sorted-or-unsorted list of numbers.

    Returns ``0.0`` for an empty list.
    """
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return float(s[mid])


# ---------------------------------------------------------------------------
# Terminal bar chart rendering
# ---------------------------------------------------------------------------

_BAR_CHARS = " ▏▎▍▌▋▊▉█"


def bar_chart(
    value: float,
    max_value: float,
    width: int = 30,
) -> str:
    """Render a horizontal bar using Unicode block characters.

    Parameters
    ----------
    value:
        The current value to render.
    max_value:
        The value that corresponds to a full-width bar.
    width:
        Maximum character width of the bar.

    Returns
    -------
    str
        A string of block characters representing the bar.
    """
    if max_value <= 0 or value <= 0:
        return ""
    ratio = min(value / max_value, 1.0)
    full_blocks = ratio * width
    n_full = int(full_blocks)
    remainder = full_blocks - n_full
    bar = "█" * n_full
    # Add a fractional block character if there's a meaningful remainder.
    frac_idx = int(remainder * 8)
    if frac_idx > 0 and n_full < width:
        bar += _BAR_CHARS[frac_idx]
    return bar


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------

_SEVERITY_ORDER = {"critical": 0, "error": 1, "warning": 2, "info": 3}

_SEVERITY_STYLES: dict[str, str] = {
    "critical": "bold red",
    "error": "red",
    "warning": "yellow",
    "info": "dim",
}


def severity_order(severity: str) -> int:
    """Return a sort key for severity (lower = more severe)."""
    return _SEVERITY_ORDER.get(severity.lower(), 99)


def severity_style(severity: str) -> str:
    """Return a Rich style string for the given severity level."""
    return _SEVERITY_STYLES.get(severity.lower(), "")


def severity_at_least(severity: str, min_severity: str) -> bool:
    """Return True if *severity* is at least as severe as *min_severity*."""
    return severity_order(severity) <= severity_order(min_severity)


# ---------------------------------------------------------------------------
# Album directory extraction
# ---------------------------------------------------------------------------


def album_dir_from_path(file_path: str | None) -> str:
    """Extract the parent directory from a file path string.

    This is the album boundary used throughout reports.
    Uses string operations (no filesystem access) for speed.
    Returns an empty string if the path is None or has no separator.

    >>> album_dir_from_path("Artist/Albums/2020 - Album/Artist - 2020 - Album [CAT]/01 Track.flac")
    'Artist/Albums/2020 - Album/Artist - 2020 - Album [CAT]'
    """
    if not file_path:
        return ""
    idx = file_path.rfind("/")
    if idx < 0:
        idx = file_path.rfind("\\")
    if idx < 0:
        return ""
    return file_path[:idx]


def release_type_from_path(file_path: str) -> str:
    """Extract the release type directory component (second path segment).

    Given a path like ``Artist/Albums/2020 - Title/.../01 Track.flac``,
    returns ``Albums``.  Returns ``Unknown`` if the path doesn't have
    enough segments.
    """
    # Split on forward slash (paths in the DB use forward slashes).
    parts = file_path.split("/")
    if len(parts) >= 2:
        return parts[1]
    return "Unknown"


# ---------------------------------------------------------------------------
# Timestamp formatting
# ---------------------------------------------------------------------------


def now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S UTC")


def now_filename_stamp() -> str:
    """Return a timestamp suitable for output filenames: ``YYYYMMDD_HHMMSS``."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Validation helpers (used by tag-formats and ids reports)
# ---------------------------------------------------------------------------

# UUID v4 regex (case-insensitive).
RE_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Date formats: YYYY, YYYY-MM, YYYY-MM-DD.
RE_DATE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")

# ISRC: CC-XXX-YY-NNNNN (with or without hyphens).
RE_ISRC_NOHYPHEN = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}\d{2}\d{5}$", re.IGNORECASE)
RE_ISRC_HYPHEN = re.compile(r"^[A-Z]{2}-[A-Z0-9]{3}-\d{2}-\d{5}$", re.IGNORECASE)

# Positive integer string.
RE_POSITIVE_INT = re.compile(r"^[0-9]+$")

# Track number with legacy N/T format.
RE_TRACKNUMBER_LEGACY = re.compile(r"^(\d+)/(\d+)$")


def is_valid_uuid(value: str) -> bool:
    """Check if a string is a valid UUID (v4 format)."""
    return RE_UUID.match(value.strip()) is not None


def is_valid_date(value: str) -> bool:
    """Check if a string matches YYYY, YYYY-MM, or YYYY-MM-DD format
    with reasonable range checks."""
    value = value.strip()
    m = RE_DATE.match(value)
    if not m:
        return False
    year = int(value[:4])
    if year < 1900 or year > 2030:
        return False
    if len(value) >= 7:
        month = int(value[5:7])
        if month < 1 or month > 12:
            return False
    if len(value) == 10:
        day = int(value[8:10])
        if day < 1 or day > 31:
            return False
    return True


def date_precision(value: str) -> str:
    """Classify a date value's precision.

    Returns ``'full_date'``, ``'year_month'``, ``'year_only'``, or ``'invalid'``.
    """
    value = value.strip()
    if not RE_DATE.match(value):
        return "invalid"
    if len(value) == 10:
        return "full_date"
    if len(value) == 7:
        return "year_month"
    if len(value) == 4:
        return "year_only"
    return "invalid"


def is_valid_isrc(value: str) -> bool:
    """Check if a string is a valid ISRC (with or without hyphens)."""
    value = value.strip()
    return (
        RE_ISRC_NOHYPHEN.match(value) is not None
        or RE_ISRC_HYPHEN.match(value) is not None
    )


def is_valid_discogs_id(value: str) -> bool:
    """Check if a string is a valid Discogs numeric ID.

    Must be a string of digits representing a positive integer < 100,000,000.
    """
    value = value.strip()
    if not RE_POSITIVE_INT.match(value):
        return False
    n = int(value)
    return 0 < n < 100_000_000


def parse_replaygain_db(value: str) -> float | None:
    """Parse a ReplayGain value like ``-6.23 dB`` to a float.

    Returns ``None`` if the value cannot be parsed.
    """
    value = value.strip()
    # Strip trailing " dB" suffix (case-insensitive).
    if value.lower().endswith(" db"):
        value = value[:-3].strip()
    elif value.lower().endswith("db"):
        value = value[:-2].strip()
    try:
        return float(value)
    except (ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Fuzzy matching helpers (used by artists and genres reports)
# ---------------------------------------------------------------------------


def token_sort_normalise(name: str) -> str:
    """Normalise a name for token-sort fuzzy comparison.

    Lowercase, strip leading "the ", tokenise on whitespace, sort
    alphabetically, rejoin with space.

    >>> token_sort_normalise("The Beatles")
    'beatles'
    >>> token_sort_normalise("David Bowie")
    'bowie david'
    """
    s = name.lower().strip()
    if s.startswith("the "):
        s = s[4:]
    tokens = sorted(s.split())
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# Output filename generation
# ---------------------------------------------------------------------------


def output_filename(
    report_name: str,
    table_name: str,
    ext: str,
    timestamp: str | None = None,
) -> str:
    """Generate an output filename following the Sprint 2 pattern.

    Pattern: ``{report_name}_{table_name}_{YYYYMMDD_HHMMSS}.{ext}``
    """
    ts = timestamp or now_filename_stamp()
    return f"{report_name}_{table_name}_{ts}.{ext}"


# ---------------------------------------------------------------------------
# Shared SQL constants
# ---------------------------------------------------------------------------

TOTAL_OK_FILES_SQL = "SELECT COUNT(*) AS cnt FROM files WHERE status = 'ok'"

TOTAL_OK_FILES_FILTERED_SQL = """
SELECT COUNT(DISTINCT f.id) AS cnt
FROM files f
JOIN raw_tags rt ON rt.file_id = f.id
WHERE f.status = 'ok'
  AND rt.tag_key_upper = 'ALBUMARTIST'
  AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
"""


# ---------------------------------------------------------------------------
# Issue classification helpers (used by album detail page)
# ---------------------------------------------------------------------------

_SEV_ORDER: dict[str, int] = {"critical": 0, "error": 1, "warning": 2, "info": 3}

_TRACK_ISSUE_TYPES: frozenset[str] = frozenset({
    "file_unreadable", "tag_read_error", "no_audio_md5", "duplicate_checksum",
})


def track_severity_class(track: object) -> str:
    """Return the CSS severity class for a track based on its worst issue."""
    issues = getattr(track, "issues", [])
    if not issues:
        return "sev-clean"
    worst = min(issues, key=lambda i: _SEV_ORDER.get(i.severity, 9))
    return f"sev-{worst.severity}"


def track_badge_counts(track: object) -> list[tuple[str, int]]:
    """Return (severity, count) pairs for a track's issues, omitting zeros."""
    counts: dict[str, int] = {}
    for issue in getattr(track, "issues", []):
        counts[issue.severity] = counts.get(issue.severity, 0) + 1
    return [(sev, counts[sev]) for sev in ("critical", "error", "warning", "info") if counts.get(sev, 0) > 0]


def classify_track_issues(track: object) -> dict[str, object]:
    """Classify a track's issues into display buckets.

    Returns a dict with:
    - ``track_issues``: list of non-tag issues
    - ``tag_buckets``: dict of severity_bucket → sub_type → list of field names
    - ``has_tag_issues``: bool
    """
    track_issues: list[object] = []
    tag_buckets: dict[str, dict[str, list[str]]] = {
        "required": {"missing": [], "invalid": []},
        "recommended": {"missing": [], "invalid": [], "encoding": [], "alias": []},
        "improvement": {"missing": [], "invalid": []},
    }

    for issue in getattr(track, "issues", []):
        itype = issue.issue_type
        if itype in _TRACK_ISSUE_TYPES:
            track_issues.append(issue)
        elif itype == "missing_required_tag":
            tag_buckets["required"]["missing"].append(issue.field or itype)
        elif itype == "invalid_required_tag":
            tag_buckets["required"]["invalid"].append(issue.field or itype)
        elif itype == "missing_recommended_tag":
            tag_buckets["recommended"]["missing"].append(issue.field or itype)
        elif itype == "invalid_recommended_tag":
            tag_buckets["recommended"]["invalid"].append(issue.field or itype)
        elif itype == "tag_encoding_suspect":
            tag_buckets["recommended"]["encoding"].append(issue.field or itype)
        elif itype == "alias_mismatch":
            tag_buckets["recommended"]["alias"].append(issue.field or itype)
        elif itype == "missing_other_tag":
            tag_buckets["improvement"]["missing"].append(issue.field or itype)
        elif itype == "invalid_other_tag":
            tag_buckets["improvement"]["invalid"].append(issue.field or itype)
        else:
            track_issues.append(issue)

    return {
        "track_issues": track_issues,
        "tag_buckets": tag_buckets,
        "has_tag_issues": any(
            fields for bucket in tag_buckets.values() for fields in bucket.values()
        ),
    }


def health_score_color(score: int) -> str:
    """Return the CSS color variable for a health score."""
    if score >= 80:
        return "var(--clean)"
    if score >= 50:
        return "var(--warning)"
    return "var(--critical)"
