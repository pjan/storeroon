"""
FLAC importer for storeroon.

For each discovered FLAC file, this module:

1. Reads the STREAMINFO block → inserts into ``flac_properties``
2. Reads all Vorbis comment tags verbatim → inserts into ``raw_tags``
3. Inserts the file record into ``files``
4. Detects encoding anomalies; sets ``encoding_suspect`` where needed
5. Raises ``scan_issues`` rows for: unreadable files, tag read errors,
   missing required tags, empty tag values, missing ``audio_md5``

Nothing is written to the FLAC files — this is a read-only import.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from mutagen.flac import FLAC, FLACNoHeaderError
from mutagen.flac import error as FLACError

from storeroon.scanner.walker import DiscoveredFile

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ImportStats:
    """Accumulates counts across the entire import run."""

    files_processed: int = 0
    files_imported: int = 0
    files_skipped_existing: int = 0
    files_unreadable: int = 0
    tags_imported: int = 0
    issues_raised: int = 0


@dataclass(slots=True)
class _TagRecord:
    """A single tag key-value pair ready for insertion into ``raw_tags``."""

    tag_key: str
    tag_key_upper: str
    tag_value: str
    tag_index: int
    encoding_suspect: bool
    raw_bytes_hex: str | None


@dataclass(slots=True)
class _IssueRecord:
    """A scan issue ready for insertion into ``scan_issues``."""

    issue_type: str
    severity: str
    description: str
    details: str | None = None


@dataclass(slots=True)
class _FlacData:
    """All data extracted from a single FLAC file."""

    # flac_properties columns
    duration_seconds: float | None = None
    sample_rate_hz: int | None = None
    bits_per_sample: int | None = None
    channels: int | None = None
    total_samples: int | None = None
    audio_md5: str | None = None
    vendor_string: str | None = None
    approx_bitrate_kbps: int | None = None

    tags: list[_TagRecord] = field(default_factory=list)
    issues: list[_IssueRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Encoding detection
# ---------------------------------------------------------------------------


def _is_valid_utf8(value: str) -> bool:
    """Check whether a Python str round-trips cleanly through UTF-8.

    Mutagen decodes Vorbis comments as UTF-8, replacing bad bytes with the
    Unicode replacement character U+FFFD.  If the string contains U+FFFD it
    *might* be a genuine replacement character in the source, but in practice
    this almost always indicates a Latin-1 / CP-1252 encoding problem.
    """
    return "\ufffd" not in value


def _try_raw_bytes_hex(value: str) -> str | None:
    """If a tag value looks encoding-suspect, return a hex representation of
    its UTF-8 bytes for forensic inspection.  Otherwise return ``None``."""
    if _is_valid_utf8(value):
        return None
    try:
        return value.encode("utf-8").hex()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# FLAC reading
# ---------------------------------------------------------------------------


def _read_flac(filepath: Path, required_tags: Sequence[str]) -> _FlacData:
    """Read STREAMINFO and Vorbis comments from a FLAC file.

    Returns a ``_FlacData`` bundle with properties, tags, and any issues
    detected.  Never raises — all errors are captured as issues.
    """
    data = _FlacData()

    try:
        flac = FLAC(filepath)
    except FLACNoHeaderError as exc:
        data.issues.append(
            _IssueRecord(
                issue_type="file_unreadable",
                severity="critical",
                description=f"Not a valid FLAC file: {exc}",
            )
        )
        return data
    except FLACError as exc:
        data.issues.append(
            _IssueRecord(
                issue_type="file_unreadable",
                severity="critical",
                description=f"FLAC read error: {exc}",
            )
        )
        return data
    except Exception as exc:
        data.issues.append(
            _IssueRecord(
                issue_type="file_unreadable",
                severity="critical",
                description=f"Unexpected error reading FLAC: {type(exc).__name__}: {exc}",
            )
        )
        return data

    # -- STREAMINFO / audio properties ------------------------------------
    info = flac.info
    if info is not None:
        # duration: mutagen gives us info.length as a float in seconds.
        # Store at full precision (IEEE 754 double ≈ 15 significant digits).
        data.duration_seconds = info.length if info.length else None
        data.sample_rate_hz = getattr(info, "sample_rate", None)
        data.bits_per_sample = getattr(info, "bits_per_sample", None)
        data.channels = getattr(info, "channels", None)
        data.total_samples = getattr(info, "total_samples", None)

        # audio_md5: the MD5 of the raw audio stream embedded by the encoder.
        # mutagen exposes this as info.md5_signature — a 128-bit int.
        # A value of 0 means the encoder did not set it.
        md5_int = getattr(info, "md5_signature", None)
        if md5_int is not None and md5_int != 0:
            data.audio_md5 = f"{md5_int:032x}"
        else:
            data.audio_md5 = None
            data.issues.append(
                _IssueRecord(
                    issue_type="no_audio_md5",
                    severity="info",
                    description="FLAC file has no embedded audio MD5 checksum",
                )
            )

        # Approximate bitrate (kbps).  mutagen gives info.bitrate in bps.
        bitrate = getattr(info, "bitrate", None)
        if bitrate and bitrate > 0:
            data.approx_bitrate_kbps = round(bitrate / 1000)

    # Vendor string from the Vorbis comment header.
    # mutagen types flac.tags as MetadataBlock | None, but at runtime it's
    # a VorbisComment (iterable over (key, value) pairs).  We materialise
    # the pairs into a plain list so pyright stops complaining about the
    # incomplete stubs.
    raw_vc = flac.tags
    data.vendor_string = getattr(raw_vc, "vendor", None) if raw_vc is not None else None

    # Materialise tag pairs: list[tuple[str, str]] (or empty list).
    tag_pairs: list[tuple[str, str]] = list(raw_vc) if raw_vc is not None else []  # type: ignore[arg-type]

    # -- Vorbis comment tags ----------------------------------------------
    if not tag_pairs:
        data.issues.append(
            _IssueRecord(
                issue_type="tag_read_error",
                severity="error",
                description="No Vorbis comment tags found in FLAC file",
            )
        )
        # Every required tag is missing.
        for tag_name in required_tags:
            data.issues.append(
                _IssueRecord(
                    issue_type="missing_required_tag",
                    severity="error",
                    description=f"Required tag missing: {tag_name}",
                    details=json.dumps({"tag": tag_name}),
                )
            )
        return data

    # Track how many values we've seen for each key (for tag_index).
    key_counter: Counter[str] = Counter()

    for tag_key, tag_value in tag_pairs:
        key_upper = tag_key.upper()
        idx = key_counter[key_upper]
        key_counter[key_upper] += 1

        encoding_suspect = not _is_valid_utf8(tag_value)
        raw_hex = _try_raw_bytes_hex(tag_value) if encoding_suspect else None

        data.tags.append(
            _TagRecord(
                tag_key=tag_key,
                tag_key_upper=key_upper,
                tag_value=tag_value,
                tag_index=idx,
                encoding_suspect=encoding_suspect,
                raw_bytes_hex=raw_hex,
            )
        )

        if encoding_suspect:
            data.issues.append(
                _IssueRecord(
                    issue_type="tag_encoding_suspect",
                    severity="warning",
                    description=f"Suspect encoding in tag {tag_key!r}",
                    details=json.dumps(
                        {"tag": tag_key, "value_preview": tag_value[:200]}
                    ),
                )
            )

        if tag_value.strip() == "":
            data.issues.append(
                _IssueRecord(
                    issue_type="empty_tag_value",
                    severity="warning",
                    description=f"Empty value for tag {tag_key!r}",
                    details=json.dumps({"tag": tag_key, "tag_index": idx}),
                )
            )

    # -- Required tag checks ----------------------------------------------
    present_keys = set(key_counter.keys())
    for tag_name in required_tags:
        if tag_name not in present_keys:
            data.issues.append(
                _IssueRecord(
                    issue_type="missing_required_tag",
                    severity="error",
                    description=f"Required tag missing: {tag_name}",
                    details=json.dumps({"tag": tag_name}),
                )
            )

    return data


# ---------------------------------------------------------------------------
# Database insertion
# ---------------------------------------------------------------------------

_INSERT_FILE = """\
INSERT INTO files (path, filename, size_bytes, checksum_sha256, mtime_on_disk, status)
VALUES (?, ?, ?, ?, ?, ?)
"""

_INSERT_PROPERTIES = """\
INSERT INTO flac_properties
    (file_id, duration_seconds, sample_rate_hz, bits_per_sample,
     channels, total_samples, audio_md5, vendor_string, approx_bitrate_kbps)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_TAG = """\
INSERT INTO raw_tags
    (file_id, tag_key, tag_key_upper, tag_value, tag_index,
     encoding_suspect, raw_bytes_hex)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_ISSUE = """\
INSERT INTO scan_issues
    (file_id, issue_type, severity, description, details)
VALUES (?, ?, ?, ?, ?)
"""

_FILE_EXISTS = """\
SELECT id FROM files WHERE path = ?
"""


def _insert_file_record(
    conn: sqlite3.Connection,
    discovered: DiscoveredFile,
    data: _FlacData,
) -> int:
    """Insert one FLAC file plus all related rows into the database.

    Returns the ``files.id`` of the inserted row.
    """
    # Determine file status based on issues.
    status = "ok"
    for issue in data.issues:
        if issue.issue_type == "file_unreadable":
            status = "unreadable"
            break

    cur = conn.execute(
        _INSERT_FILE,
        (
            discovered.relative_path,
            discovered.filename,
            discovered.size_bytes,
            discovered.checksum_sha256,
            discovered.mtime_iso,
            status,
        ),
    )
    file_id = cur.lastrowid
    assert file_id is not None

    # Only insert properties if we successfully read the STREAMINFO block.
    if status != "unreadable":
        conn.execute(
            _INSERT_PROPERTIES,
            (
                file_id,
                data.duration_seconds,
                data.sample_rate_hz,
                data.bits_per_sample,
                data.channels,
                data.total_samples,
                data.audio_md5,
                data.vendor_string,
                data.approx_bitrate_kbps,
            ),
        )

    # Tags
    for tag in data.tags:
        conn.execute(
            _INSERT_TAG,
            (
                file_id,
                tag.tag_key,
                tag.tag_key_upper,
                tag.tag_value,
                tag.tag_index,
                1 if tag.encoding_suspect else 0,
                tag.raw_bytes_hex,
            ),
        )

    # Issues
    for issue in data.issues:
        conn.execute(
            _INSERT_ISSUE,
            (
                file_id,
                issue.issue_type,
                issue.severity,
                issue.description,
                issue.details,
            ),
        )

    return file_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def import_file(
    conn: sqlite3.Connection,
    discovered: DiscoveredFile,
    required_tags: Sequence[str],
    *,
    dry_run: bool = False,
) -> tuple[bool, _FlacData]:
    """Import a single FLAC file into the database.

    Parameters
    ----------
    conn:
        An open SQLite connection with an active transaction.
    discovered:
        A :class:`DiscoveredFile` from the walker.
    required_tags:
        Upper-cased tag names that every file must have.
    dry_run:
        When *True*, read and analyse the file but don't write to the DB.

    Returns
    -------
    tuple[bool, _FlacData]
        ``(was_imported, flac_data)``.  ``was_imported`` is *False* when the
        file was skipped (already in DB) or when ``dry_run`` is *True*.
    """
    # Check if the file is already imported.
    existing = conn.execute(_FILE_EXISTS, (discovered.relative_path,)).fetchone()
    if existing is not None:
        log.debug("File already in database, skipping: %s", discovered.relative_path)
        return False, _FlacData()

    data = _read_flac(discovered.path, required_tags)

    if dry_run:
        return False, data

    _insert_file_record(conn, discovered, data)
    return True, data


def import_batch(
    conn: sqlite3.Connection,
    files: Sequence[DiscoveredFile],
    required_tags: Sequence[str],
    *,
    dry_run: bool = False,
) -> ImportStats:
    """Import a batch of FLAC files inside a single transaction.

    Parameters
    ----------
    conn:
        An open SQLite connection.  The caller is responsible for the
        connection lifecycle; this function manages its own transaction.
    files:
        FLAC files discovered by the walker.
    required_tags:
        Upper-cased tag names that every file must have.
    dry_run:
        When *True*, analyse files but do not write anything to the database.

    Returns
    -------
    ImportStats
        Cumulative statistics for the batch.
    """
    stats = ImportStats()

    for discovered in files:
        stats.files_processed += 1

        # Skip check before reading the file (cheap DB lookup).
        existing = conn.execute(_FILE_EXISTS, (discovered.relative_path,)).fetchone()
        if existing is not None:
            stats.files_skipped_existing += 1
            log.debug("Already imported, skipping: %s", discovered.relative_path)
            continue

        data = _read_flac(discovered.path, required_tags)

        is_unreadable = any(i.issue_type == "file_unreadable" for i in data.issues)
        if is_unreadable:
            stats.files_unreadable += 1

        stats.tags_imported += len(data.tags)
        stats.issues_raised += len(data.issues)

        if dry_run:
            log.info(
                "[DRY RUN] Would import %s (%d tags, %d issues)",
                discovered.relative_path,
                len(data.tags),
                len(data.issues),
            )
            continue

        _insert_file_record(conn, discovered, data)
        stats.files_imported += 1

        if stats.files_imported % 100 == 0:
            log.info("Imported %d files so far…", stats.files_imported)

    # Commit the whole batch in one transaction (unless dry-run).
    if not dry_run:
        conn.commit()
        log.debug("Committed batch of %d files", stats.files_imported)

    return stats
