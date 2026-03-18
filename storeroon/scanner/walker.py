"""
File walker for storeroon.

Recursively walks a collection root directory, discovers all ``.flac`` files,
and yields file records with path, size, mtime, and (optionally) SHA-256
checksum.

Non-FLAC files are also reported so the caller can flag them for review.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)

# SHA-256 is computed in 256 KiB chunks — large enough for throughput,
# small enough to keep memory modest when processing 40k files.
_HASH_CHUNK_SIZE = 256 * 1024

# File extensions that are expected in a well-maintained collection.
_KNOWN_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".flac",
        ".lrc",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".webp",
        ".cue",
        ".log",
        ".txt",
        ".nfo",
        ".m3u",
        ".m3u8",
    }
)

# Audio extensions that are NOT FLAC — candidates for conversion / removal.
_NON_FLAC_AUDIO_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".mp3",
        ".aac",
        ".m4a",
        ".ogg",
        ".opus",
        ".wma",
        ".wav",
        ".ape",
        ".wv",
        ".alac",
        ".aiff",
        ".aif",
        ".dsf",
        ".dff",
    }
)


class FileKind(Enum):
    """Classification of a discovered file."""

    FLAC = "flac"
    LRC = "lrc"
    IMAGE = "image"
    CUE_LOG = "cue_log"
    NON_FLAC_AUDIO = "non_flac_audio"
    OTHER = "other"


@dataclass(slots=True)
class DiscoveredFile:
    """A single file found during a collection walk."""

    path: Path
    relative_path: str
    filename: str
    kind: FileKind
    size_bytes: int
    mtime_iso: str
    checksum_sha256: str | None = None


def _classify(ext: str) -> FileKind:
    """Classify a file based on its lowercased extension."""
    if ext == ".flac":
        return FileKind.FLAC
    if ext == ".lrc":
        return FileKind.LRC
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}:
        return FileKind.IMAGE
    if ext in {".cue", ".log"}:
        return FileKind.CUE_LOG
    if ext in _NON_FLAC_AUDIO_EXTENSIONS:
        return FileKind.NON_FLAC_AUDIO
    return FileKind.OTHER


def _sha256(path: Path) -> str:
    """Compute the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_HASH_CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _mtime_iso(path: Path) -> str:
    """Return the file's mtime as an ISO-8601 string in UTC."""
    ts = os.path.getmtime(path)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat()


def walk_collection(
    root: Path,
    *,
    checksums: bool = True,
    flac_only: bool = False,
) -> Iterator[DiscoveredFile]:
    """Recursively walk *root* and yield a :class:`DiscoveredFile` for every
    file found.

    Parameters
    ----------
    root:
        The absolute path to the collection root directory.
    checksums:
        When *True* (the default), compute a SHA-256 checksum for every FLAC
        file.  Checksums are never computed for non-FLAC files.
    flac_only:
        When *True*, only yield FLAC files.  Useful for the import pipeline
        which only cares about FLAC.  Non-FLAC files are still logged but
        not yielded.
    """
    root = root.resolve()
    if not root.is_dir():
        log.error("Collection root does not exist or is not a directory: %s", root)
        return

    log.info("Walking collection root: %s", root)
    file_count = 0
    flac_count = 0

    # os.walk is faster than Path.rglob for large trees because it avoids
    # constructing Path objects for every intermediate result.
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        # Sort filenames for deterministic ordering.
        for fname in sorted(filenames):
            filepath = Path(dirpath) / fname
            ext = filepath.suffix.lower()
            kind = _classify(ext)

            file_count += 1

            if flac_only and kind != FileKind.FLAC:
                if kind == FileKind.NON_FLAC_AUDIO:
                    log.warning("Non-FLAC audio file: %s", filepath)
                elif kind == FileKind.OTHER and ext not in _KNOWN_EXTENSIONS:
                    log.debug("Unexpected file in collection: %s", filepath)
                continue

            try:
                stat = filepath.stat()
            except OSError as exc:
                log.error("Cannot stat file %s: %s", filepath, exc)
                continue

            relative = str(filepath.relative_to(root))
            size = stat.st_size
            mtime = _mtime_iso(filepath)

            checksum: str | None = None
            if checksums and kind == FileKind.FLAC:
                try:
                    checksum = _sha256(filepath)
                except OSError as exc:
                    log.error("Cannot compute checksum for %s: %s", filepath, exc)

            discovered = DiscoveredFile(
                path=filepath,
                relative_path=relative,
                filename=fname,
                kind=kind,
                size_bytes=size,
                mtime_iso=mtime,
                checksum_sha256=checksum,
            )

            if kind == FileKind.FLAC:
                flac_count += 1
            elif kind == FileKind.NON_FLAC_AUDIO:
                log.warning("Non-FLAC audio file: %s", filepath)
            elif kind == FileKind.OTHER and ext not in _KNOWN_EXTENSIONS:
                log.debug("Unexpected file in collection: %s", filepath)

            yield discovered

    log.info(
        "Walk complete: %d files found, %d FLAC files",
        file_count,
        flac_count,
    )
