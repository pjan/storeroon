"""
storeroon.reports.renderers.json_renderer — JSON output for Sprint 2 reports.

Serialises report data models to JSON files. Accepts report data models and
an output directory. Knows nothing about the database.

Each ``write_*`` function accepts a ``Path`` (output directory), a timestamp
string (for filename generation), and the corresponding report data model.
It returns a list of paths that were written.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from storeroon.reports.models import (
    AlbumConsistencyFullData,
    ArtistsFullData,
    DuplicatesFullData,
    GenresFullData,
    IdsFullData,
    IssuesFullData,
    LyricsFullData,
    OverviewFullData,
    ReplayGainFullData,
    TagCoverageFullData,
    TagFormatsFullData,
    TechnicalFullData,
)
from storeroon.reports.utils import now_filename_stamp

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_json(
    output_dir: Path,
    report_name: str,
    table_name: str,
    timestamp: str,
    data: Any,
) -> Path:
    """Write a single JSON file and return its path.

    Filename pattern: ``{report_name}_{table_name}_{YYYYMMDD_HHMMSS}.json``
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report_name}_{table_name}_{timestamp}.json"
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    return filepath


def _dc_to_dict(obj: Any) -> Any:
    """Convert a dataclass (or nested structure) to a plain dict.

    Handles frozen dataclasses, lists, dicts, and primitive types.
    Uses ``dataclasses.asdict`` for dataclass instances and recurses
    manually for dicts and lists that may contain dataclasses.
    """
    try:
        return asdict(obj)
    except TypeError:
        if isinstance(obj, dict):
            return {k: _dc_to_dict(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_dc_to_dict(item) for item in obj]
        return obj


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


def write_overview(
    output_dir: Path,
    data: OverviewFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the collection overview report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Totals.
    totals = {
        "total_tracks": data.totals.total_tracks,
        "total_artists": data.totals.total_artists,
        "total_albums": data.totals.total_albums,
        "total_duration_seconds": data.totals.total_duration_seconds,
        "total_size_bytes": data.totals.total_size_bytes,
    }
    written.append(_write_json(output_dir, "report_overview", "totals", ts, totals))

    # By release type.
    by_type = [
        {
            "release_type": rt.release_type,
            "track_count": rt.track_count,
            "album_count": rt.album_count,
            "total_size_bytes": rt.total_size_bytes,
            "total_duration_seconds": rt.total_duration_seconds,
            "avg_album_duration_seconds": rt.avg_album_duration_seconds,
            "avg_track_duration_seconds": rt.avg_track_duration_seconds,
        }
        for rt in data.by_release_type
    ]
    written.append(
        _write_json(output_dir, "report_overview", "by_release_type", ts, by_type)
    )

    return written


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


def _bucket_list(buckets: list[Any]) -> list[dict[str, Any]]:
    """Convert a list of BucketCount dataclasses to plain dicts."""
    return [
        {"label": b.label, "count": b.count, "percentage": b.percentage}
        for b in buckets
    ]


def write_technical(
    output_dir: Path,
    data: TechnicalFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the technical quality report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Distributions.
    distributions = {
        "total_files": data.total_files,
        "sample_rate": _bucket_list(data.sample_rate_distribution),
        "bit_depth": _bucket_list(data.bit_depth_distribution),
        "channels": _bucket_list(data.channel_distribution),
        "bitrate_kbps": _bucket_list(data.bitrate_distribution),
        "file_size": _bucket_list(data.file_size_distribution),
        "duration": _bucket_list(data.duration_distribution),
    }
    written.append(
        _write_json(output_dir, "report_technical", "distributions", ts, distributions)
    )

    # Outliers.
    outliers = [
        {
            "path": o.path,
            "duration_seconds": o.duration_seconds,
            "outlier_type": o.outlier_type,
            "albumartist": o.albumartist,
            "album": o.album,
            "title": o.title,
        }
        for o in data.duration_outliers
    ]
    written.append(
        _write_json(output_dir, "report_technical", "outliers", ts, outliers)
    )

    # Vendors.
    vendors = [
        {
            "vendor_string": v.vendor_string,
            "count": v.count,
            "is_suspicious": v.is_suspicious,
        }
        for v in data.vendors
    ]
    written.append(_write_json(output_dir, "report_technical", "vendors", ts, vendors))

    # Missing MD5.
    missing_md5 = [
        {
            "album_dir": a.album_dir,
            "albumartist": a.albumartist,
            "album": a.album,
            "missing_count": a.missing_count,
            "total_count": a.total_count,
        }
        for a in data.missing_md5_albums
    ]
    written.append(
        _write_json(output_dir, "report_technical", "missing_md5", ts, missing_md5)
    )

    return written


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


def _coverage_rows(rows: list[Any]) -> list[dict[str, Any]]:
    """Convert TagCoverageRow instances to plain dicts."""
    return [
        {
            "tag_key": r.tag_key,
            "present_nonempty_count": r.present_nonempty_count,
            "present_nonempty_pct": r.present_nonempty_pct,
            "present_empty_count": r.present_empty_count,
            "present_empty_pct": r.present_empty_pct,
            "absent_count": r.absent_count,
            "absent_pct": r.absent_pct,
        }
        for r in rows
    ]


def write_tag_coverage(
    output_dir: Path,
    data: TagCoverageFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the tag coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Per-group coverage files.
    for suffix, group_data in [
        ("coverage_required", data.required_coverage),
        ("coverage_recommended", data.recommended_coverage),
        ("coverage_mb", data.musicbrainz_coverage),
        ("coverage_discogs", data.discogs_coverage),
        ("coverage_other", data.other_coverage),
    ]:
        written.append(
            _write_json(
                output_dir,
                "report_tags",
                suffix,
                ts,
                _coverage_rows(group_data),
            )
        )

    # Aliases.
    aliases = [
        {
            "canonical_key": r.canonical_key,
            "alias_key": r.alias_key,
            "files_using_alias": r.files_using_alias,
            "files_using_alias_pct": r.files_using_alias_pct,
        }
        for r in data.alias_usage
    ]
    written.append(_write_json(output_dir, "report_tags", "aliases", ts, aliases))

    # Full inventory.
    inventory = [
        {
            "tag_key_upper": r.tag_key_upper,
            "file_count": r.file_count,
            "coverage_pct": r.coverage_pct,
            "classification": r.classification,
        }
        for r in data.full_inventory
    ]
    written.append(_write_json(output_dir, "report_tags", "inventory", ts, inventory))

    # Unknown keys.
    unknown = [
        {
            "tag_key_upper": r.tag_key_upper,
            "file_count": r.file_count,
            "coverage_pct": r.coverage_pct,
        }
        for r in data.unknown_keys
    ]
    written.append(_write_json(output_dir, "report_tags", "unknown", ts, unknown))

    return written


# =========================================================================
# Report 5 — Tag format quality
# =========================================================================


def write_tag_formats(
    output_dir: Path,
    data: TagFormatsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the tag format quality report.

    One file per validated field group:
        - report_tag_formats_date.json (DATE + ORIGINALDATE)
        - report_tag_formats_tracknumber.json (TRACKNUMBER + DISCNUMBER + TOTALDISCS)
        - report_tag_formats_isrc.json (ISRC)
        - report_tag_formats_mbids.json (all MusicBrainz UUID fields)
    """
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    date_sections = [
        s for s in data.sections if s.field_name in ("DATE", "ORIGINALDATE")
    ]
    tracknumber_sections = [
        s
        for s in data.sections
        if s.field_name in ("TRACKNUMBER", "DISCNUMBER", "TOTALDISCS")
    ]
    isrc_sections = [s for s in data.sections if s.field_name == "ISRC"]
    mbid_sections = [
        s for s in data.sections if s.field_name.startswith("MUSICBRAINZ_")
    ]

    def _section_to_dict(section: Any) -> dict[str, Any]:
        result: dict[str, Any] = {
            "field_name": section.field_name,
            "summary": {
                "valid_count": section.summary.valid_count,
                "valid_pct": section.summary.valid_pct,
                "invalid_count": section.summary.invalid_count,
                "invalid_pct": section.summary.invalid_pct,
                "absent_count": section.summary.absent_count,
                "absent_pct": section.summary.absent_pct,
            },
            "invalid_values": [
                {"value": iv.value, "count": iv.count} for iv in section.invalid_values
            ],
            "invalid_values_total": section.invalid_values_total,
        }
        if section.extra:
            result["extra"] = {}
            for key, rows in section.extra.items():
                result["extra"][key] = [
                    {
                        "precision": r.precision,
                        "count": r.count,
                        "percentage": r.percentage,
                    }
                    for r in rows
                ]
        return result

    for suffix, sections in [
        ("date", date_sections),
        ("tracknumber", tracknumber_sections),
        ("isrc", isrc_sections),
        ("mbids", mbid_sections),
    ]:
        if sections:
            written.append(
                _write_json(
                    output_dir,
                    "report_tag_formats",
                    suffix,
                    ts,
                    [_section_to_dict(s) for s in sections],
                )
            )

    return written


# =========================================================================
# Report 6 — Intra-album consistency
# =========================================================================


def write_album_consistency(
    output_dir: Path,
    data: AlbumConsistencyFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the album consistency report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Field violations.
    fields = [
        {
            "album_dir": v.album_dir,
            "field_name": v.field_name,
            "distinct_values": v.distinct_values,
            "track_counts_per_value": v.track_counts_per_value,
            "null_track_count": v.null_track_count,
        }
        for v in data.field_violations
    ]
    written.append(
        _write_json(output_dir, "report_album_consistency", "fields", ts, fields)
    )

    # Numbering violations.
    numbering = [
        {
            "album_dir": v.album_dir,
            "check_type": v.check_type,
            "description": v.description,
        }
        for v in data.numbering_violations
    ]
    written.append(
        _write_json(output_dir, "report_album_consistency", "numbering", ts, numbering)
    )

    # Summary.
    summary = [
        {"check_type": s.check_type, "album_count": s.album_count}
        for s in data.summary_by_type
    ]
    written.append(
        _write_json(output_dir, "report_album_consistency", "summary", ts, summary)
    )

    return written


# =========================================================================
# Report 7 — External ID coverage and integrity
# =========================================================================


def write_ids(
    output_dir: Path,
    data: IdsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the external IDs report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    for prefix, section in [
        ("mb", data.musicbrainz),
        ("discogs", data.discogs),
    ]:
        # Coverage.
        coverage = [
            {
                "tag_key": r.tag_key,
                "valid_count": r.valid_count,
                "valid_pct": r.valid_pct,
                "malformed_count": r.malformed_count,
                "malformed_pct": r.malformed_pct,
                "absent_count": r.absent_count,
                "absent_pct": r.absent_pct,
            }
            for r in section.coverage
        ]
        written.append(
            _write_json(output_dir, "report_ids", f"{prefix}_coverage", ts, coverage)
        )

        # Partial albums.
        partial = [
            {
                "album_dir": pa.album_dir,
                "tracks_with_id": pa.tracks_with_id,
                "tracks_without_id": pa.tracks_without_id,
                "total_tracks": pa.total_tracks,
            }
            for pa in section.partial_albums
        ]
        written.append(
            _write_json(output_dir, "report_ids", f"{prefix}_partial", ts, partial)
        )

        # Duplicates.
        duplicates = [
            {
                "id_value": d.id_value,
                "file_count": d.file_count,
                "same_directory": d.same_directory,
                "file_paths": d.file_paths,
            }
            for d in section.duplicate_ids
        ]
        written.append(
            _write_json(
                output_dir, "report_ids", f"{prefix}_duplicates", ts, duplicates
            )
        )

        # Backfill.
        backfill = None
        if section.backfill:
            bf = section.backfill
            backfill = {
                "description": bf.description,
                "affected_tracks": bf.affected_tracks,
                "distinct_source_ids": bf.distinct_source_ids,
            }
        written.append(
            _write_json(output_dir, "report_ids", f"{prefix}_backfill", ts, backfill)
        )

    return written


# =========================================================================
# Report 8 — Duplicates
# =========================================================================


def write_duplicates(
    output_dir: Path,
    data: DuplicatesFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the duplicates report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Exact.
    exact = [
        {
            "checksum": g.checksum,
            "copy_count": g.copy_count,
            "paths": g.paths,
        }
        for g in data.exact
    ]
    written.append(_write_json(output_dir, "report_duplicates", "exact", ts, exact))

    # MBID.
    mbid = [
        {
            "mbid": g.mbid,
            "file_count": g.file_count,
            "same_directory": g.same_directory,
            "files": [
                {"path": f.path, "album": f.album, "date": f.date} for f in g.files
            ],
        }
        for g in data.mbid
    ]
    written.append(_write_json(output_dir, "report_duplicates", "mbid", ts, mbid))

    # Probable.
    probable = [
        {
            "albumartist": g.albumartist,
            "album": g.album,
            "discnumber": g.discnumber,
            "tracknumber": g.tracknumber,
            "file_count": g.file_count,
            "paths": g.paths,
            "checksums": g.checksums,
        }
        for g in data.probable
    ]
    written.append(
        _write_json(output_dir, "report_duplicates", "probable", ts, probable)
    )

    return written


# =========================================================================
# Report 9 — Scan issues
# =========================================================================


def write_issues(
    output_dir: Path,
    data: IssuesFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the scan issues report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Summary pivot.
    summary = [
        {
            "severity": r.severity,
            "issue_type": r.issue_type,
            "count": r.count,
        }
        for r in data.pivot
    ]
    written.append(_write_json(output_dir, "report_issues", "summary", ts, summary))

    # By album.
    by_album = [
        {"album_dir": r.album_dir, "issue_count": r.issue_count} for r in data.by_album
    ]
    written.append(_write_json(output_dir, "report_issues", "by_album", ts, by_album))

    # Full detail.
    detail: list[dict[str, Any]] = []
    for issue_type, details in sorted(data.by_type.items()):
        for d in details:
            detail.append(
                {
                    "issue_type": d.issue_type,
                    "severity": d.severity,
                    "file_path": d.file_path,
                    "description": d.description,
                    "details": d.details,
                }
            )
    written.append(_write_json(output_dir, "report_issues", "detail", ts, detail))

    return written


# =========================================================================
# Report 10 — Artist name consistency
# =========================================================================


def write_artists(
    output_dir: Path,
    data: ArtistsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the artist consistency report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # ALBUMARTIST values.
    albumartist = [
        {
            "value": v.value,
            "track_count": v.track_count,
            "album_count": v.album_count,
        }
        for v in data.albumartist_values
    ]
    written.append(
        _write_json(output_dir, "report_artists", "albumartist", ts, albumartist)
    )

    # ALBUMARTIST fuzzy pairs (including case variants).
    aa_fuzzy: list[dict[str, Any]] = []
    for g in data.albumartist_case_variants:
        for i, a in enumerate(g.variants):
            for b in g.variants[i + 1 :]:
                aa_fuzzy.append(
                    {
                        "name_a": a,
                        "name_b": b,
                        "similarity": 1.0,
                        "type": "case_variant",
                        "count_a": g.total_track_count,
                        "count_b": g.total_track_count,
                    }
                )
    for p in data.albumartist_fuzzy_pairs:
        aa_fuzzy.append(
            {
                "name_a": p.name_a,
                "name_b": p.name_b,
                "similarity": p.similarity,
                "type": "fuzzy",
                "count_a": p.count_a,
                "count_b": p.count_b,
            }
        )
    written.append(
        _write_json(output_dir, "report_artists", "albumartist_fuzzy", ts, aa_fuzzy)
    )

    # ARTIST values.
    artist = [
        {
            "value": v.value,
            "track_count": v.track_count,
            "album_count": v.album_count,
        }
        for v in data.artist_values
    ]
    written.append(_write_json(output_dir, "report_artists", "artist", ts, artist))

    # ARTIST fuzzy pairs.
    a_fuzzy: list[dict[str, Any]] = []
    for g in data.artist_case_variants:
        for i, a in enumerate(g.variants):
            for b in g.variants[i + 1 :]:
                a_fuzzy.append(
                    {
                        "name_a": a,
                        "name_b": b,
                        "similarity": 1.0,
                        "type": "case_variant",
                        "count_a": g.total_track_count,
                        "count_b": g.total_track_count,
                    }
                )
    for p in data.artist_fuzzy_pairs:
        a_fuzzy.append(
            {
                "name_a": p.name_a,
                "name_b": p.name_b,
                "similarity": p.similarity,
                "type": "fuzzy",
                "count_a": p.count_a,
                "count_b": p.count_b,
            }
        )
    written.append(
        _write_json(output_dir, "report_artists", "artist_fuzzy", ts, a_fuzzy)
    )

    return written


# =========================================================================
# Report 11 — Genre analysis
# =========================================================================


def write_genres(
    output_dir: Path,
    data: GenresFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the genre analysis report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Genre values.
    values = [
        {
            "value": gv.value,
            "file_count": gv.file_count,
            "file_pct": gv.file_pct,
        }
        for gv in data.genre_values
    ]
    written.append(_write_json(output_dir, "report_genres", "values", ts, values))

    # Fuzzy pairs.
    fuzzy = [
        {
            "name_a": p.name_a,
            "name_b": p.name_b,
            "similarity": p.similarity,
            "count_a": p.count_a,
            "count_b": p.count_b,
        }
        for p in data.fuzzy_pairs
    ]
    written.append(_write_json(output_dir, "report_genres", "fuzzy", ts, fuzzy))

    # Missing genre by artist.
    missing = [
        {"artist": a.artist, "missing_count": a.missing_count}
        for a in data.no_genre_by_artist
    ]
    written.append(_write_json(output_dir, "report_genres", "missing", ts, missing))

    return written


# =========================================================================
# Report 12 — Lyrics coverage
# =========================================================================


def write_lyrics(
    output_dir: Path,
    data: LyricsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the lyrics coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Overall.
    o = data.overall
    overall = {
        "total_files": data.total_files,
        "with_lyrics_count": o.with_lyrics_count,
        "with_lyrics_pct": o.with_lyrics_pct,
        "empty_lyrics_count": o.empty_lyrics_count,
        "empty_lyrics_pct": o.empty_lyrics_pct,
        "no_lyrics_count": o.no_lyrics_count,
        "no_lyrics_pct": o.no_lyrics_pct,
        "lyrics_key_count": o.lyrics_key_count,
        "unsyncedlyrics_key_count": o.unsyncedlyrics_key_count,
    }
    written.append(_write_json(output_dir, "report_lyrics", "overall", ts, overall))

    # By artist.
    by_artist = [
        {
            "artist": a.name,
            "with_lyrics": a.with_lyrics,
            "total": a.total,
            "coverage_pct": a.coverage_pct,
        }
        for a in data.by_artist
    ]
    written.append(_write_json(output_dir, "report_lyrics", "by_artist", ts, by_artist))

    # By album.
    by_album = [
        {
            "album_dir": a.name,
            "with_lyrics": a.with_lyrics,
            "total": a.total,
            "coverage_pct": a.coverage_pct,
        }
        for a in data.by_album
    ]
    written.append(_write_json(output_dir, "report_lyrics", "by_album", ts, by_album))

    return written


# =========================================================================
# Report 13 — ReplayGain coverage
# =========================================================================


def write_replaygain(
    output_dir: Path,
    data: ReplayGainFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write JSON files for the ReplayGain coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Coverage.
    coverage = [
        {
            "tag_key": r.tag_key,
            "valid_count": r.valid_count,
            "valid_pct": r.valid_pct,
            "malformed_count": r.malformed_count,
            "malformed_pct": r.malformed_pct,
            "absent_count": r.absent_count,
            "absent_pct": r.absent_pct,
        }
        for r in data.coverage
    ]
    written.append(
        _write_json(output_dir, "report_replaygain", "coverage", ts, coverage)
    )

    # Partial albums.
    partial = [
        {
            "album_dir": pa.album_dir,
            "tracks_with_rg": pa.tracks_with_rg,
            "tracks_without_rg": pa.tracks_without_rg,
            "total_tracks": pa.total_tracks,
        }
        for pa in data.partial_albums
    ]
    written.append(
        _write_json(output_dir, "report_replaygain", "partial_albums", ts, partial)
    )

    # Outliers.
    outliers = [
        {
            "path": o.path,
            "tag_key": o.tag_key,
            "value": o.value,
            "parsed_db": o.parsed_db,
        }
        for o in data.outliers
    ]
    written.append(
        _write_json(output_dir, "report_replaygain", "outliers", ts, outliers)
    )

    return written
