"""
storeroon.reports.renderers.csv_renderer — CSV output for Sprint 2 reports.

Writes one CSV file per logical table within a report. Accepts report data
models and an output directory. Knows nothing about the database.

Each ``write_*`` function accepts a ``Path`` (output directory), a timestamp
string (for filename generation), and the corresponding report data model.
It returns a list of paths that were written.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Sequence

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


def _write_csv(
    output_dir: Path,
    report_name: str,
    table_name: str,
    timestamp: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> Path:
    """Write a single CSV file and return its path.

    Filename pattern: ``{report_name}_{table_name}_{YYYYMMDD_HHMMSS}.csv``
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report_name}_{table_name}_{timestamp}.csv"
    filepath = output_dir / filename

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return filepath


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


def write_overview(
    output_dir: Path,
    data: OverviewFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the collection overview report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Table 1: Totals.
    t = data.totals
    written.append(
        _write_csv(
            output_dir,
            "report_overview",
            "totals",
            ts,
            ["metric", "value"],
            [
                ["total_tracks", t.total_tracks],
                ["total_artists", t.total_artists],
                ["total_albums", t.total_albums],
                ["total_duration_seconds", f"{t.total_duration_seconds:.2f}"],
                ["total_size_bytes", t.total_size_bytes],
            ],
        )
    )

    # Table 2: By release type.
    written.append(
        _write_csv(
            output_dir,
            "report_overview",
            "by_release_type",
            ts,
            [
                "release_type",
                "track_count",
                "album_count",
                "total_size_bytes",
                "total_duration_seconds",
                "avg_album_duration_seconds",
                "avg_track_duration_seconds",
            ],
            [
                [
                    rt.release_type,
                    rt.track_count,
                    rt.album_count,
                    rt.total_size_bytes,
                    f"{rt.total_duration_seconds:.2f}",
                    f"{rt.avg_album_duration_seconds:.2f}",
                    f"{rt.avg_track_duration_seconds:.2f}",
                ]
                for rt in data.by_release_type
            ],
        )
    )

    return written


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


def write_technical(
    output_dir: Path,
    data: TechnicalFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the technical quality report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # All distributions in one file.
    dist_rows: list[list[Any]] = []
    for dist_name, buckets in [
        ("sample_rate", data.sample_rate_distribution),
        ("bit_depth", data.bit_depth_distribution),
        ("channels", data.channel_distribution),
        ("bitrate_kbps", data.bitrate_distribution),
        ("file_size", data.file_size_distribution),
        ("duration", data.duration_distribution),
    ]:
        for b in buckets:
            dist_rows.append([dist_name, b.label, b.count, f"{b.percentage:.2f}"])

    written.append(
        _write_csv(
            output_dir,
            "report_technical",
            "distributions",
            ts,
            ["distribution", "bucket", "count", "percentage"],
            dist_rows,
        )
    )

    # Outliers.
    written.append(
        _write_csv(
            output_dir,
            "report_technical",
            "outliers",
            ts,
            [
                "path",
                "duration_seconds",
                "outlier_type",
                "albumartist",
                "album",
                "title",
            ],
            [
                [
                    o.path,
                    f"{o.duration_seconds:.3f}",
                    o.outlier_type,
                    o.albumartist,
                    o.album,
                    o.title,
                ]
                for o in data.duration_outliers
            ],
        )
    )

    # Vendors.
    written.append(
        _write_csv(
            output_dir,
            "report_technical",
            "vendors",
            ts,
            ["vendor_string", "count", "is_suspicious"],
            [[v.vendor_string, v.count, v.is_suspicious] for v in data.vendors],
        )
    )

    # Missing MD5.
    written.append(
        _write_csv(
            output_dir,
            "report_technical",
            "missing_md5",
            ts,
            ["album_dir", "albumartist", "album", "missing_count", "total_count"],
            [
                [a.album_dir, a.albumartist, a.album, a.missing_count, a.total_count]
                for a in data.missing_md5_albums
            ],
        )
    )

    return written


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


def write_tag_coverage(
    output_dir: Path,
    data: TagCoverageFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the tag coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    coverage_headers = [
        "tag_key",
        "present_nonempty_count",
        "present_nonempty_pct",
        "present_empty_count",
        "present_empty_pct",
        "absent_count",
        "absent_pct",
    ]

    def _coverage_rows(rows):
        return [
            [
                r.tag_key,
                r.present_nonempty_count,
                f"{r.present_nonempty_pct:.2f}",
                r.present_empty_count,
                f"{r.present_empty_pct:.2f}",
                r.absent_count,
                f"{r.absent_pct:.2f}",
            ]
            for r in rows
        ]

    # Per-group coverage files.
    for suffix, group_data in [
        ("required", data.required_coverage),
        ("recommended", data.recommended_coverage),
        ("mb", data.musicbrainz_coverage),
        ("discogs", data.discogs_coverage),
        ("other", data.other_coverage),
    ]:
        written.append(
            _write_csv(
                output_dir,
                "report_tags",
                f"coverage_{suffix}",
                ts,
                coverage_headers,
                _coverage_rows(group_data),
            )
        )

    # Aliases.
    written.append(
        _write_csv(
            output_dir,
            "report_tags",
            "aliases",
            ts,
            [
                "canonical_key",
                "alias_key",
                "files_using_alias",
                "files_using_alias_pct",
            ],
            [
                [
                    r.canonical_key,
                    r.alias_key,
                    r.files_using_alias,
                    f"{r.files_using_alias_pct:.2f}",
                ]
                for r in data.alias_usage
            ],
        )
    )

    # Full inventory.
    written.append(
        _write_csv(
            output_dir,
            "report_tags",
            "inventory",
            ts,
            ["tag_key_upper", "file_count", "coverage_pct", "classification"],
            [
                [
                    r.tag_key_upper,
                    r.file_count,
                    f"{r.coverage_pct:.2f}",
                    r.classification,
                ]
                for r in data.full_inventory
            ],
        )
    )

    # Unknown keys.
    written.append(
        _write_csv(
            output_dir,
            "report_tags",
            "unknown",
            ts,
            ["tag_key_upper", "file_count", "coverage_pct"],
            [
                [r.tag_key_upper, r.file_count, f"{r.coverage_pct:.2f}"]
                for r in data.unknown_keys
            ],
        )
    )

    return written


# =========================================================================
# Report 5 — Tag format quality
# =========================================================================


def write_tag_formats(
    output_dir: Path,
    data: TagFormatsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the tag format quality report.

    One file per validated field group:
        - report_tag_formats_date.csv (DATE + ORIGINALDATE)
        - report_tag_formats_tracknumber.csv (TRACKNUMBER)
        - report_tag_formats_isrc.csv (ISRC)
        - report_tag_formats_mbids.csv (all MusicBrainz UUID fields)
    """
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    headers = [
        "field_name",
        "valid_count",
        "valid_pct",
        "invalid_count",
        "invalid_pct",
        "absent_count",
        "absent_pct",
    ]

    # Group sections by output file.
    date_sections = [
        s for s in data.sections if s.field_name in ("DATE", "ORIGINALDATE")
    ]
    tracknumber_sections = [s for s in data.sections if s.field_name == "TRACKNUMBER"]
    disc_sections = [
        s for s in data.sections if s.field_name in ("DISCNUMBER", "TOTALDISCS")
    ]
    isrc_sections = [s for s in data.sections if s.field_name == "ISRC"]
    mbid_sections = [
        s for s in data.sections if s.field_name.startswith("MUSICBRAINZ_")
    ]

    def _section_rows(sections):
        rows = []
        for section in sections:
            s = section.summary
            rows.append(
                [
                    s.field_name,
                    s.valid_count,
                    f"{s.valid_pct:.2f}",
                    s.invalid_count,
                    f"{s.invalid_pct:.2f}",
                    s.absent_count,
                    f"{s.absent_pct:.2f}",
                ]
            )
        return rows

    for suffix, sections in [
        ("date", date_sections),
        ("tracknumber", tracknumber_sections + disc_sections),
        ("isrc", isrc_sections),
        ("mbids", mbid_sections),
    ]:
        if sections:
            written.append(
                _write_csv(
                    output_dir,
                    "report_tag_formats",
                    suffix,
                    ts,
                    headers,
                    _section_rows(sections),
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
    """Write CSV files for the album consistency report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Field consistency violations.
    written.append(
        _write_csv(
            output_dir,
            "report_album_consistency",
            "fields",
            ts,
            [
                "album_dir",
                "field_name",
                "distinct_values",
                "track_counts_per_value",
                "null_track_count",
            ],
            [
                [
                    v.album_dir,
                    v.field_name,
                    " | ".join(v.distinct_values),
                    " | ".join(
                        f"{val}={v.track_counts_per_value.get(val, '?')}"
                        for val in v.distinct_values
                    ),
                    v.null_track_count,
                ]
                for v in data.field_violations
            ],
        )
    )

    # Track numbering violations.
    written.append(
        _write_csv(
            output_dir,
            "report_album_consistency",
            "numbering",
            ts,
            ["album_dir", "check_type", "description"],
            [
                [v.album_dir, v.check_type, v.description]
                for v in data.numbering_violations
            ],
        )
    )

    # Summary by violation type.
    written.append(
        _write_csv(
            output_dir,
            "report_album_consistency",
            "summary",
            ts,
            ["check_type", "album_count"],
            [[s.check_type, s.album_count] for s in data.summary_by_type],
        )
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
    """Write CSV files for the external IDs report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    coverage_headers = [
        "tag_key",
        "valid_count",
        "valid_pct",
        "malformed_count",
        "malformed_pct",
        "absent_count",
        "absent_pct",
    ]

    partial_headers = [
        "album_dir",
        "tracks_with_id",
        "tracks_without_id",
        "total_tracks",
    ]

    duplicate_headers = [
        "id_value",
        "file_count",
        "same_directory",
        "file_paths",
    ]

    backfill_headers = [
        "description",
        "affected_tracks",
        "distinct_source_ids",
    ]

    for prefix, section in [
        ("mb", data.musicbrainz),
        ("discogs", data.discogs),
    ]:
        # Coverage.
        written.append(
            _write_csv(
                output_dir,
                "report_ids",
                f"{prefix}_coverage",
                ts,
                coverage_headers,
                [
                    [
                        r.tag_key,
                        r.valid_count,
                        f"{r.valid_pct:.2f}",
                        r.malformed_count,
                        f"{r.malformed_pct:.2f}",
                        r.absent_count,
                        f"{r.absent_pct:.2f}",
                    ]
                    for r in section.coverage
                ],
            )
        )

        # Partial.
        written.append(
            _write_csv(
                output_dir,
                "report_ids",
                f"{prefix}_partial",
                ts,
                partial_headers,
                [
                    [
                        pa.album_dir,
                        pa.tracks_with_id,
                        pa.tracks_without_id,
                        pa.total_tracks,
                    ]
                    for pa in section.partial_albums
                ],
            )
        )

        # Duplicates.
        written.append(
            _write_csv(
                output_dir,
                "report_ids",
                f"{prefix}_duplicates",
                ts,
                duplicate_headers,
                [
                    [
                        d.id_value,
                        d.file_count,
                        d.same_directory,
                        " | ".join(d.file_paths),
                    ]
                    for d in section.duplicate_ids
                ],
            )
        )

        # Backfill.
        backfill_rows = []
        if section.backfill:
            bf = section.backfill
            backfill_rows.append(
                [
                    bf.description,
                    bf.affected_tracks,
                    bf.distinct_source_ids,
                ]
            )
        written.append(
            _write_csv(
                output_dir,
                "report_ids",
                f"{prefix}_backfill",
                ts,
                backfill_headers,
                backfill_rows,
            )
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
    """Write CSV files for the duplicates report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Exact duplicates.
    written.append(
        _write_csv(
            output_dir,
            "report_duplicates",
            "exact",
            ts,
            ["checksum", "copy_count", "paths"],
            [[g.checksum, g.copy_count, " | ".join(g.paths)] for g in data.exact],
        )
    )

    # MBID duplicates.
    mbid_rows: list[list[Any]] = []
    for g in data.mbid:
        for f in g.files:
            mbid_rows.append(
                [
                    g.mbid,
                    g.file_count,
                    g.same_directory,
                    f.path,
                    f.album,
                    f.date,
                ]
            )
    written.append(
        _write_csv(
            output_dir,
            "report_duplicates",
            "mbid",
            ts,
            ["mbid", "group_file_count", "same_directory", "path", "album", "date"],
            mbid_rows,
        )
    )

    # Probable duplicates.
    written.append(
        _write_csv(
            output_dir,
            "report_duplicates",
            "probable",
            ts,
            [
                "albumartist",
                "album",
                "discnumber",
                "tracknumber",
                "file_count",
                "paths",
                "checksums",
            ],
            [
                [
                    g.albumartist,
                    g.album,
                    g.discnumber,
                    g.tracknumber,
                    g.file_count,
                    " | ".join(g.paths),
                    " | ".join(g.checksums),
                ]
                for g in data.probable
            ],
        )
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
    """Write CSV files for the scan issues report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Summary pivot.
    written.append(
        _write_csv(
            output_dir,
            "report_issues",
            "summary",
            ts,
            ["severity", "issue_type", "count"],
            [[r.severity, r.issue_type, r.count] for r in data.pivot],
        )
    )

    # By album.
    written.append(
        _write_csv(
            output_dir,
            "report_issues",
            "by_album",
            ts,
            ["album_dir", "issue_count"],
            [[r.album_dir, r.issue_count] for r in data.by_album],
        )
    )

    # Full detail.
    detail_rows: list[list[Any]] = []
    for issue_type, details in sorted(data.by_type.items()):
        for d in details:
            detail_rows.append(
                [
                    d.issue_type,
                    d.severity,
                    d.file_path or "",
                    d.description,
                    d.details or "",
                ]
            )
    written.append(
        _write_csv(
            output_dir,
            "report_issues",
            "detail",
            ts,
            ["issue_type", "severity", "file_path", "description", "details"],
            detail_rows,
        )
    )

    return written


# =========================================================================
# Report 10 — Artist name consistency
# =========================================================================


def write_artists(
    output_dir: Path,
    data: ArtistsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the artist consistency report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # ALBUMARTIST values.
    written.append(
        _write_csv(
            output_dir,
            "report_artists",
            "albumartist",
            ts,
            ["value", "track_count", "album_count"],
            [[v.value, v.track_count, v.album_count] for v in data.albumartist_values],
        )
    )

    # ALBUMARTIST fuzzy pairs.
    aa_fuzzy_rows: list[list[Any]] = []
    # Include case variants as well.
    for g in data.albumartist_case_variants:
        for i, a in enumerate(g.variants):
            for b in g.variants[i + 1 :]:
                aa_fuzzy_rows.append(
                    [a, b, 1.0, g.total_track_count, g.total_track_count]
                )
    for p in data.albumartist_fuzzy_pairs:
        aa_fuzzy_rows.append(
            [p.name_a, p.name_b, f"{p.similarity:.4f}", p.count_a, p.count_b]
        )

    written.append(
        _write_csv(
            output_dir,
            "report_artists",
            "albumartist_fuzzy",
            ts,
            ["name_a", "name_b", "similarity", "count_a", "count_b"],
            aa_fuzzy_rows,
        )
    )

    # ARTIST values.
    written.append(
        _write_csv(
            output_dir,
            "report_artists",
            "artist",
            ts,
            ["value", "track_count", "album_count"],
            [[v.value, v.track_count, v.album_count] for v in data.artist_values],
        )
    )

    # ARTIST fuzzy pairs.
    a_fuzzy_rows: list[list[Any]] = []
    for g in data.artist_case_variants:
        for i, a in enumerate(g.variants):
            for b in g.variants[i + 1 :]:
                a_fuzzy_rows.append(
                    [a, b, 1.0, g.total_track_count, g.total_track_count]
                )
    for p in data.artist_fuzzy_pairs:
        a_fuzzy_rows.append(
            [p.name_a, p.name_b, f"{p.similarity:.4f}", p.count_a, p.count_b]
        )

    written.append(
        _write_csv(
            output_dir,
            "report_artists",
            "artist_fuzzy",
            ts,
            ["name_a", "name_b", "similarity", "count_a", "count_b"],
            a_fuzzy_rows,
        )
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
    """Write CSV files for the genre analysis report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Genre values.
    written.append(
        _write_csv(
            output_dir,
            "report_genres",
            "values",
            ts,
            ["value", "file_count", "file_pct"],
            [
                [gv.value, gv.file_count, f"{gv.file_pct:.2f}"]
                for gv in data.genre_values
            ],
        )
    )

    # Fuzzy pairs.
    written.append(
        _write_csv(
            output_dir,
            "report_genres",
            "fuzzy",
            ts,
            ["name_a", "name_b", "similarity", "count_a", "count_b"],
            [
                [p.name_a, p.name_b, f"{p.similarity:.4f}", p.count_a, p.count_b]
                for p in data.fuzzy_pairs
            ],
        )
    )

    # Missing genre by artist.
    written.append(
        _write_csv(
            output_dir,
            "report_genres",
            "missing",
            ts,
            ["artist", "missing_count"],
            [[a.artist, a.missing_count] for a in data.no_genre_by_artist],
        )
    )

    return written


# =========================================================================
# Report 12 — Lyrics coverage
# =========================================================================


def write_lyrics(
    output_dir: Path,
    data: LyricsFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the lyrics coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Overall.
    o = data.overall
    written.append(
        _write_csv(
            output_dir,
            "report_lyrics",
            "overall",
            ts,
            ["metric", "count", "percentage"],
            [
                ["with_lyrics", o.with_lyrics_count, f"{o.with_lyrics_pct:.2f}"],
                ["empty_lyrics", o.empty_lyrics_count, f"{o.empty_lyrics_pct:.2f}"],
                ["no_lyrics", o.no_lyrics_count, f"{o.no_lyrics_pct:.2f}"],
                ["using_LYRICS_key", o.lyrics_key_count, ""],
                ["using_UNSYNCEDLYRICS_key", o.unsyncedlyrics_key_count, ""],
            ],
        )
    )

    # By artist.
    written.append(
        _write_csv(
            output_dir,
            "report_lyrics",
            "by_artist",
            ts,
            ["artist", "with_lyrics", "total", "coverage_pct"],
            [
                [a.name, a.with_lyrics, a.total, f"{a.coverage_pct:.2f}"]
                for a in data.by_artist
            ],
        )
    )

    # By album.
    written.append(
        _write_csv(
            output_dir,
            "report_lyrics",
            "by_album",
            ts,
            ["album_dir", "with_lyrics", "total", "coverage_pct"],
            [
                [a.name, a.with_lyrics, a.total, f"{a.coverage_pct:.2f}"]
                for a in data.by_album
            ],
        )
    )

    return written


# =========================================================================
# Report 13 — ReplayGain coverage
# =========================================================================


def write_replaygain(
    output_dir: Path,
    data: ReplayGainFullData,
    timestamp: str | None = None,
) -> list[Path]:
    """Write CSV files for the ReplayGain coverage report."""
    ts = timestamp or now_filename_stamp()
    written: list[Path] = []

    # Coverage.
    written.append(
        _write_csv(
            output_dir,
            "report_replaygain",
            "coverage",
            ts,
            [
                "tag_key",
                "valid_count",
                "valid_pct",
                "malformed_count",
                "malformed_pct",
                "absent_count",
                "absent_pct",
            ],
            [
                [
                    r.tag_key,
                    r.valid_count,
                    f"{r.valid_pct:.2f}",
                    r.malformed_count,
                    f"{r.malformed_pct:.2f}",
                    r.absent_count,
                    f"{r.absent_pct:.2f}",
                ]
                for r in data.coverage
            ],
        )
    )

    # Partial albums.
    written.append(
        _write_csv(
            output_dir,
            "report_replaygain",
            "partial_albums",
            ts,
            ["album_dir", "tracks_with_rg", "tracks_without_rg", "total_tracks"],
            [
                [
                    pa.album_dir,
                    pa.tracks_with_rg,
                    pa.tracks_without_rg,
                    pa.total_tracks,
                ]
                for pa in data.partial_albums
            ],
        )
    )

    # Outliers.
    written.append(
        _write_csv(
            output_dir,
            "report_replaygain",
            "outliers",
            ts,
            ["path", "tag_key", "value", "parsed_db"],
            [[o.path, o.tag_key, o.value, f"{o.parsed_db:.2f}"] for o in data.outliers],
        )
    )

    return written
