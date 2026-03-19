"""
storeroon.reports.renderers.html_renderer — HTML output for Sprint 2 reports.

Produces a single self-contained HTML file per report subcommand using Jinja2
templates. No external dependencies — inline <style> block only, no JavaScript.
CSS uses @media (prefers-color-scheme: dark) for passive dark mode.

Each ``write_*`` function accepts a ``Path`` (output directory), a timestamp
string (for filename generation), and the corresponding report data model.
It returns a list of paths that were written.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Any

from jinja2 import Template

from storeroon.reports.models import (
    AlbumConsistencyFullData,
    ArtistsFullData,
    BucketCount,
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
from storeroon.reports.utils import (
    fmt_bytes,
    fmt_count,
    fmt_duration_hms,
    fmt_duration_short,
    fmt_pct,
    fmt_size_gb,
    now_filename_stamp,
    now_iso,
    safe_pct,
)

# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

_TEMPLATE: Template | None = None


def _get_template() -> Template:
    """Load and cache the Jinja2 HTML template from the package resources."""
    global _TEMPLATE
    if _TEMPLATE is None:
        ref = resources.files("storeroon.reports.templates").joinpath("report.html")
        html = ref.read_text(encoding="utf-8")
        _TEMPLATE = Template(html)
    return _TEMPLATE


# ---------------------------------------------------------------------------
# Data-structure helpers for the template
# ---------------------------------------------------------------------------
# The template expects a list of ``sections``, each with optional:
#   - heading (str)
#   - note (str | None)
#   - summary_cards (list[dict] | None)
#   - tables (list[dict] | None)
#   - text_blocks (list[dict] | None)
#
# Each table dict has:
#   - title (str | None)
#   - headers: list[{label, align?}]
#   - rows: list[list[{value, cls?, bar_pct?, bar_cls?}]]
#   - footer (str | None)
#   - empty_message (str | None)


def _hdr(label: str, align: str | None = None) -> dict[str, Any]:
    """Build a header cell dict."""
    d: dict[str, Any] = {"label": label}
    if align:
        d["align"] = align
    return d


def _cell(
    value: Any,
    cls: str | None = None,
    bar_pct: float | None = None,
    bar_cls: str | None = None,
) -> dict[str, Any]:
    """Build a table cell dict."""
    d: dict[str, Any] = {"value": str(value) if value is not None else ""}
    if cls:
        d["cls"] = cls
    if bar_pct is not None:
        d["bar_pct"] = min(max(bar_pct, 0.0), 100.0)
    if bar_cls:
        d["bar_cls"] = bar_cls
    return d


def _card(value: str, label: str) -> dict[str, str]:
    """Build a summary card dict."""
    return {"value": value, "label": label}


def _text(content: str, cls: str | None = None) -> dict[str, Any]:
    """Build a text block dict."""
    d: dict[str, Any] = {"content": content}
    if cls:
        d["cls"] = cls
    return d


def _table(
    title: str | None,
    headers: list[dict[str, Any]],
    rows: list[list[dict[str, Any]]],
    footer: str | None = None,
    empty_message: str | None = None,
) -> dict[str, Any]:
    return {
        "title": title,
        "headers": headers,
        "rows": rows,
        "footer": footer,
        "empty_message": empty_message or "No data.",
    }


def _section(
    heading: str,
    *,
    note: str | None = None,
    summary_cards: list[dict[str, str]] | None = None,
    tables: list[dict[str, Any]] | None = None,
    text_blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "heading": heading,
        "note": note,
        "summary_cards": summary_cards,
        "tables": tables,
        "text_blocks": text_blocks,
    }


def _bucket_table(
    title: str | None,
    buckets: list[BucketCount],
    bar_color: str | None = None,
) -> dict[str, Any]:
    """Build a table dict from a list of BucketCount instances with bar charts."""
    max_pct = max((b.percentage for b in buckets), default=1.0)
    rows: list[list[dict[str, Any]]] = []
    for b in buckets:
        rows.append(
            [
                _cell(b.label),
                _cell(fmt_count(b.count), cls="num"),
                _cell(
                    fmt_pct(b.percentage),
                    cls="num",
                    bar_pct=(b.percentage / max_pct * 100.0) if max_pct > 0 else 0,
                    bar_cls=bar_color,
                ),
            ]
        )
    return _table(
        title,
        [_hdr("Bucket"), _hdr("Count", "num"), _hdr("%", "num")],
        rows,
    )


# ---------------------------------------------------------------------------
# File writing helper
# ---------------------------------------------------------------------------


def _write_html(
    output_dir: Path,
    report_name: str,
    timestamp: str,
    title: str,
    sections: list[dict[str, Any]],
    filters: str | None = None,
) -> Path:
    """Render and write a single self-contained HTML file. Returns the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{report_name}_{timestamp}.html"
    filepath = output_dir / filename

    template = _get_template()
    html = template.render(
        title=title,
        generated_at=now_iso(),
        filters=filters or "",
        sections=sections,
    )
    filepath.write_text(html, encoding="utf-8")
    return filepath


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


def write_overview(
    output_dir: Path,
    data: OverviewFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the collection overview report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    t = data.totals
    sections.append(
        _section(
            "Collection Totals",
            summary_cards=[
                _card(fmt_count(t.total_tracks), "Tracks"),
                _card(fmt_count(t.total_artists), "Album Artists"),
                _card(fmt_count(t.total_albums), "Albums"),
                _card(fmt_duration_hms(t.total_duration_seconds), "Duration"),
                _card(fmt_size_gb(t.total_size_bytes), "Size"),
            ],
        )
    )

    if data.by_release_type:
        rows: list[list[dict[str, Any]]] = []
        for rt in data.by_release_type:
            rows.append(
                [
                    _cell(rt.release_type),
                    _cell(fmt_count(rt.track_count), cls="num"),
                    _cell(fmt_count(rt.album_count), cls="num"),
                    _cell(fmt_size_gb(rt.total_size_bytes), cls="num"),
                    _cell(fmt_duration_hms(rt.total_duration_seconds), cls="num"),
                    _cell(fmt_duration_short(rt.avg_album_duration_seconds), cls="num"),
                    _cell(fmt_duration_short(rt.avg_track_duration_seconds), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Breakdown by Release Type",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Type"),
                            _hdr("Tracks", "num"),
                            _hdr("Albums", "num"),
                            _hdr("Size", "num"),
                            _hdr("Duration", "num"),
                            _hdr("Avg Album Dur", "num"),
                            _hdr("Avg Track Dur", "num"),
                        ],
                        rows,
                    )
                ],
            )
        )

    dist = data.distribution
    sections.append(
        _section(
            "Distribution Summary",
            summary_cards=[
                _card(
                    fmt_duration_short(dist.median_track_duration_seconds),
                    "Median Track Duration",
                ),
                _card(fmt_bytes(dist.median_file_size_bytes), "Median File Size"),
                _card(f"{dist.avg_bitrate_kbps:.0f} kbps", "Average Bitrate"),
                _card(f"{dist.median_bitrate_kbps:.0f} kbps", "Median Bitrate"),
            ],
        )
    )

    return [
        _write_html(
            output_dir, "report_overview", ts, "Collection Overview", sections, filters
        )
    ]


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


def write_technical(
    output_dir: Path,
    data: TechnicalFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the technical quality report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Audio Technical Quality",
            summary_cards=[
                _card(fmt_count(data.total_files), "Total Files"),
                _card(fmt_count(len(data.duration_outliers)), "Duration Outliers"),
                _card(
                    fmt_count(sum(1 for v in data.vendors if v.is_suspicious)),
                    "Suspicious Encoders",
                ),
                _card(
                    f"{fmt_count(data.missing_md5_count)} ({fmt_pct(data.missing_md5_pct)})",
                    "Missing audio_md5",
                ),
            ],
        )
    )

    dist_tables = []
    for label, buckets in [
        ("Sample Rate Distribution", data.sample_rate_distribution),
        ("Bit Depth Distribution", data.bit_depth_distribution),
        ("Channel Distribution", data.channel_distribution),
        ("Approximate Bitrate (kbps)", data.bitrate_distribution),
        ("File Size Distribution", data.file_size_distribution),
        ("Track Duration Distribution", data.duration_distribution),
    ]:
        dist_tables.append(_bucket_table(label, buckets))
    sections.append(_section("Distributions", tables=dist_tables))

    # Outliers
    if data.duration_outliers:
        outlier_rows: list[list[dict[str, Any]]] = []
        for o in data.duration_outliers:
            otype_cls = (
                "severity-error" if o.outlier_type == "short" else "severity-warning"
            )
            outlier_rows.append(
                [
                    _cell(o.outlier_type.upper(), cls=otype_cls),
                    _cell(fmt_duration_short(o.duration_seconds), cls="num"),
                    _cell(o.albumartist),
                    _cell(o.album),
                    _cell(o.title),
                    _cell(o.path, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Duration Outliers ({len(data.duration_outliers)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Type"),
                            _hdr("Duration", "num"),
                            _hdr("Artist"),
                            _hdr("Album"),
                            _hdr("Title"),
                            _hdr("Path"),
                        ],
                        outlier_rows,
                    )
                ],
            )
        )

    # Vendors
    if data.vendors:
        vendor_rows: list[list[dict[str, Any]]] = []
        for v in data.vendors:
            flag_cls = "flag-error" if v.is_suspicious else "flag-ok"
            flag_text = "SUSPICIOUS" if v.is_suspicious else "OK"
            vendor_rows.append(
                [
                    _cell(v.vendor_string, cls="mono"),
                    _cell(fmt_count(v.count), cls="num"),
                    _cell(f'<span class="flag {flag_cls}">{flag_text}</span>'),
                ]
            )
        sections.append(
            _section(
                "Encoder Provenance",
                tables=[
                    _table(
                        None,
                        [_hdr("Vendor String"), _hdr("Count", "num"), _hdr("Status")],
                        vendor_rows,
                    )
                ],
            )
        )

    # Missing MD5
    if data.missing_md5_albums:
        md5_rows: list[list[dict[str, Any]]] = []
        for a in data.missing_md5_albums:
            md5_rows.append(
                [
                    _cell(a.albumartist),
                    _cell(a.album),
                    _cell(a.missing_count, cls="num"),
                    _cell(a.total_count, cls="num"),
                    _cell(a.album_dir, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Missing audio_md5 ({fmt_count(data.missing_md5_count)} files)",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Artist"),
                            _hdr("Album"),
                            _hdr("Missing", "num"),
                            _hdr("Total", "num"),
                            _hdr("Album Dir"),
                        ],
                        md5_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir,
            "report_technical",
            ts,
            "Audio Technical Quality",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


def write_tag_coverage(
    output_dir: Path,
    data: TagCoverageFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the tag coverage report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Tag Coverage & Key Inventory",
            summary_cards=[_card(fmt_count(data.total_files), "Total Files")],
        )
    )

    cov_headers = [
        _hdr("Tag Key"),
        _hdr("Present (non-empty)", "num"),
        _hdr("%", "num"),
        _hdr("Present (empty)", "num"),
        _hdr("%", "num"),
        _hdr("Absent", "num"),
        _hdr("%", "num"),
    ]

    for group_name, group_data in [
        ("Required Tags", data.required_coverage),
        ("Recommended Tags", data.recommended_coverage),
        ("MusicBrainz Tags", data.musicbrainz_coverage),
        ("Discogs Tags", data.discogs_coverage),
        ("Other Tracked Tags", data.other_coverage),
    ]:
        rows: list[list[dict[str, Any]]] = []
        for row in group_data:
            absent_cls = ""
            if "Required" in group_name and row.absent_count > 0:
                absent_cls = "severity-error"
            elif "Recommended" in group_name and row.absent_pct > 20.0:
                absent_cls = "severity-warning"

            rows.append(
                [
                    _cell(row.tag_key, cls="mono"),
                    _cell(fmt_count(row.present_nonempty_count), cls="num"),
                    _cell(fmt_pct(row.present_nonempty_pct), cls="num"),
                    _cell(fmt_count(row.present_empty_count), cls="num"),
                    _cell(fmt_pct(row.present_empty_pct), cls="num"),
                    _cell(fmt_count(row.absent_count), cls=f"num {absent_cls}".strip()),
                    _cell(fmt_pct(row.absent_pct), cls=f"num {absent_cls}".strip()),
                ]
            )
        sections.append(
            _section(
                f"Section A: {group_name}",
                tables=[_table(None, cov_headers, rows)],
            )
        )

    # Aliases
    if data.alias_usage:
        alias_rows: list[list[dict[str, Any]]] = []
        for row in data.alias_usage:
            cls = "severity-warning" if row.files_using_alias > 0 else "dim"
            alias_rows.append(
                [
                    _cell(row.canonical_key, cls="mono"),
                    _cell(row.alias_key, cls=f"mono {cls}"),
                    _cell(fmt_count(row.files_using_alias), cls=f"num {cls}"),
                    _cell(fmt_pct(row.files_using_alias_pct), cls=f"num {cls}"),
                ]
            )
        sections.append(
            _section(
                "Section B: Alias Usage",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Canonical Key"),
                            _hdr("Alias"),
                            _hdr("Files Using Alias", "num"),
                            _hdr("%", "num"),
                        ],
                        alias_rows,
                    )
                ],
            )
        )

    # Full inventory
    inv_rows: list[list[dict[str, Any]]] = []
    for row in data.full_inventory:
        inv_rows.append(
            [
                _cell(row.tag_key_upper, cls="mono"),
                _cell(fmt_count(row.file_count), cls="num"),
                _cell(fmt_pct(row.coverage_pct), cls="num"),
                _cell(row.classification, cls=f"tag-{row.classification}"),
            ]
        )
    sections.append(
        _section(
            "Section C: Full Tag Key Inventory",
            tables=[
                _table(
                    None,
                    [
                        _hdr("Tag Key"),
                        _hdr("File Count", "num"),
                        _hdr("Coverage %", "num"),
                        _hdr("Classification"),
                    ],
                    inv_rows,
                )
            ],
        )
    )

    # Unknown keys
    if data.unknown_keys:
        unk_rows: list[list[dict[str, Any]]] = []
        for row in data.unknown_keys:
            note = " ⚠ <0.1%" if row.coverage_pct < 0.1 else ""
            unk_rows.append(
                [
                    _cell(row.tag_key_upper, cls="mono severity-error"),
                    _cell(fmt_count(row.file_count), cls="num"),
                    _cell(fmt_pct(row.coverage_pct) + note, cls="num"),
                ]
            )
        sections.append(
            _section(
                f"Section C: Unknown Keys ({len(data.unknown_keys)} — stripping candidates)",
                note="Review these keys and add to [tags.strip] in your config to remove them in Phase 4.",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Tag Key"),
                            _hdr("File Count", "num"),
                            _hdr("Coverage %", "num"),
                        ],
                        unk_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir,
            "report_tags",
            ts,
            "Tag Coverage & Key Inventory",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 5 — Tag format quality
# =========================================================================


def write_tag_formats(
    output_dir: Path,
    data: TagFormatsFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the tag format quality report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Tag Format Quality",
            summary_cards=[_card(fmt_count(data.total_files), "Total Files")],
        )
    )

    for sec in data.sections:
        tables: list[dict[str, Any]] = []
        s = sec.summary
        inv_cls = "severity-error" if s.invalid_count > 0 else ""

        summary_rows: list[list[dict[str, Any]]] = [
            [
                _cell("Valid"),
                _cell(fmt_count(s.valid_count), cls="num"),
                _cell(
                    fmt_pct(s.valid_pct),
                    cls="num",
                    bar_pct=s.valid_pct,
                    bar_cls="bar-green",
                ),
            ],
            [
                _cell("Invalid", cls=inv_cls),
                _cell(fmt_count(s.invalid_count), cls=f"num {inv_cls}"),
                _cell(
                    fmt_pct(s.invalid_pct),
                    cls=f"num {inv_cls}",
                    bar_pct=s.invalid_pct,
                    bar_cls="bar-red",
                ),
            ],
            [
                _cell("Absent"),
                _cell(fmt_count(s.absent_count), cls="num"),
                _cell(fmt_pct(s.absent_pct), cls="num"),
            ],
        ]
        tables.append(
            _table(
                "Validation Summary",
                [_hdr("Status"), _hdr("Count", "num"), _hdr("%", "num")],
                summary_rows,
            )
        )

        # Extra distributions (e.g. date precision)
        for extra_name, extra_rows in sec.extra.items():
            if extra_rows:
                ext_rows: list[list[dict[str, Any]]] = []
                for er in extra_rows:
                    ext_rows.append(
                        [
                            _cell(er.precision),
                            _cell(fmt_count(er.count), cls="num"),
                            _cell(fmt_pct(er.percentage), cls="num"),
                        ]
                    )
                tables.append(
                    _table(
                        extra_name.replace("_", " ").title(),
                        [_hdr("Precision"), _hdr("Count", "num"), _hdr("%", "num")],
                        ext_rows,
                    )
                )

        # Invalid values
        if sec.invalid_values:
            iv_rows: list[list[dict[str, Any]]] = []
            for iv in sec.invalid_values:
                iv_rows.append(
                    [
                        _cell(iv.value, cls="mono"),
                        _cell(fmt_count(iv.count), cls="num"),
                    ]
                )
            footer = None
            if sec.invalid_values_total > len(sec.invalid_values):
                footer = f"Showing top {len(sec.invalid_values)} of {sec.invalid_values_total} distinct invalid values."
            tables.append(
                _table(
                    "Invalid Values",
                    [_hdr("Value"), _hdr("Count", "num")],
                    iv_rows,
                    footer=footer,
                )
            )

        sections.append(_section(f"Field: {sec.field_name}", tables=tables))

    return [
        _write_html(
            output_dir,
            "report_tag_formats",
            ts,
            "Tag Format Quality",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 6 — Intra-album consistency
# =========================================================================


def write_album_consistency(
    output_dir: Path,
    data: AlbumConsistencyFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the album consistency report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Intra-Album Consistency",
            summary_cards=[
                _card(fmt_count(data.total_albums), "Albums Checked"),
                _card(fmt_count(data.albums_with_violations), "With Violations"),
                _card(fmt_count(len(data.field_violations)), "Field Violations"),
                _card(
                    fmt_count(len(data.numbering_violations)), "Numbering Violations"
                ),
            ],
        )
    )

    # Field consistency violations
    if data.field_violations:
        fv_rows: list[list[dict[str, Any]]] = []
        for v in data.field_violations:
            vals_display = " | ".join(
                f"{val} ({v.track_counts_per_value.get(val, '?')})"
                for val in v.distinct_values[:5]
            )
            if len(v.distinct_values) > 5:
                vals_display += f" … +{len(v.distinct_values) - 5} more"
            fv_rows.append(
                [
                    _cell(v.album_dir, cls="path"),
                    _cell(v.field_name, cls="mono"),
                    _cell(vals_display),
                    _cell(
                        str(v.null_track_count) if v.null_track_count > 0 else "",
                        cls="num",
                    ),
                ]
            )
        sections.append(
            _section(
                f"Field Consistency Violations ({len(data.field_violations)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Album Directory"),
                            _hdr("Field"),
                            _hdr("Distinct Values"),
                            _hdr("Null Tracks", "num"),
                        ],
                        fv_rows,
                    )
                ],
            )
        )

    # Track numbering violations
    if data.numbering_violations:
        nv_rows: list[list[dict[str, Any]]] = []
        for v in data.numbering_violations:
            nv_rows.append(
                [
                    _cell(v.album_dir, cls="path"),
                    _cell(v.check_type, cls="mono"),
                    _cell(v.description),
                ]
            )
        sections.append(
            _section(
                f"Track Numbering Violations ({len(data.numbering_violations)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Album Directory"),
                            _hdr("Check Type"),
                            _hdr("Description"),
                        ],
                        nv_rows,
                    )
                ],
            )
        )

    # Summary by violation type
    if data.summary_by_type:
        sv_rows: list[list[dict[str, Any]]] = []
        for s in data.summary_by_type:
            sv_rows.append(
                [
                    _cell(s.check_type, cls="mono"),
                    _cell(fmt_count(s.album_count), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Summary by Violation Type",
                tables=[
                    _table(
                        None,
                        [_hdr("Check Type"), _hdr("Albums Affected", "num")],
                        sv_rows,
                    )
                ],
            )
        )

    if not data.field_violations and not data.numbering_violations:
        sections.append(
            _section(
                "Result",
                text_blocks=[_text("No consistency violations found.", cls="dim")],
            )
        )

    return [
        _write_html(
            output_dir,
            "report_album_consistency",
            ts,
            "Intra-Album Consistency",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 7 — External ID coverage and integrity
# =========================================================================


def write_ids(
    output_dir: Path,
    data: IdsFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the external IDs report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "External ID Coverage & Integrity",
            summary_cards=[_card(fmt_count(data.total_files), "Total Files")],
        )
    )

    for id_section in [data.musicbrainz, data.discogs]:
        # Coverage table
        cov_rows: list[list[dict[str, Any]]] = []
        for row in id_section.coverage:
            mal_cls = "severity-error" if row.malformed_count > 0 else ""
            cov_rows.append(
                [
                    _cell(row.tag_key, cls="mono"),
                    _cell(fmt_count(row.valid_count), cls="num"),
                    _cell(fmt_pct(row.valid_pct), cls="num"),
                    _cell(fmt_count(row.malformed_count), cls=f"num {mal_cls}".strip()),
                    _cell(fmt_pct(row.malformed_pct), cls=f"num {mal_cls}".strip()),
                    _cell(fmt_count(row.absent_count), cls="num"),
                    _cell(fmt_pct(row.absent_pct), cls="num"),
                ]
            )
        tables: list[dict[str, Any]] = [
            _table(
                "Coverage",
                [
                    _hdr("Tag Key"),
                    _hdr("Valid", "num"),
                    _hdr("Valid %", "num"),
                    _hdr("Malformed", "num"),
                    _hdr("Malformed %", "num"),
                    _hdr("Absent", "num"),
                    _hdr("Absent %", "num"),
                ],
                cov_rows,
            )
        ]

        # Partial albums
        if id_section.partial_albums:
            pa_rows: list[list[dict[str, Any]]] = []
            for pa in id_section.partial_albums:
                pa_rows.append(
                    [
                        _cell(pa.album_dir, cls="path"),
                        _cell(pa.tracks_with_id, cls="num"),
                        _cell(pa.tracks_without_id, cls="num"),
                        _cell(pa.total_tracks, cls="num"),
                    ]
                )
            tables.append(
                _table(
                    f"Partial Album Coverage ({len(id_section.partial_albums)} albums)",
                    [
                        _hdr("Album Directory"),
                        _hdr("With ID", "num"),
                        _hdr("Without ID", "num"),
                        _hdr("Total", "num"),
                    ],
                    pa_rows,
                )
            )

        # Duplicate IDs
        if id_section.duplicate_ids:
            dup_rows: list[list[dict[str, Any]]] = []
            for d in id_section.duplicate_ids:
                same_text = "Yes" if d.same_directory else "No"
                same_cls = "severity-error" if d.same_directory else "severity-warning"
                paths_str = "<br>".join(d.file_paths[:10])
                if len(d.file_paths) > 10:
                    paths_str += f"<br>… +{len(d.file_paths) - 10} more"
                dup_rows.append(
                    [
                        _cell(d.id_value, cls="mono"),
                        _cell(d.file_count, cls="num"),
                        _cell(same_text, cls=same_cls),
                        _cell(paths_str, cls="path"),
                    ]
                )
            tables.append(
                _table(
                    f"Duplicate IDs ({len(id_section.duplicate_ids)})",
                    [
                        _hdr("ID Value"),
                        _hdr("Files", "num"),
                        _hdr("Same Dir?"),
                        _hdr("Paths"),
                    ],
                    dup_rows,
                )
            )

        text_blocks: list[dict[str, Any]] = []
        if id_section.backfill:
            bf = id_section.backfill
            text_blocks.append(
                _text(
                    f"<strong>Quick-win backfill:</strong> {fmt_count(bf.affected_tracks)} tracks, "
                    f"{fmt_count(bf.distinct_source_ids)} API calls needed. {bf.description}"
                )
            )

        sections.append(
            _section(
                f"{id_section.source_name} IDs",
                tables=tables,
                text_blocks=text_blocks or None,
            )
        )

    return [
        _write_html(
            output_dir,
            "report_ids",
            ts,
            "External ID Coverage & Integrity",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 8 — Duplicates
# =========================================================================


def write_duplicates(
    output_dir: Path,
    data: DuplicatesFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the duplicates report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Duplicates",
            summary_cards=[
                _card(fmt_count(len(data.exact)), "Exact Groups"),
                _card(fmt_count(len(data.mbid)), "MBID Groups"),
                _card(fmt_count(len(data.probable)), "Probable Groups"),
            ],
        )
    )

    # Exact duplicates
    if data.exact:
        exact_rows: list[list[dict[str, Any]]] = []
        for g in data.exact:
            paths_str = "<br>".join(g.paths[:10])
            if len(g.paths) > 10:
                paths_str += f"<br>… +{len(g.paths) - 10} more"
            exact_rows.append(
                [
                    _cell(g.checksum[:20] + "…", cls="mono"),
                    _cell(g.copy_count, cls="num"),
                    _cell(paths_str, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Exact Duplicates (SHA-256) — {len(data.exact)} group(s)",
                tables=[
                    _table(
                        None,
                        [_hdr("Checksum"), _hdr("Copies", "num"), _hdr("Paths")],
                        exact_rows,
                    )
                ],
            )
        )
    else:
        sections.append(
            _section(
                "Exact Duplicates",
                text_blocks=[_text("No exact duplicates found.", cls="dim")],
            )
        )

    # MBID duplicates
    if data.mbid:
        mbid_rows: list[list[dict[str, Any]]] = []
        for g in data.mbid:
            same_text = "Yes" if g.same_directory else "No"
            same_cls = "severity-error" if g.same_directory else "severity-warning"
            file_details = "<br>".join(
                f"{f.path} [{f.album} / {f.date}]" for f in g.files[:10]
            )
            if len(g.files) > 10:
                file_details += f"<br>… +{len(g.files) - 10} more"
            mbid_rows.append(
                [
                    _cell(g.mbid[:20] + "…", cls="mono"),
                    _cell(g.file_count, cls="num"),
                    _cell(same_text, cls=same_cls),
                    _cell(file_details, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Same Recording Duplicates (MUSICBRAINZ_TRACKID) — {len(data.mbid)} group(s)",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("MBID"),
                            _hdr("Files", "num"),
                            _hdr("Same Dir?"),
                            _hdr("Details"),
                        ],
                        mbid_rows,
                    )
                ],
            )
        )

    # Probable duplicates
    if data.probable:
        prob_rows: list[list[dict[str, Any]]] = []
        for g in data.probable:
            paths_str = "<br>".join(g.paths[:5])
            if len(g.paths) > 5:
                paths_str += f"<br>… +{len(g.paths) - 5} more"
            prob_rows.append(
                [
                    _cell(g.albumartist),
                    _cell(g.album),
                    _cell(g.discnumber, cls="num"),
                    _cell(g.tracknumber, cls="num"),
                    _cell(g.file_count, cls="num"),
                    _cell(paths_str, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Probable Duplicates (by position) — {len(data.probable)} group(s)",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Album Artist"),
                            _hdr("Album"),
                            _hdr("Disc", "num"),
                            _hdr("Track", "num"),
                            _hdr("Files", "num"),
                            _hdr("Paths"),
                        ],
                        prob_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir, "report_duplicates", ts, "Duplicates", sections, filters
        )
    ]


# =========================================================================
# Report 9 — Scan issues
# =========================================================================


def write_issues(
    output_dir: Path,
    data: IssuesFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the scan issues report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Scan Issues",
            summary_cards=[_card(fmt_count(data.total_open), "Open Issues")],
        )
    )

    if data.total_open == 0:
        sections.append(
            _section(
                "Result",
                text_blocks=[_text("No open scan issues.", cls="dim")],
            )
        )
        return [
            _write_html(
                output_dir, "report_issues", ts, "Scan Issues", sections, filters
            )
        ]

    # Pivot table
    if data.pivot:
        piv_rows: list[list[dict[str, Any]]] = []
        for row in data.pivot:
            sev_cls = f"severity-{row.severity}"
            piv_rows.append(
                [
                    _cell(row.severity.upper(), cls=sev_cls),
                    _cell(row.issue_type, cls="mono"),
                    _cell(fmt_count(row.count), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Issues by Severity and Type",
                tables=[
                    _table(
                        None,
                        [_hdr("Severity"), _hdr("Issue Type"), _hdr("Count", "num")],
                        piv_rows,
                    )
                ],
            )
        )

    # By album (top 50)
    if data.by_album:
        album_rows: list[list[dict[str, Any]]] = []
        for row in data.by_album[:50]:
            album_rows.append(
                [
                    _cell(row.album_dir, cls="path"),
                    _cell(fmt_count(row.issue_count), cls="num"),
                ]
            )
        footer = None
        if len(data.by_album) > 50:
            footer = f"Showing top 50 of {len(data.by_album)} albums."
        sections.append(
            _section(
                "Most-Affected Albums",
                tables=[
                    _table(
                        None,
                        [_hdr("Album Directory"), _hdr("Issues", "num")],
                        album_rows,
                        footer=footer,
                    )
                ],
            )
        )

    # By artist (top 20)
    if data.by_artist:
        art_rows: list[list[dict[str, Any]]] = []
        for row in data.by_artist[:20]:
            art_rows.append(
                [
                    _cell(row.artist),
                    _cell(fmt_count(row.issue_count), cls="num"),
                ]
            )
        footer_a = None
        if len(data.by_artist) > 20:
            footer_a = f"Showing top 20 of {len(data.by_artist)} artists."
        sections.append(
            _section(
                "Most-Affected Artists",
                tables=[
                    _table(
                        None,
                        [_hdr("Artist"), _hdr("Issues", "num")],
                        art_rows,
                        footer=footer_a,
                    )
                ],
            )
        )

    # Per-issue-type drill-down
    for issue_type, detail_rows in sorted(data.by_type.items()):
        dt_rows: list[list[dict[str, Any]]] = []
        for dr in detail_rows:
            sev_cls = f"severity-{dr.severity}"
            dt_rows.append(
                [
                    _cell(dr.severity.upper(), cls=sev_cls),
                    _cell(dr.file_path or "(collection-wide)", cls="path"),
                    _cell(dr.description),
                ]
            )
        sections.append(
            _section(
                f"Issue Type: {issue_type} ({len(detail_rows)} issues)",
                tables=[
                    _table(
                        None,
                        [_hdr("Severity"), _hdr("File Path"), _hdr("Description")],
                        dt_rows,
                    )
                ],
            )
        )

    return [
        _write_html(output_dir, "report_issues", ts, "Scan Issues", sections, filters)
    ]


# =========================================================================
# Report 10 — Artist name consistency
# =========================================================================


def write_artists(
    output_dir: Path,
    data: ArtistsFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the artist consistency report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Artist Name Consistency",
            summary_cards=[
                _card(
                    fmt_count(len(data.albumartist_values)),
                    "Distinct ALBUMARTIST Values",
                ),
                _card(
                    fmt_count(len(data.albumartist_case_variants)),
                    "Case Variant Groups",
                ),
                _card(
                    fmt_count(len(data.albumartist_fuzzy_pairs)),
                    "Fuzzy Pairs (ALBUMARTIST)",
                ),
                _card(fmt_count(len(data.artist_values)), "Distinct ARTIST Values"),
            ],
        )
    )

    # ALBUMARTIST values (top 100)
    if data.albumartist_values:
        aa_rows: list[list[dict[str, Any]]] = []
        for v in data.albumartist_values[:100]:
            aa_rows.append(
                [
                    _cell(v.value),
                    _cell(fmt_count(v.track_count), cls="num"),
                    _cell(fmt_count(v.album_count), cls="num"),
                ]
            )
        footer = None
        if len(data.albumartist_values) > 100:
            footer = f"Showing top 100 of {len(data.albumartist_values)} values."
        sections.append(
            _section(
                "ALBUMARTIST Values",
                tables=[
                    _table(
                        None,
                        [_hdr("Value"), _hdr("Tracks", "num"), _hdr("Albums", "num")],
                        aa_rows,
                        footer=footer,
                    )
                ],
            )
        )

    # Case variants
    if data.albumartist_case_variants:
        cv_text: list[dict[str, Any]] = []
        for g in data.albumartist_case_variants:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            cv_text.append(
                _text(
                    f'<span class="severity-error">●</span> {variants_str} '
                    f"(total: {fmt_count(g.total_track_count)} tracks)"
                )
            )
        sections.append(
            _section(
                f"ALBUMARTIST Case/Whitespace Variants ({len(data.albumartist_case_variants)} groups)",
                text_blocks=cv_text,
            )
        )

    # Fuzzy pairs
    if data.albumartist_fuzzy_pairs:
        fp_rows: list[list[dict[str, Any]]] = []
        for p in data.albumartist_fuzzy_pairs:
            fp_rows.append(
                [
                    _cell(p.name_a),
                    _cell(p.name_b),
                    _cell(f"{p.similarity:.2%}", cls="num"),
                    _cell(fmt_count(p.count_a), cls="num"),
                    _cell(fmt_count(p.count_b), cls="num"),
                ]
            )
        sections.append(
            _section(
                f"ALBUMARTIST Fuzzy Similarity Pairs ({len(data.albumartist_fuzzy_pairs)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Name A"),
                            _hdr("Name B"),
                            _hdr("Similarity", "num"),
                            _hdr("Tracks A", "num"),
                            _hdr("Tracks B", "num"),
                        ],
                        fp_rows,
                    )
                ],
            )
        )

    # ARTIST values (top 100)
    if data.artist_values:
        a_rows: list[list[dict[str, Any]]] = []
        for v in data.artist_values[:100]:
            a_rows.append(
                [
                    _cell(v.value),
                    _cell(fmt_count(v.track_count), cls="num"),
                    _cell(fmt_count(v.album_count), cls="num"),
                ]
            )
        footer_a = None
        if len(data.artist_values) > 100:
            footer_a = f"Showing top 100 of {len(data.artist_values)} values."
        sections.append(
            _section(
                "ARTIST Values",
                tables=[
                    _table(
                        None,
                        [_hdr("Value"), _hdr("Tracks", "num"), _hdr("Albums", "num")],
                        a_rows,
                        footer=footer_a,
                    )
                ],
            )
        )

    # ARTIST case variants
    if data.artist_case_variants:
        acv_text: list[dict[str, Any]] = []
        for g in data.artist_case_variants[:50]:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            acv_text.append(
                _text(
                    f'<span class="severity-error">●</span> {variants_str} '
                    f"(total: {fmt_count(g.total_track_count)} tracks)"
                )
            )
        if len(data.artist_case_variants) > 50:
            acv_text.append(
                _text(
                    f"… and {len(data.artist_case_variants) - 50} more groups.",
                    cls="dim",
                )
            )
        sections.append(
            _section(
                f"ARTIST Case/Whitespace Variants ({len(data.artist_case_variants)} groups)",
                text_blocks=acv_text,
            )
        )

    # ARTIST fuzzy pairs
    if data.artist_fuzzy_pairs:
        afp_rows: list[list[dict[str, Any]]] = []
        for p in data.artist_fuzzy_pairs:
            afp_rows.append(
                [
                    _cell(p.name_a),
                    _cell(p.name_b),
                    _cell(f"{p.similarity:.2%}", cls="num"),
                    _cell(fmt_count(p.count_a), cls="num"),
                    _cell(fmt_count(p.count_b), cls="num"),
                ]
            )
        sections.append(
            _section(
                f"ARTIST Fuzzy Similarity Pairs ({len(data.artist_fuzzy_pairs)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Name A"),
                            _hdr("Name B"),
                            _hdr("Similarity", "num"),
                            _hdr("Tracks A", "num"),
                            _hdr("Tracks B", "num"),
                        ],
                        afp_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir,
            "report_artists",
            ts,
            "Artist Name Consistency",
            sections,
            filters,
        )
    ]


# =========================================================================
# Report 11 — Genre analysis
# =========================================================================


def write_genres(
    output_dir: Path,
    data: GenresFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the genre analysis report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Genre Analysis",
            summary_cards=[
                _card(fmt_count(data.total_files), "Total Files"),
                _card(fmt_count(len(data.genre_values)), "Distinct Genres"),
                _card(
                    f"{fmt_count(data.no_genre_count)} ({fmt_pct(data.no_genre_pct)})",
                    "No Genre Tag",
                ),
                _card(fmt_count(data.multi_genre_count), "Multi-Genre Files"),
            ],
        )
    )

    # Genre values (all of them in HTML — it's scrollable)
    if data.genre_values:
        max_count = data.genre_values[0].file_count if data.genre_values else 1
        gv_rows: list[list[dict[str, Any]]] = []
        for gv in data.genre_values:
            bar_pct_val = safe_pct(gv.file_count, max_count) if max_count > 0 else 0
            gv_rows.append(
                [
                    _cell(gv.value),
                    _cell(fmt_count(gv.file_count), cls="num"),
                    _cell(
                        fmt_pct(gv.file_pct),
                        cls="num",
                        bar_pct=bar_pct_val,
                    ),
                ]
            )
        sections.append(
            _section(
                "Genre Values",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Genre"),
                            _hdr("Files", "num"),
                            _hdr("% of Collection", "num"),
                        ],
                        gv_rows,
                    )
                ],
            )
        )

    # Fuzzy pairs
    if data.fuzzy_pairs:
        fp_rows: list[list[dict[str, Any]]] = []
        for p in data.fuzzy_pairs:
            fp_rows.append(
                [
                    _cell(p.name_a),
                    _cell(p.name_b),
                    _cell(f"{p.similarity:.2%}", cls="num"),
                    _cell(fmt_count(p.count_a), cls="num"),
                    _cell(fmt_count(p.count_b), cls="num"),
                ]
            )
        sections.append(
            _section(
                f"Genre Fuzzy Similarity Pairs ({len(data.fuzzy_pairs)})",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Genre A"),
                            _hdr("Genre B"),
                            _hdr("Similarity", "num"),
                            _hdr("Files A", "num"),
                            _hdr("Files B", "num"),
                        ],
                        fp_rows,
                    )
                ],
            )
        )

    # Missing genre by artist
    if data.no_genre_by_artist:
        mga_rows: list[list[dict[str, Any]]] = []
        for a in data.no_genre_by_artist:
            mga_rows.append(
                [
                    _cell(a.artist),
                    _cell(fmt_count(a.missing_count), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Missing Genre by Artist",
                tables=[
                    _table(
                        None,
                        [_hdr("Artist"), _hdr("Missing Files", "num")],
                        mga_rows,
                    )
                ],
            )
        )

    # Multi-genre combos
    if data.multi_genre_combos:
        mg_rows: list[list[dict[str, Any]]] = []
        for combo in data.multi_genre_combos:
            mg_rows.append(
                [
                    _cell(" + ".join(combo.values)),
                    _cell(fmt_count(combo.file_count), cls="num"),
                ]
            )
        sections.append(
            _section(
                f"Multi-Genre Combinations ({fmt_count(data.multi_genre_count)} files)",
                tables=[
                    _table(
                        None,
                        [_hdr("Genre Combination"), _hdr("Files", "num")],
                        mg_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir, "report_genres", ts, "Genre Analysis", sections, filters
        )
    ]


# =========================================================================
# Report 12 — Lyrics coverage
# =========================================================================


def write_lyrics(
    output_dir: Path,
    data: LyricsFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the lyrics coverage report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    o = data.overall
    sections.append(
        _section(
            "Lyrics Coverage",
            summary_cards=[
                _card(fmt_count(data.total_files), "Total Files"),
                _card(
                    f"{fmt_count(o.with_lyrics_count)} ({fmt_pct(o.with_lyrics_pct)})",
                    "With Lyrics",
                ),
                _card(
                    f"{fmt_count(o.no_lyrics_count)} ({fmt_pct(o.no_lyrics_pct)})",
                    "No Lyrics",
                ),
            ],
        )
    )

    # Overall coverage table
    ov_rows: list[list[dict[str, Any]]] = [
        [
            _cell("With lyrics"),
            _cell(fmt_count(o.with_lyrics_count), cls="num"),
            _cell(
                fmt_pct(o.with_lyrics_pct),
                cls="num",
                bar_pct=o.with_lyrics_pct,
                bar_cls="bar-green",
            ),
        ],
        [
            _cell("Empty lyrics tag"),
            _cell(fmt_count(o.empty_lyrics_count), cls="num"),
            _cell(
                fmt_pct(o.empty_lyrics_pct),
                cls="num",
                bar_pct=o.empty_lyrics_pct,
                bar_cls="bar-yellow",
            ),
        ],
        [
            _cell("No lyrics tag"),
            _cell(fmt_count(o.no_lyrics_count), cls="num"),
            _cell(
                fmt_pct(o.no_lyrics_pct),
                cls="num",
                bar_pct=o.no_lyrics_pct,
                bar_cls="bar-red",
            ),
        ],
    ]
    sections.append(
        _section(
            "Overall Coverage",
            tables=[
                _table(
                    None,
                    [_hdr("Category"), _hdr("Count", "num"), _hdr("%", "num")],
                    ov_rows,
                )
            ],
            text_blocks=[
                _text(
                    f"Files using <code>LYRICS</code> key: {fmt_count(o.lyrics_key_count)} | "
                    f"Files using <code>UNSYNCEDLYRICS</code> key: {fmt_count(o.unsyncedlyrics_key_count)}",
                    cls="dim",
                )
            ],
        )
    )

    # By artist
    if data.by_artist:
        ba_rows: list[list[dict[str, Any]]] = []
        for a in data.by_artist:
            cov_cls = "severity-error" if a.coverage_pct == 0 else ""
            ba_rows.append(
                [
                    _cell(a.name),
                    _cell(fmt_count(a.with_lyrics), cls="num"),
                    _cell(fmt_count(a.total), cls="num"),
                    _cell(
                        fmt_pct(a.coverage_pct),
                        cls=f"num {cov_cls}".strip(),
                        bar_pct=a.coverage_pct,
                        bar_cls="bar-green" if a.coverage_pct > 0 else "bar-red",
                    ),
                ]
            )
        sections.append(
            _section(
                "Coverage by Artist (worst first)",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Artist"),
                            _hdr("With Lyrics", "num"),
                            _hdr("Total", "num"),
                            _hdr("Coverage %", "num"),
                        ],
                        ba_rows,
                    )
                ],
            )
        )

    # By album
    if data.by_album:
        alb_rows: list[list[dict[str, Any]]] = []
        for a in data.by_album:
            cov_cls = "severity-error" if a.coverage_pct == 0 else ""
            alb_rows.append(
                [
                    _cell(a.name, cls="path"),
                    _cell(fmt_count(a.with_lyrics), cls="num"),
                    _cell(fmt_count(a.total), cls="num"),
                    _cell(
                        fmt_pct(a.coverage_pct),
                        cls=f"num {cov_cls}".strip(),
                        bar_pct=a.coverage_pct,
                        bar_cls="bar-green" if a.coverage_pct > 0 else "bar-red",
                    ),
                ]
            )
        sections.append(
            _section(
                "Coverage by Album (worst first)",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Album Directory"),
                            _hdr("With Lyrics", "num"),
                            _hdr("Total", "num"),
                            _hdr("Coverage %", "num"),
                        ],
                        alb_rows,
                    )
                ],
            )
        )

    return [
        _write_html(
            output_dir, "report_lyrics", ts, "Lyrics Coverage", sections, filters
        )
    ]


# =========================================================================
# Report 13 — ReplayGain coverage
# =========================================================================


def write_replaygain(
    output_dir: Path,
    data: ReplayGainFullData,
    timestamp: str | None = None,
    *,
    filters: str | None = None,
) -> list[Path]:
    """Write HTML file for the ReplayGain coverage report."""
    ts = timestamp or now_filename_stamp()
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "ReplayGain Coverage",
            summary_cards=[
                _card(fmt_count(data.total_files), "Total Files"),
                _card(fmt_count(len(data.partial_albums)), "Partial Albums"),
                _card(fmt_count(len(data.outliers)), "Outliers"),
            ],
        )
    )

    # Coverage table
    if data.coverage:
        cov_rows: list[list[dict[str, Any]]] = []
        for row in data.coverage:
            mal_cls = "severity-error" if row.malformed_count > 0 else ""
            cov_rows.append(
                [
                    _cell(row.tag_key, cls="mono"),
                    _cell(fmt_count(row.valid_count), cls="num"),
                    _cell(fmt_pct(row.valid_pct), cls="num"),
                    _cell(fmt_count(row.malformed_count), cls=f"num {mal_cls}".strip()),
                    _cell(fmt_pct(row.malformed_pct), cls=f"num {mal_cls}".strip()),
                    _cell(fmt_count(row.absent_count), cls="num"),
                    _cell(fmt_pct(row.absent_pct), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Tag Coverage",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Tag Key"),
                            _hdr("Valid", "num"),
                            _hdr("Valid %", "num"),
                            _hdr("Malformed", "num"),
                            _hdr("Malformed %", "num"),
                            _hdr("Absent", "num"),
                            _hdr("Absent %", "num"),
                        ],
                        cov_rows,
                    )
                ],
            )
        )

    # Partial albums
    if data.partial_albums:
        pa_rows: list[list[dict[str, Any]]] = []
        for pa in data.partial_albums:
            pa_rows.append(
                [
                    _cell(pa.album_dir, cls="path"),
                    _cell(pa.tracks_with_rg, cls="num"),
                    _cell(pa.tracks_without_rg, cls="num"),
                    _cell(pa.total_tracks, cls="num"),
                ]
            )
        sections.append(
            _section(
                f"Partially-Tagged Albums ({len(data.partial_albums)})",
                note="Partial album ReplayGain is worse than none — it produces inconsistent playback volume.",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Album Directory"),
                            _hdr("With RG", "num"),
                            _hdr("Without RG", "num"),
                            _hdr("Total", "num"),
                        ],
                        pa_rows,
                    )
                ],
            )
        )

    # Track gain distribution
    if data.gain_distribution:
        sections.append(
            _section(
                "Track Gain Distribution (REPLAYGAIN_TRACK_GAIN)",
                tables=[_bucket_table(None, data.gain_distribution)],
            )
        )

    # Outliers
    if data.outliers:
        out_rows: list[list[dict[str, Any]]] = []
        for o in data.outliers:
            out_rows.append(
                [
                    _cell(o.path, cls="path"),
                    _cell(o.tag_key, cls="mono"),
                    _cell(o.value, cls="mono"),
                    _cell(f"{o.parsed_db:.2f}", cls="num severity-error"),
                ]
            )
        sections.append(
            _section(
                f"Gain Outliers (outside [-20, +10] dB) — {len(data.outliers)} found",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Path"),
                            _hdr("Tag Key"),
                            _hdr("Value"),
                            _hdr("Parsed (dB)", "num"),
                        ],
                        out_rows,
                    )
                ],
            )
        )
    else:
        sections.append(
            _section(
                "Gain Outliers",
                text_blocks=[_text("No gain outliers found.", cls="dim")],
            )
        )

    return [
        _write_html(
            output_dir,
            "report_replaygain",
            ts,
            "ReplayGain Coverage",
            sections,
            filters,
        )
    ]
