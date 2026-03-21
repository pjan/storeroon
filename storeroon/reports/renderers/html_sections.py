"""
storeroon.reports.renderers.html_sections — HTML section builders.

Pure functions that transform report *FullData models into the intermediate
dict structure expected by the Jinja2 ``report.html`` template.  These
functions have no I/O side-effects and know nothing about the filesystem or
HTTP layer — they are shared by both the static HTML renderer and the
``storeroon serve`` web server.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from storeroon.reports.models import (
    AlbumConsistencyFullData,
    ArtistBreakdown,
    ArtistsFullData,
    BucketCount,
    DuplicatesFullData,
    GenresFullData,
    IssuesFullData,
    LyricsFullData,
    OverviewFullData,
    ReplayGainFullData,
    TagCoverageFullData,
    TagCoverageRow,
    TagGroupQuality,
    TagQualityFullData,
    TechnicalFullData,
)
from storeroon.reports.utils import (
    fmt_count,
    fmt_duration_hms,
    fmt_duration_short,
    fmt_pct,
    fmt_size_gb,
    safe_pct,
)

# ---------------------------------------------------------------------------
# Report title registry
# ---------------------------------------------------------------------------

REPORT_TITLES: dict[str, str] = {
    "overview": "Collection Overview",
    "technical": "Audio Technical Quality",
    "tags": "Tag Coverage & Key Inventory",
    "tag_quality": "Tag Quality & Integrity",
    "album_consistency": "Intra-Album Consistency",
    "duplicates": "Duplicates",
    "issues": "Scan Issues",
    "artists": "Artist Name Consistency",
    "genres": "Genre Analysis",
    "lyrics": "Lyrics Coverage",
    "replaygain": "ReplayGain Coverage",
}

# ---------------------------------------------------------------------------
# Low-level template data-structure helpers
# ---------------------------------------------------------------------------


def _hdr(
    label: str, align: str | None = None, cls: str | None = None
) -> dict[str, Any]:
    d: dict[str, Any] = {"label": label}
    if align:
        d["align"] = align
    if cls:
        d["cls"] = cls
    return d


def _cell(
    value: Any,
    cls: str | None = None,
    bar_pct: float | None = None,
    bar_cls: str | None = None,
) -> dict[str, Any]:
    d: dict[str, Any] = {"value": str(value) if value is not None else ""}
    if cls:
        d["cls"] = cls
    if bar_pct is not None:
        d["bar_pct"] = min(max(bar_pct, 0.0), 100.0)
    if bar_cls:
        d["bar_cls"] = bar_cls
    return d


def _card(value: str, label: str) -> dict[str, str]:
    return {"value": value, "label": label}


def _text(content: str, cls: str | None = None) -> dict[str, Any]:
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


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


def _hierarchy_row(
    name_html: str,
    indent: int,
    tracks: int,
    discs: int,
    size: int,
    duration: float,
) -> str:
    """Build a single <tr> for the hierarchy table."""
    pad = f"padding-left:{indent * 1.5}rem" if indent else ""
    return (
        f"<tr>"
        f'<td style="{pad}">{name_html}</td>'
        f'<td class="num">{fmt_count(tracks)}</td>'
        f'<td class="num">{fmt_count(discs)}</td>'
        f'<td class="num">{fmt_size_gb(size)}</td>'
        f'<td class="num">{fmt_duration_hms(duration)}</td>'
        f"</tr>"
    )


def _expandable_row(
    label: str,
    indent: int,
    tracks: int,
    discs: int,
    size: int,
    duration: float,
    children_html: str,
) -> str:
    """Build an expandable row: a summary row + a hidden children row."""
    row = _hierarchy_row(
        f'<details><summary style="cursor:pointer">{label}</summary></details>',
        indent,
        tracks,
        discs,
        size,
        duration,
    )
    child_row = (
        f'<tr class="child-rows" style="display:none"><td colspan="5">'
        f'<table style="width:100%;border-collapse:collapse">{children_html}</table>'
        f"</td></tr>"
    )
    return row + child_row


def _build_hierarchy_html(artists: list[ArtistBreakdown]) -> str:
    """Build a table with nested expandable rows for the collection hierarchy."""
    parts: list[str] = [
        '<table style="width:100%;border-collapse:collapse">',
        "<thead><tr>"
        '<th style="text-align:left">Name</th>'
        '<th class="num">Tracks</th>'
        '<th class="num">Discs</th>'
        '<th class="num">Size</th>'
        '<th class="num">Duration</th>'
        "</tr></thead>",
        "<tbody>",
    ]

    for a in artists:
        rt_rows: list[str] = []
        for rt in a.release_types:
            album_rows: list[str] = []
            for alb in rt.albums:
                if len(alb.catalogs) == 1:
                    # Single version — show flat
                    album_rows.append(
                        _hierarchy_row(
                            alb.album,
                            3,
                            alb.track_count,
                            alb.disc_count,
                            alb.total_size_bytes,
                            alb.total_duration_seconds,
                        )
                    )
                else:
                    # Multiple releases — expandable
                    cat_rows = "".join(
                        _hierarchy_row(
                            f'<span class="dim">{c.catalog_number}</span>',
                            4,
                            c.track_count,
                            c.disc_count,
                            c.total_size_bytes,
                            c.total_duration_seconds,
                        )
                        for c in alb.catalogs
                    )
                    album_rows.append(
                        _expandable_row(
                            alb.album,
                            3,
                            alb.track_count,
                            alb.disc_count,
                            alb.total_size_bytes,
                            alb.total_duration_seconds,
                            cat_rows,
                        )
                    )

            rt_rows.append(
                _expandable_row(
                    rt.release_type,
                    2,
                    rt.track_count,
                    rt.disc_count,
                    rt.total_size_bytes,
                    rt.total_duration_seconds,
                    "".join(album_rows),
                )
            )

        parts.append(
            _expandable_row(
                f"<strong>{a.artist}</strong>",
                0,
                a.track_count,
                a.disc_count,
                a.total_size_bytes,
                a.total_duration_seconds,
                "".join(rt_rows),
            )
        )

    parts.append("</tbody></table>")

    parts.append("""<script>
document.querySelectorAll('td details').forEach(d => {
  d.addEventListener('toggle', () => {
    const childRow = d.closest('tr').nextElementSibling;
    if (childRow) childRow.style.display = d.open ? '' : 'none';
  });
});
</script>""")

    return "\n".join(parts)


def build_overview_sections(data: OverviewFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    t = data.totals
    sections.append(
        _section(
            "Collection Totals",
            summary_cards=[
                _card(fmt_count(t.total_album_artists), "Album Artists"),
                _card(fmt_count(t.total_albums), "Albums"),
                _card(fmt_count(t.total_releases), "Releases"),
                _card(fmt_count(t.total_tracks), "Tracks"),
            ],
        )
    )

    if data.by_artist:
        hierarchy_html = _build_hierarchy_html(data.by_artist)
        sections.append(
            _section(
                f"Collection Breakdown ({fmt_count(t.total_album_artists)} artists)",
                text_blocks=[_text(hierarchy_html)],
            )
        )

    return sections


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


def _histogram_html(
    title: str,
    buckets: list[BucketCount],
    bar_cls: str | None = None,
) -> str:
    """Build an HTML histogram bar chart from BucketCount instances.

    Each bar's height is proportional to its percentage relative to the
    largest bucket.  Hovering shows a tooltip with count and percentage.
    """
    if not buckets:
        return ""
    max_pct = max(b.percentage for b in buckets) or 1.0
    bars: list[str] = []
    labels: list[str] = []
    for b in buckets:
        height_pct = (b.percentage / max_pct * 100.0) if max_pct > 0 else 0
        cls = f"histogram-bar {bar_cls}" if bar_cls else "histogram-bar"
        tooltip = f"{fmt_count(b.count)} ({fmt_pct(b.percentage)})"
        bars.append(
            f'<div class="{cls}" style="height:{height_pct:.1f}%"'
            f' title="{b.label}: {tooltip}">'
            f'<span class="tooltip">{b.label}: {tooltip}</span>'
            f"</div>"
        )
        labels.append(f"<span>{b.label}</span>")

    return (
        f'<div class="histogram-wrapper">'
        f"<h4>{title}</h4>"
        f'<div class="histogram">{"".join(bars)}</div>'
        f'<div class="histogram-labels">{"".join(labels)}</div>'
        f"</div>"
    )


def build_technical_sections(data: TechnicalFullData) -> list[dict[str, Any]]:
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

    # Histogram charts for distributions — 2-column grid
    charts: list[str] = []
    for label, buckets in [
        ("Sample Rate", data.sample_rate_distribution),
        ("Bit Depth", data.bit_depth_distribution),
        ("Channels", data.channel_distribution),
        ("Approximate Bitrate (kbps)", data.bitrate_distribution),
        ("File Size", data.file_size_distribution),
        ("Track Duration", data.duration_distribution),
    ]:
        charts.append(_histogram_html(label, buckets))

    if charts:
        grid_html = f'<div class="histogram-grid">{"".join(charts)}</div>'
        sections.append(_section("Distributions", text_blocks=[_text(grid_html)]))

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

    return sections


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


def _coverage_table_rows(
    group_data: list[TagCoverageRow],
    severity_threshold: str = "",
) -> list[list[dict[str, Any]]]:
    """Build table rows for a tag coverage group.

    Columns: Tag Key | Coverage (bar + %) | Present
    """
    rows: list[list[dict[str, Any]]] = []
    for row in group_data:
        bar_cls = "bar-green"
        if severity_threshold == "required" and row.missing_count > 0:
            bar_cls = "bar-red"
        elif severity_threshold == "recommended" and row.missing_pct > 20.0:
            bar_cls = "bar-yellow"

        rows.append(
            [
                _cell(row.tag_key, cls="mono"),
                _cell(
                    fmt_pct(row.present_pct),
                    cls="num",
                    bar_pct=row.present_pct,
                    bar_cls=bar_cls,
                ),
                _cell(fmt_count(row.present_count), cls="num"),
            ]
        )
    return rows


_COV_HEADERS = [
    _hdr("Tag Key", cls="cov-col-tag"),
    _hdr("Coverage", cls="cov-col-coverage"),
    _hdr("Present", "num", cls="cov-col-present"),
]


def build_tag_coverage_sections(data: TagCoverageFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Tag Coverage & Key Inventory",
            summary_cards=[_card(fmt_count(data.total_files), "Total Files")],
        )
    )

    # Coverage tables: required, recommended, other tracked + unknown (tags to strip)
    for group_name, group_data, severity in [
        ("Required Tags", data.required_coverage, "required"),
        ("Recommended Tags", data.recommended_coverage, "recommended"),
        ("Other Tracked Tags", data.other_coverage, ""),
    ]:
        rows = _coverage_table_rows(group_data, severity)
        sections.append(_section(group_name, tables=[_table(None, _COV_HEADERS, rows)]))

    # Tags to strip — unknown keys shown with same coverage columns
    if data.unknown_keys:
        unk_rows = _coverage_table_rows(
            [
                TagCoverageRow(
                    tag_key=row.tag_key_upper,
                    present_count=row.file_count,
                    present_pct=row.coverage_pct,
                    missing_count=data.total_files - row.file_count,
                    missing_pct=100.0 - row.coverage_pct,
                )
                for row in data.unknown_keys
            ],
            severity_threshold="",
        )
        sections.append(
            _section(
                f"Tags to Strip ({len(data.unknown_keys)})",
                tables=[_table(None, _COV_HEADERS, unk_rows)],
            )
        )

    # Alias consistency
    if data.alias_usage:
        alias_rows: list[list[dict[str, Any]]] = []
        for row in data.alias_usage:
            bar_cls = "bar-green" if row.consistency_pct >= 100.0 else "bar-red"
            alias_rows.append(
                [
                    _cell(row.canonical_key, cls="mono"),
                    _cell(row.alias_key, cls="mono"),
                    _cell(
                        fmt_pct(row.consistency_pct),
                        bar_pct=row.consistency_pct,
                        bar_cls=bar_cls,
                    ),
                ]
            )
        sections.append(
            _section(
                "Alias Consistency",
                note="For files with the alias key, shows what % also have the canonical key set to the same value. Target: 100%.",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Canonical Key"),
                            _hdr("Alias Key"),
                            _hdr("Consistency", cls="cov-col-coverage"),
                        ],
                        alias_rows,
                    )
                ],
            )
        )

    # Full tag key inventory — collapsible, rendered as raw HTML
    inv_table_rows: list[str] = []
    for row in data.full_inventory:
        cls = f"tag-{row.classification}"
        bar_pct = min(max(row.coverage_pct, 0.0), 100.0)
        inv_table_rows.append(
            f"<tr>"
            f'<td class="{cls}">{row.classification}</td>'
            f'<td class="mono">{row.tag_key_upper}</td>'
            f'<td class="cov-col-coverage">'
            f'<span class="bar-container"><span class="bar-fill bar-green" style="width:{bar_pct:.1f}%"></span></span>'
            f"{fmt_pct(row.coverage_pct)}</td>"
            f'<td class="num">{fmt_count(row.file_count)}</td>'
            f"</tr>"
        )
    inv_html = (
        f'<details style="margin-top:1rem">'
        f'<summary style="cursor:pointer;font-size:1.1rem;font-weight:600;padding:0.5rem 0">'
        f"Full Tag Key Inventory ({len(data.full_inventory)} tags)"
        f"</summary>"
        f'<table style="width:100%;border-collapse:collapse;margin-top:0.5rem">'
        f"<thead><tr>"
        f"<th>Classification</th>"
        f"<th>Tag Key</th>"
        f'<th class="cov-col-coverage">Coverage</th>'
        f'<th class="num">Present</th>'
        f"</tr></thead>"
        f"<tbody>{''.join(inv_table_rows)}</tbody>"
        f"</table>"
        f"</details>"
    )
    sections.append(_section("", text_blocks=[_text(inv_html)]))

    return sections


# =========================================================================
# Report 5 — Tag format quality
# =========================================================================


def _build_quality_group_section(group: TagGroupQuality) -> dict[str, Any]:
    """Build a coverage-style table for a config tag group.

    Each row: Tag Key | Valid | Valid% | Invalid | Invalid% | Absent | Absent%
    Only includes tags with format validators.
    """
    headers = [
        _hdr("Tag Key"),
        _hdr("Valid", "num"),
        _hdr("Valid %", "num"),
        _hdr("Invalid", "num"),
        _hdr("Invalid %", "num"),
        _hdr("Absent", "num"),
        _hdr("Absent %", "num"),
    ]

    rows: list[list[dict[str, Any]]] = []
    for sec in group.fields:
        s = sec.summary
        inv_cls = "severity-error" if s.invalid_count > 0 else ""

        rows.append(
            [
                _cell(sec.field_name, cls="mono"),
                _cell(fmt_count(s.valid_count), cls="num"),
                _cell(fmt_pct(s.valid_pct), cls="num"),
                _cell(fmt_count(s.invalid_count), cls=f"num {inv_cls}".strip()),
                _cell(fmt_pct(s.invalid_pct), cls=f"num {inv_cls}".strip()),
                _cell(fmt_count(s.absent_count), cls="num"),
                _cell(fmt_pct(s.absent_pct), cls="num"),
            ]
        )

    tables: list[dict[str, Any]] = [_table(None, headers, rows)]

    # Invalid values per field (only for fields that actually have them).
    for sec in group.fields:
        if sec.invalid_values:
            iv_rows: list[list[dict[str, Any]]] = []
            for iv in sec.invalid_values:
                iv_rows.append(
                    [_cell(iv.value, cls="mono"), _cell(fmt_count(iv.count), cls="num")]
                )
            footer = None
            if sec.invalid_values_total > len(sec.invalid_values):
                footer = f"Showing top {len(sec.invalid_values)} of {sec.invalid_values_total} distinct invalid values."
            tables.append(
                _table(
                    f"Invalid Values \u2014 {sec.field_name}",
                    [_hdr("Value"), _hdr("Count", "num")],
                    iv_rows,
                    footer=footer,
                )
            )

    return _section(group.group_name, tables=tables)


def _build_id_extras_section(id_section: Any) -> dict[str, Any] | None:
    """Build partial album / duplicate / backfill sub-section for an ID source.

    Coverage is handled by the group tables. This only renders extra insights.
    Returns None if there's nothing to show.
    """
    tables: list[dict[str, Any]] = []
    text_blocks: list[dict[str, Any]] = []

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

    if id_section.duplicate_ids:
        dup_rows: list[list[dict[str, Any]]] = []
        for d in id_section.duplicate_ids:
            same_text = "Yes" if d.same_directory else "No"
            same_cls = "severity-error" if d.same_directory else "severity-warning"
            paths_str = "<br>".join(d.file_paths[:10])
            if len(d.file_paths) > 10:
                paths_str += f"<br>\u2026 +{len(d.file_paths) - 10} more"
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

    if id_section.backfill:
        bf = id_section.backfill
        text_blocks.append(
            _text(
                f"<strong>Quick-win backfill:</strong> {fmt_count(bf.affected_tracks)} tracks, "
                f"{fmt_count(bf.distinct_source_ids)} API calls needed. {bf.description}"
            )
        )

    if not tables and not text_blocks:
        return None

    return _section(
        f"{id_section.source_name} — Issues",
        tables=tables or None,
        text_blocks=text_blocks or None,
    )


def build_tag_quality_sections(data: TagQualityFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Tag Quality & Integrity",
            summary_cards=[_card(fmt_count(data.total_files), "Total Files")],
        )
    )

    # Date format quality table.
    if data.date_quality:
        date_rows: list[list[dict[str, Any]]] = []
        for dq in data.date_quality:
            inv_cls = "severity-error" if dq.invalid_count > 0 else ""
            date_rows.append(
                [
                    _cell(dq.field_name, cls="mono"),
                    _cell(fmt_count(dq.full_date_count), cls="num"),
                    _cell(fmt_count(dq.year_only_count), cls="num"),
                    _cell(fmt_count(dq.invalid_count), cls=f"num {inv_cls}".strip()),
                    _cell(fmt_count(dq.missing_count), cls="num"),
                ]
            )
        sections.append(
            _section(
                "Date Format Quality",
                tables=[
                    _table(
                        None,
                        [
                            _hdr("Tag Key"),
                            _hdr("Full Date", "num"),
                            _hdr("Year Only", "num"),
                            _hdr("Invalid", "num"),
                            _hdr("Missing", "num"),
                        ],
                        date_rows,
                    )
                ],
            )
        )

    # Grouped field validation tables (required, recommended, other).
    for group in data.groups:
        if group.fields:
            sections.append(_build_quality_group_section(group))

    # External ID extras (partial albums, duplicates, backfill — coverage is in group tables).
    for id_section in [data.musicbrainz, data.discogs]:
        extras = _build_id_extras_section(id_section)
        if extras:
            sections.append(extras)

    return sections


# =========================================================================
# Report 6 — Intra-album consistency
# =========================================================================


def build_album_consistency_sections(
    data: AlbumConsistencyFullData,
) -> list[dict[str, Any]]:
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

    if data.field_violations:
        fv_rows: list[list[dict[str, Any]]] = []
        for v in data.field_violations:
            vals_display = " | ".join(
                f"{val} ({v.track_counts_per_value.get(val, '?')})"
                for val in v.distinct_values[:5]
            )
            if len(v.distinct_values) > 5:
                vals_display += f" \u2026 +{len(v.distinct_values) - 5} more"
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

    return sections


# =========================================================================
# Report 8 — Duplicates
# =========================================================================


def build_duplicates_sections(data: DuplicatesFullData) -> list[dict[str, Any]]:
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

    if data.exact:
        exact_rows: list[list[dict[str, Any]]] = []
        for g in data.exact:
            paths_str = "<br>".join(g.paths[:10])
            if len(g.paths) > 10:
                paths_str += f"<br>\u2026 +{len(g.paths) - 10} more"
            exact_rows.append(
                [
                    _cell(g.checksum[:20] + "\u2026", cls="mono"),
                    _cell(g.copy_count, cls="num"),
                    _cell(paths_str, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Exact Duplicates (SHA-256) \u2014 {len(data.exact)} group(s)",
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

    if data.mbid:
        mbid_rows: list[list[dict[str, Any]]] = []
        for g in data.mbid:
            same_text = "Yes" if g.same_directory else "No"
            same_cls = "severity-error" if g.same_directory else "severity-warning"
            file_details = "<br>".join(
                f"{f.path} [{f.album} / {f.date}]" for f in g.files[:10]
            )
            if len(g.files) > 10:
                file_details += f"<br>\u2026 +{len(g.files) - 10} more"
            mbid_rows.append(
                [
                    _cell(g.mbid[:20] + "\u2026", cls="mono"),
                    _cell(g.file_count, cls="num"),
                    _cell(same_text, cls=same_cls),
                    _cell(file_details, cls="path"),
                ]
            )
        sections.append(
            _section(
                f"Same Recording Duplicates (MUSICBRAINZ_TRACKID) \u2014 {len(data.mbid)} group(s)",
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

    if data.probable:
        prob_rows: list[list[dict[str, Any]]] = []
        for g in data.probable:
            paths_str = "<br>".join(g.paths[:5])
            if len(g.paths) > 5:
                paths_str += f"<br>\u2026 +{len(g.paths) - 5} more"
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
                f"Probable Duplicates (by position) \u2014 {len(data.probable)} group(s)",
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

    return sections


# =========================================================================
# Report 9 — Scan issues
# =========================================================================


def _album_severity_class(album: Any) -> str:
    """Return CSS severity class for an album based on its worst issue level."""
    if album.error_count > 0:
        return "sev-error"
    if album.warning_count > 0:
        return "sev-warning"
    if album.info_count > 0:
        return "sev-info"
    return "sev-clean"


def _album_badges_html(album: Any) -> str:
    """Build badge HTML for an album's severity counts."""
    badges: list[str] = []
    if album.error_count > 0:
        badges.append(f'<span class="badge badge-error">{album.error_count}</span>')
    if album.warning_count > 0:
        badges.append(f'<span class="badge badge-warning">{album.warning_count}</span>')
    if album.info_count > 0:
        badges.append(f'<span class="badge badge-info">{album.info_count}</span>')
    return "".join(badges)


def build_issues_sections(data: IssuesFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Scan Issues by Album",
            summary_cards=[
                _card(fmt_count(data.total_albums), "Albums with Issues"),
                _card(fmt_count(data.total_files_with_issues), "Files with Issues"),
                _card(fmt_count(data.total_issues), "Total Issues"),
            ],
        )
    )

    if data.total_albums == 0:
        sections.append(
            _section(
                "Result",
                text_blocks=[_text("No albums with scan issues.", cls="dim")],
            )
        )
        return sections

    from urllib.parse import quote

    # Build tracklist-style album rows
    rows_html: list[str] = []
    for album in data.albums:
        encoded_dir = quote(album.album_dir, safe="")
        link = f"/report/album-issues?dir={encoded_dir}"
        sev_cls = _album_severity_class(album)
        cat_display = f" [{album.catalog_number}]" if album.catalog_number else ""
        badges = _album_badges_html(album)

        rows_html.append(
            f'<a href="{link}" class="album-row" style="text-decoration:none;color:inherit">'
            f'<div class="album-row-inner">'
            f'<div class="album-indicator {sev_cls}"></div>'
            f'<div class="album-artist">{album.artist}</div>'
            f'<div class="album-title">{album.album}{cat_display}</div>'
            f'<div class="album-badges">{badges}</div>'
            f'</div>'
            f'</a>'
        )

    # CSS for album rows (injected once)
    style = (
        '<style>'
        '.album-row{display:block;border-bottom:1px solid var(--bg-alt);transition:background 0.1s}'
        '.album-row:hover{background:var(--bg-alt)}'
        '.album-row-inner{display:flex;align-items:center;padding:0.5rem 0;gap:0.75rem}'
        '.album-indicator{width:4px;border-radius:2px;align-self:stretch;min-height:1.5rem;flex-shrink:0}'
        '.sev-error{background:var(--red)}'
        '.sev-warning{background:var(--yellow)}'
        '.sev-info{background:var(--dim)}'
        '.sev-clean{background:var(--green)}'
        '.album-artist{font-size:0.85rem;color:var(--dim);flex-shrink:0;width:20%}'
        '.album-title{flex:1;font-size:0.9rem;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
        '.album-badges{display:flex;gap:0.3rem;flex-shrink:0}'
        '.badge{font-size:0.7rem;font-weight:700;padding:0.1rem 0.4rem;border-radius:4px;line-height:1.3}'
        '.badge-error{background:var(--red-bg);color:var(--red)}'
        '.badge-warning{background:var(--yellow-bg);color:var(--yellow)}'
        '.badge-info{background:var(--bg-alt);color:var(--dim)}'
        '</style>'
    )

    list_html = style + "".join(rows_html)

    sections.append(
        _section(
            f"Albums with Issues ({fmt_count(data.total_albums)})",
            text_blocks=[_text(list_html)],
        )
    )

    return sections


def build_album_issues_sections(data: Any) -> list[dict[str, Any]]:
    """Build HTML sections for a single album's issue detail page."""
    sections: list[dict[str, Any]] = []

    cat_display = data.catalog_number or ""
    sections.append(
        _section(
            f"{data.artist} \u2014 {data.album}",
            summary_cards=[
                _card(str(data.total_files), "Files in Album"),
                _card(str(data.files_with_issues), "Files with Issues"),
                _card(str(data.error_count), "Errors"),
                _card(str(data.warning_count), "Warnings"),
                _card(str(data.info_count), "Info"),
            ],
            text_blocks=[
                _text(f'<span class="dim">Directory: <code>{data.album_dir}</code></span>')
            ] + ([_text(f'<span class="dim">Catalog #: {cat_display}</span>')] if cat_display else []),
        )
    )

    if data.issues:
        issue_rows: list[list[dict[str, Any]]] = []
        for issue in data.issues:
            sev_cls = f"severity-{issue.severity}"
            issue_rows.append([
                _cell(issue.severity.upper(), cls=sev_cls),
                _cell(issue.file_name, cls="mono"),
                _cell(issue.issue_type, cls="mono"),
                _cell(issue.description),
            ])
        sections.append(
            _section(
                "Issues",
                tables=[_table(None, [
                    _hdr("Severity"),
                    _hdr("File"),
                    _hdr("Issue Type"),
                    _hdr("Description"),
                ], issue_rows)],
            )
        )
    else:
        sections.append(
            _section("Issues", text_blocks=[_text("No issues found.", cls="dim")])
        )

    return sections


# =========================================================================
# Report 10 — Artist name consistency
# =========================================================================


def build_artists_sections(data: ArtistsFullData) -> list[dict[str, Any]]:
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

    if data.albumartist_case_variants:
        cv_text: list[dict[str, Any]] = []
        for g in data.albumartist_case_variants:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            cv_text.append(
                _text(
                    f'<span class="severity-error">\u25cf</span> {variants_str} '
                    f"(total: {fmt_count(g.total_track_count)} tracks)"
                )
            )
        sections.append(
            _section(
                f"ALBUMARTIST Case/Whitespace Variants ({len(data.albumartist_case_variants)} groups)",
                text_blocks=cv_text,
            )
        )

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

    if data.artist_case_variants:
        acv_text: list[dict[str, Any]] = []
        for g in data.artist_case_variants[:50]:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            acv_text.append(
                _text(
                    f'<span class="severity-error">\u25cf</span> {variants_str} '
                    f"(total: {fmt_count(g.total_track_count)} tracks)"
                )
            )
        if len(data.artist_case_variants) > 50:
            acv_text.append(
                _text(
                    f"\u2026 and {len(data.artist_case_variants) - 50} more groups.",
                    cls="dim",
                )
            )
        sections.append(
            _section(
                f"ARTIST Case/Whitespace Variants ({len(data.artist_case_variants)} groups)",
                text_blocks=acv_text,
            )
        )

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

    return sections


# =========================================================================
# Report 11 — Genre analysis
# =========================================================================


def build_genres_sections(data: GenresFullData) -> list[dict[str, Any]]:
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

    return sections


# =========================================================================
# Report 12 — Lyrics coverage
# =========================================================================


def build_lyrics_sections(data: LyricsFullData) -> list[dict[str, Any]]:
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

    return sections


# =========================================================================
# Report 13 — ReplayGain coverage
# =========================================================================


def build_replaygain_sections(data: ReplayGainFullData) -> list[dict[str, Any]]:
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
                note="Partial album ReplayGain is worse than none \u2014 it produces inconsistent playback volume.",
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

    if data.gain_distribution:
        sections.append(
            _section(
                "Track Gain Distribution (REPLAYGAIN_TRACK_GAIN)",
                tables=[_bucket_table(None, data.gain_distribution)],
            )
        )

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
                f"Gain Outliers (outside [-20, +10] dB) \u2014 {len(data.outliers)} found",
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

    return sections


# ---------------------------------------------------------------------------
# Section builder registry
# ---------------------------------------------------------------------------

SECTION_BUILDERS: dict[str, Callable[..., list[dict[str, Any]]]] = {
    "overview": build_overview_sections,
    "technical": build_technical_sections,
    "tags": build_tag_coverage_sections,
    "tag_quality": build_tag_quality_sections,
    "album_consistency": build_album_consistency_sections,
    "duplicates": build_duplicates_sections,
    "issues": build_issues_sections,
    "artists": build_artists_sections,
    "genres": build_genres_sections,
    "lyrics": build_lyrics_sections,
    "replaygain": build_replaygain_sections,
}
