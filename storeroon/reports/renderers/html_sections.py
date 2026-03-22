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
    ArtistBreakdown,
    ArtistsFullData,
    BucketCount,
    CollectionIssuesFullData,
    GenresFullData,
    LyricsFullData,
    OverviewFullData,
    ReplayGainFullData,
    TagBar,
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
    "collection_issues": "Collection Issues Overview",
    "key_inventory": "Key Inventory",
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
# Collection overview (with scan issues)
# =========================================================================


# =========================================================================
# Artist name consistency
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
# Genre analysis
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
# Lyrics coverage
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
# ReplayGain coverage
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

# =========================================================================
# Report 1b — Collection overview 2 (with scan issues)
# =========================================================================


def _severity_class_from_counts(critical: int, error: int, warning: int, info: int) -> str:
    """Return CSS severity class from issue counts."""
    if critical > 0:
        return "sev-critical"
    if error > 0:
        return "sev-error"
    if warning > 0:
        return "sev-warning"
    if info > 0:
        return "sev-info"
    return "sev-clean"


def _issue_badges_html(critical: int, error: int, warning: int, info: int) -> str:
    """Build severity badge HTML, hiding zero counts."""
    badges: list[str] = []
    if critical > 0:
        badges.append(f'<span class="badge badge-critical">{critical}</span>')
    if error > 0:
        badges.append(f'<span class="badge badge-error">{error}</span>')
    if warning > 0:
        badges.append(f'<span class="badge badge-warning">{warning}</span>')
    if info > 0:
        badges.append(f'<span class="badge badge-info">{info}</span>')
    return "".join(badges)


def _build_overview_html(artists: list[ArtistBreakdown]) -> str:
    """Build the overview hierarchy with issue indicators."""
    from urllib.parse import quote

    style = (
        "<style>"
        ".ov-row{display:flex;align-items:center;padding:0.5rem 0;gap:0.75rem;"
        "cursor:pointer;border-bottom:1px solid var(--bg-alt);transition:background 0.1s}"
        ".ov-row:hover{background:var(--bg-alt)}"
        ".ov-indicator{width:4px;border-radius:2px;align-self:stretch;min-height:1.5rem;flex-shrink:0}"
        ".ov-name{flex:1;font-size:0.9rem;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
        ".ov-name strong{font-weight:600}"
        ".ov-right{display:flex;gap:0.35rem;flex-shrink:0;align-items:center}"
        ".ov-stat{font-size:0.7rem;font-weight:600;padding:0.15rem 0.5rem;border-radius:4px;"
        "background:var(--bg-alt);color:var(--dim);font-variant-numeric:tabular-nums;"
        "min-width:4.5rem;text-align:right;display:inline-block}"
        ".ov-issue-badges{display:flex;gap:0.25rem;margin-left:0.5rem}"
        ".ov-children{display:none;padding-left:1.5rem}"
        ".ov-children.open{display:block}"
        ".ov-dim{color:var(--dim);font-size:0.85rem}"
        "a.ov-row{text-decoration:none;color:inherit}"
        ".sev-critical{background:var(--red)}"
        ".sev-error{background:var(--red)}"
        ".sev-warning{background:var(--yellow)}"
        ".sev-info{background:var(--dim)}"
        ".sev-clean{background:var(--green)}"
        ".badge{font-size:0.7rem;font-weight:700;padding:0.1rem 0.4rem;border-radius:4px;line-height:1.3}"
        ".badge-critical{background:var(--red-bg);color:var(--red)}"
        ".badge-error{background:var(--red-bg);color:var(--red)}"
        ".badge-warning{background:var(--yellow-bg);color:var(--yellow)}"
        ".badge-info{background:var(--bg-alt);color:var(--dim)}"
        "</style>"
    )

    rows: list[str] = [style]
    _counter = [0]

    def _uid() -> str:
        _counter[0] += 1
        return f"ov-{_counter[0]}"

    def _stats(album_count: int, track_count: int) -> str:
        return (
            f'<span class="ov-stat">{fmt_count(album_count)} albums</span>'
            f'<span class="ov-stat">{fmt_count(track_count)} tracks</span>'
        )

    def _album_stats(track_count: int) -> str:
        return f'<span class="ov-stat">{fmt_count(track_count)} tracks</span>'

    def _health_sev_class(score: int) -> str:
        if score >= 80:
            return "sev-clean"
        if score >= 50:
            return "sev-warning"
        return "sev-error"

    for a in artists:
        aid = _uid()
        ibadges = _issue_badges_html(a.critical_count, a.error_count, a.warning_count, a.info_count)
        sev = _severity_class_from_counts(a.critical_count, a.error_count, a.warning_count, a.info_count)
        rows.append(
            f'<div class="ov-row" onclick="toggleOv(\'{aid}\')">'
            f'<div class="ov-indicator {sev}"></div>'
            f'<div class="ov-name"><strong>{a.artist}</strong></div>'
            f'<div class="ov-right">{_stats(a.album_count, a.track_count)}'
            f'<div class="ov-issue-badges">{ibadges}</div></div>'
            f"</div>"
            f'<div class="ov-children" id="{aid}">'
        )

        for rt in a.release_types:
            rid = _uid()
            rt_badges = _issue_badges_html(rt.critical_count, rt.error_count, rt.warning_count, rt.info_count)
            rt_sev = _severity_class_from_counts(rt.critical_count, rt.error_count, rt.warning_count, rt.info_count)
            rows.append(
                f'<div class="ov-row" onclick="toggleOv(\'{rid}\'); event.stopPropagation()">'
                f'<div class="ov-indicator {rt_sev}"></div>'
                f'<div class="ov-name ov-dim">{rt.release_type}</div>'
                f'<div class="ov-right">{_stats(rt.album_count, rt.track_count)}'
                f'<div class="ov-issue-badges">{rt_badges}</div></div>'
                f"</div>"
                f'<div class="ov-children" id="{rid}">'
            )

            for alb in rt.albums:
                encoded_dir = quote(alb.album_dir, safe="")
                link = f"/report/album-issues?dir={encoded_dir}"
                alb_badges = _issue_badges_html(alb.critical_count, alb.error_count, alb.warning_count, alb.info_count)
                alb_sev = _health_sev_class(alb.health_score)
                rows.append(
                    f'<a class="ov-row" href="{link}" onclick="event.stopPropagation()">'
                    f'<div class="ov-indicator {alb_sev}"></div>'
                    f'<div class="ov-name">{alb.display_name}</div>'
                    f'<div class="ov-right">{_album_stats(alb.track_count)}'
                    f'<div class="ov-issue-badges">{alb_badges}</div></div>'
                    f"</a>"
                )

            rows.append("</div>")
        rows.append("</div>")

    rows.append("""<script>
function toggleOv(id) {
  var el = document.getElementById(id);
  if (el) el.classList.toggle('open');
}
</script>""")

    return "\n".join(rows)


def build_overview_sections(data: OverviewFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    t = data.totals
    it = data.issues_totals
    sections.append(
        _section(
            "Collection Totals",
            summary_cards=[
                _card(fmt_count(t.total_album_artists), "Album Artists"),
                _card(fmt_count(t.total_albums), "Albums"),
                _card(fmt_count(t.total_tracks), "Tracks"),
                _card(fmt_size_gb(t.total_size_bytes), "Size"),
                _card(fmt_duration_hms(t.total_duration_seconds), "Duration"),
            ],
        )
    )

    sections.append(
        _section(
            "Scan Issues",
            summary_cards=[
                _card(fmt_count(it.albums_with_issues), "Albums with Issues"),
                _card(fmt_count(it.files_with_issues), "Files with Issues"),
                _card(fmt_count(it.total_issues), "Total Issues"),
            ],
        )
    )

    if data.by_artist:
        hierarchy_html = _build_overview_html(data.by_artist)
        sections.append(
            _section(
                "Collection Breakdown",
                text_blocks=[_text(hierarchy_html)],
            )
        )

    return sections


# =========================================================================
# Collection Issues Overview
# =========================================================================


def _two_seg_bar(label: str, clean_pct: float, affected: int, total: int) -> str:
    """Build a 2-segment stacked bar (clean / affected)."""
    affected_pct = 100.0 - clean_pct
    legend_parts: list[str] = [
        f'<span class="leg-item"><span class="leg-dot leg-dot-clean"></span>clean: {fmt_pct(clean_pct)} [{fmt_count(total - affected)}]</span>',
    ]
    if affected > 0:
        legend_parts.append(
            f'<span class="leg-item"><span class="leg-dot leg-dot-affected"></span>affected: {fmt_pct(affected_pct)} [{fmt_count(affected)}]</span>'
        )
    return (
        f'<div class="stacked-row">'
        f'<div class="stacked-label"><span>{label}</span></div>'
        f'<div class="stacked-bar">'
        f'<div class="seg seg-clean" style="width:{clean_pct:.1f}%"></div>'
        f'<div class="seg seg-affected" style="width:{affected_pct:.1f}%"></div>'
        f'</div>'
        f'<div class="stacked-legend">{"".join(legend_parts)}</div>'
        f'</div>'
    )


def _tag_bar_html(tag: TagBar) -> str:
    """Build a stacked bar for a tag with valid/invalid/misencoded/missing segments."""
    has_invalid = tag.invalid_count > 0 or tag.invalid_pct > 0
    has_misencoded = tag.misencoded_count > 0
    # For tags with no validator, invalid is always 0 — don't show that segment
    show_invalid = has_invalid or tag.invalid_pct > 0
    # Determine if this tag has a validator by checking if invalid + valid + missing == total
    # (if no validator, all present = valid, so invalid_count stays 0)
    has_validator = (tag.valid_count + tag.invalid_count + tag.missing_count) > 0 and tag.invalid_count > 0

    # Build segments
    segments = (
        f'<div class="seg seg-valid" style="width:{tag.valid_pct:.1f}%"></div>'
        f'<div class="seg seg-invalid" style="width:{tag.invalid_pct:.1f}%"></div>'
        f'<div class="seg seg-misencoded" style="width:{tag.misencoded_pct:.1f}%"></div>'
        f'<div class="seg seg-missing" style="width:{tag.missing_pct:.1f}%"></div>'
    )

    # Build legend
    legend_parts = [
        f'<span class="leg-item"><span class="leg-dot leg-dot-valid"></span>'
        f'valid: {fmt_pct(tag.valid_pct)} [{fmt_count(tag.valid_count)}]</span>',
    ]
    if tag.invalid_count > 0:
        legend_parts.append(
            f'<span class="leg-item"><span class="leg-dot leg-dot-invalid"></span>'
            f'invalid: {fmt_pct(tag.invalid_pct)} [{fmt_count(tag.invalid_count)}]</span>'
        )
    if has_misencoded:
        legend_parts.append(
            f'<span class="leg-item"><span class="leg-dot leg-dot-misencoded"></span>'
            f'misencoded: {fmt_pct(tag.misencoded_pct)} [{fmt_count(tag.misencoded_count)}]</span>'
        )
    if tag.missing_count > 0:
        legend_parts.append(
            f'<span class="leg-item"><span class="leg-dot leg-dot-missing"></span>'
            f'missing: {fmt_pct(tag.missing_pct)} [{fmt_count(tag.missing_count)}]</span>'
        )

    return (
        f'<div class="stacked-row">'
        f'<div class="stacked-label"><span class="tag-name">{tag.tag_key}</span></div>'
        f'<div class="stacked-bar">{segments}</div>'
        f'<div class="stacked-legend">{"".join(legend_parts)}</div>'
        f'</div>'
    )


def build_collection_issues_sections(data: CollectionIssuesFullData) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Collection Issues Overview",
            summary_cards=[
                _card(fmt_count(data.total_albums), "Albums"),
                _card(fmt_count(data.total_files), "Files"),
            ],
        )
    )

    # Album Health
    if data.album_health:
        bars_html = "".join(
            _two_seg_bar(b.issue_label, b.clean_pct, b.albums_affected, b.total_albums)
            for b in data.album_health
        )
        sections.append(
            _section("Album Health", text_blocks=[_text(bars_html)])
        )

    # Track Health
    if data.track_health:
        bars_html = "".join(
            _two_seg_bar(b.issue_label, b.clean_pct, b.files_affected, b.total_files)
            for b in data.track_health
        )
        sections.append(
            _section("Track Health", text_blocks=[_text(bars_html)])
        )

    # Tag Quality sections
    for group_name, tags in [
        ("Required Tags", data.required_tags),
        ("Recommended Tags", data.recommended_tags),
        ("Other Tracked Tags", data.other_tags),
    ]:
        if tags:
            bars_html = "".join(_tag_bar_html(t) for t in tags)
            sections.append(
                _section(group_name, text_blocks=[_text(bars_html)])
            )

    # Alias Consistency
    if data.alias_consistency:
        alias_bars = "".join(
            _two_seg_bar(
                f"{row.canonical_key} \u2194 {row.alias_key}",
                row.consistency_pct,
                round((100.0 - row.consistency_pct) / 100.0 * data.total_files) if data.total_files else 0,
                data.total_files,
            )
            for row in data.alias_consistency
        )
        sections.append(
            _section(
                "Alias Consistency",
                note="For files with the canonical tag, shows what % also have the alias tag set to the same value. Target: 100%.",
                text_blocks=[_text(alias_bars)],
            )
        )

    return sections


# =========================================================================
# Key Inventory
# =========================================================================

_CLASSIFICATION_DISPLAY: dict[str, str] = {
    "required": "Required",
    "recommended": "Recommended",
    "other": "Other",
    "alias": "Optional",
    "standard_optional": "Optional",
    "strip": "To be stripped",
    "unknown": "To be stripped",
}

_CLASSIFICATION_ORDER: dict[str, int] = {
    "required": 0,
    "recommended": 1,
    "other": 2,
    "alias": 3,
    "standard_optional": 3,
    "strip": 4,
    "unknown": 4,
}


def build_key_inventory_sections(data: Any) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []

    sections.append(
        _section(
            "Key Inventory",
            summary_cards=[
                _card(fmt_count(data.total_files), "Total Files"),
                _card(fmt_count(len(data.inventory)), "Distinct Tag Keys"),
            ],
        )
    )

    # Sort by classification order, then by tag key
    sorted_inv = sorted(
        data.inventory,
        key=lambda r: (_CLASSIFICATION_ORDER.get(r.classification, 9), r.tag_key_upper),
    )

    rows: list[list[dict[str, Any]]] = []
    for row in sorted_inv:
        display_cls = _CLASSIFICATION_DISPLAY.get(row.classification, row.classification)
        css_cls = f"tag-{row.classification}"
        rows.append([
            _cell(display_cls, cls=css_cls),
            _cell(row.tag_key_upper, cls="mono"),
            _cell(fmt_count(row.file_count), cls="num"),
            _cell(fmt_pct(row.coverage_pct), cls="num"),
        ])

    sections.append(
        _section(
            f"All Tag Keys ({len(data.inventory)})",
            tables=[_table(None, [
                _hdr("Classification"),
                _hdr("Tag Key"),
                _hdr("Present", "num"),
                _hdr("Coverage", "num"),
            ], rows)],
        )
    )

    return sections


SECTION_BUILDERS: dict[str, Callable[..., list[dict[str, Any]]]] = {
    "overview": build_overview_sections,
    "collection_issues": build_collection_issues_sections,
    "key_inventory": build_key_inventory_sections,
    "artists": build_artists_sections,
    "genres": build_genres_sections,
    "lyrics": build_lyrics_sections,
    "replaygain": build_replaygain_sections,
}
