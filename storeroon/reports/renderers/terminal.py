"""
storeroon.reports.renderers.terminal — Rich terminal renderer for Sprint 2 reports.

Accepts report data models and renders them as Rich tables, panels, and
text to the console. Knows nothing about the database.

Every ``render_*`` function accepts a Rich ``Console`` and one of the
dataclass instances from ``storeroon.reports.models``.
"""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from storeroon.reports.models import (
    AlbumConsistencyFullData,
    AlbumConsistencySummaryData,
    ArtistsFullData,
    ArtistsSummaryData,
    BucketCount,
    DuplicatesFullData,
    DuplicatesSummaryData,
    GenresFullData,
    GenresSummaryData,
    IdsFullData,
    IdsSummaryData,
    IssuesFullData,
    IssuesSummaryData,
    LyricsFullData,
    LyricsSummaryData,
    MasterSummary,
    OverviewFullData,
    OverviewSummaryData,
    ReplayGainFullData,
    ReplayGainSummaryData,
    TagCoverageFullData,
    TagCoverageSummaryData,
    TagFormatsFullData,
    TagFormatsSummaryData,
    TechnicalFullData,
    TechnicalSummaryData,
)
from storeroon.reports.utils import (
    bar_chart,
    fmt_bytes,
    fmt_count,
    fmt_duration_hms,
    fmt_duration_short,
    fmt_pct,
    fmt_size_gb,
    now_iso,
    severity_style,
)

# =========================================================================
# Helpers
# =========================================================================


def _section_heading(console: Console, title: str) -> None:
    """Print a prominent section heading."""
    console.print()
    console.print(f"[bold cyan]── {title} ──[/bold cyan]")
    console.print()


def _subsection_heading(console: Console, title: str) -> None:
    """Print a smaller sub-section heading."""
    console.print()
    console.print(f"[bold]{title}[/bold]")


def _distribution_table(
    title: str,
    buckets: list[BucketCount],
    show_bar: bool = True,
) -> Table:
    """Build a Rich table for a distribution (histogram) with optional bar chart."""
    table = Table(title=title, show_header=True, header_style="bold")
    table.add_column("Bucket", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("%", justify="right")
    if show_bar:
        table.add_column("Distribution", no_wrap=True)

    max_count = max((b.count for b in buckets), default=1)
    for b in buckets:
        row: list[str] = [b.label, fmt_count(b.count), fmt_pct(b.percentage)]
        if show_bar:
            row.append(bar_chart(b.count, max_count, width=30))
        table.add_row(*row)

    return table


def _empty_db_message(console: Console) -> None:
    """Print a message indicating the database is empty."""
    console.print(
        "[yellow]The database is empty — no files have been imported yet. "
        "Run [bold]storeroon scan[/bold] first.[/yellow]"
    )


def _no_results_message(console: Console) -> None:
    """Print a message indicating no tracks matched the given filters."""
    console.print("[yellow]No tracks matched the given filters.[/yellow]")


# =========================================================================
# Report 1 — Collection overview
# =========================================================================


def _indent(text: str, level: int) -> str:
    """Indent text with spaces for hierarchy depth."""
    return "  " * level + text


def render_overview(console: Console, data: OverviewFullData) -> None:
    """Render the full collection overview report."""
    if data.totals.total_tracks == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Collection Overview")

    # Top-level totals.
    t = data.totals
    totals_table = Table(title="Collection Totals", show_header=False, min_width=50)
    totals_table.add_column("Metric", style="bold")
    totals_table.add_column("Value", justify="right")
    totals_table.add_row("Album artists", fmt_count(t.total_album_artists))
    totals_table.add_row("Albums", fmt_count(t.total_albums))
    totals_table.add_row("Releases", fmt_count(t.total_releases))
    totals_table.add_row("Tracks", fmt_count(t.total_tracks))
    totals_table.add_row("Duration", fmt_duration_hms(t.total_duration_seconds))
    totals_table.add_row("Size", fmt_size_gb(t.total_size_bytes))
    console.print(totals_table)

    # Hierarchical breakdown as an indented table.
    if data.by_artist:
        _subsection_heading(console, "Collection Breakdown")
        tbl = Table(show_header=True, header_style="bold", pad_edge=False)
        tbl.add_column("Name", min_width=40)
        tbl.add_column("Tracks", justify="right")
        tbl.add_column("Discs", justify="right")
        tbl.add_column("Size", justify="right")
        tbl.add_column("Duration", justify="right")

        for a in data.by_artist:
            tbl.add_row(
                f"[cyan bold]{a.artist}[/cyan bold]",
                fmt_count(a.track_count),
                fmt_count(a.disc_count),
                fmt_size_gb(a.total_size_bytes),
                fmt_duration_hms(a.total_duration_seconds),
            )
            for rt in a.release_types:
                tbl.add_row(
                    f"[green]{_indent(rt.release_type, 1)}[/green]",
                    fmt_count(rt.track_count),
                    fmt_count(rt.disc_count),
                    fmt_size_gb(rt.total_size_bytes),
                    fmt_duration_hms(rt.total_duration_seconds),
                )
                for alb in rt.albums:
                    tbl.add_row(
                        _indent(alb.album, 2),
                        fmt_count(alb.track_count),
                        fmt_count(alb.disc_count),
                        fmt_size_gb(alb.total_size_bytes),
                        fmt_duration_hms(alb.total_duration_seconds),
                    )
                    if len(alb.catalogs) > 1:
                        for c in alb.catalogs:
                            tbl.add_row(
                                f"[dim]{_indent(c.catalog_number, 3)}[/dim]",
                                f"[dim]{fmt_count(c.track_count)}[/dim]",
                                f"[dim]{fmt_count(c.disc_count)}[/dim]",
                                f"[dim]{fmt_size_gb(c.total_size_bytes)}[/dim]",
                                f"[dim]{fmt_duration_hms(c.total_duration_seconds)}[/dim]",
                            )
        console.print(tbl)


def render_overview_summary(console: Console, data: OverviewSummaryData) -> None:
    """Render overview totals in summary mode."""
    if data.totals.total_tracks == 0:
        return

    t = data.totals
    table = Table(
        title="[bold]📊 Collection Overview[/bold]",
        show_header=False,
        min_width=42,
    )
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Album artists", fmt_count(t.total_album_artists))
    table.add_row("Albums", fmt_count(t.total_albums))
    table.add_row("Releases", fmt_count(t.total_releases))
    table.add_row("Tracks", fmt_count(t.total_tracks))
    table.add_row("Duration", fmt_duration_hms(t.total_duration_seconds))
    table.add_row("Size", fmt_size_gb(t.total_size_bytes))
    console.print(table)


# =========================================================================
# Report 2 — Audio technical quality
# =========================================================================


def render_technical(console: Console, data: TechnicalFullData) -> None:
    """Render the full technical quality report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Audio Technical Quality")
    console.print(f"Total files analysed: [bold]{fmt_count(data.total_files)}[/bold]")

    # Table 1: Sample rate distribution.
    console.print(
        _distribution_table("Sample Rate Distribution", data.sample_rate_distribution)
    )

    # Table 2: Bit depth distribution.
    console.print(
        _distribution_table("Bit Depth Distribution", data.bit_depth_distribution)
    )

    # Table 3: Channel distribution.
    console.print(
        _distribution_table("Channel Distribution", data.channel_distribution)
    )

    # Table 4: Bitrate distribution.
    console.print(
        _distribution_table(
            "Approximate Bitrate Distribution (kbps)", data.bitrate_distribution
        )
    )

    # Table 5: File size distribution.
    console.print(
        _distribution_table("File Size Distribution", data.file_size_distribution)
    )

    # Table 6: Duration distribution.
    console.print(
        _distribution_table("Track Duration Distribution", data.duration_distribution)
    )

    # Table 7: Duration outliers.
    if data.duration_outliers:
        _subsection_heading(
            console, f"Duration Outliers ({len(data.duration_outliers)} tracks)"
        )
        outlier_table = Table(show_header=True, header_style="bold")
        outlier_table.add_column("Type", style="bold")
        outlier_table.add_column("Duration", justify="right")
        outlier_table.add_column("Artist")
        outlier_table.add_column("Album")
        outlier_table.add_column("Title")
        outlier_table.add_column("Path", style="dim")

        for o in data.duration_outliers:
            style = "red" if o.outlier_type == "short" else "yellow"
            outlier_table.add_row(
                Text(o.outlier_type.upper(), style=style),
                fmt_duration_short(o.duration_seconds),
                o.albumartist,
                o.album,
                o.title,
                o.path,
            )
        console.print(outlier_table)
    else:
        console.print("[green]No duration outliers found.[/green]")

    # Table 8: Encoder provenance.
    if data.vendors:
        _subsection_heading(console, "Encoder Provenance (vendor_string)")
        vendor_table = Table(show_header=True, header_style="bold")
        vendor_table.add_column("Vendor String")
        vendor_table.add_column("Count", justify="right")
        vendor_table.add_column("Status")

        for v in data.vendors:
            status = (
                Text("⚠ SUSPICIOUS", style="bold red")
                if v.is_suspicious
                else Text("OK", style="green")
            )
            vendor_table.add_row(v.vendor_string, fmt_count(v.count), status)
        console.print(vendor_table)

    # Table 9: Missing audio_md5.
    _subsection_heading(console, "Missing audio_md5")
    console.print(
        f"Files missing audio_md5: [bold]{fmt_count(data.missing_md5_count)}[/bold] "
        f"({fmt_pct(data.missing_md5_pct)})"
    )
    if data.missing_md5_albums:
        md5_table = Table(show_header=True, header_style="bold")
        md5_table.add_column("Artist")
        md5_table.add_column("Album")
        md5_table.add_column("Missing", justify="right")
        md5_table.add_column("Total", justify="right")

        for a in data.missing_md5_albums[:50]:
            md5_table.add_row(
                a.albumartist,
                a.album,
                str(a.missing_count),
                str(a.total_count),
            )
        if len(data.missing_md5_albums) > 50:
            console.print(
                f"[dim]  … and {len(data.missing_md5_albums) - 50} more albums "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(md5_table)


def render_technical_summary(console: Console, data: TechnicalSummaryData) -> None:
    """Render technical summary in summary mode."""
    if data.total_files == 0:
        return

    _subsection_heading(console, "🔊 Technical Quality")

    # Sample rate and bit depth as compact tables.
    for title, buckets in [
        ("Sample Rate", data.sample_rate_distribution),
        ("Bit Depth", data.bit_depth_distribution),
        ("Bitrate (kbps)", data.bitrate_distribution),
    ]:
        table = Table(title=title, show_header=True, header_style="bold", expand=False)
        table.add_column("Bucket", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("%", justify="right")
        for b in buckets:
            if b.count > 0:
                table.add_row(b.label, fmt_count(b.count), fmt_pct(b.percentage))
        console.print(table)

    # Headline flags.
    flags: list[str] = []
    if data.duration_outlier_count > 0:
        flags.append(
            f"[yellow]⚠ {data.duration_outlier_count} duration outlier(s)[/yellow]"
        )
    if data.suspicious_vendor_count > 0:
        flags.append(
            f"[red]⚠ {data.suspicious_vendor_count} file(s) with suspicious encoder[/red]"
        )
    if data.missing_md5_count > 0:
        flags.append(
            f"[dim]ℹ {data.missing_md5_count} file(s) missing audio_md5 "
            f"({fmt_pct(data.missing_md5_pct)})[/dim]"
        )
    for f in flags:
        console.print(f"  {f}")


# =========================================================================
# Report 3 — Tag coverage and key inventory
# =========================================================================


def render_tag_coverage(console: Console, data: TagCoverageFullData) -> None:
    """Render the full tag coverage report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Tag Coverage & Key Inventory")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")

    # Section A: Canonical tag coverage — one table per group.
    for group_name, group_data in [
        ("Required Tags", data.required_coverage),
        ("Recommended Tags", data.recommended_coverage),
        ("MusicBrainz Tags", data.musicbrainz_coverage),
        ("Discogs Tags", data.discogs_coverage),
        ("Other Tracked Tags", data.other_coverage),
    ]:
        _subsection_heading(console, f"Section A: {group_name}")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Tag Key", style="cyan")
        table.add_column("Present (non-empty)", justify="right")
        table.add_column("% ", justify="right")
        table.add_column("Present (empty)", justify="right")
        table.add_column("% ", justify="right")
        table.add_column("Absent", justify="right")
        table.add_column("% ", justify="right")

        for row in group_data:
            # Severity colouring.
            absent_style = ""
            if "Required" in group_name and row.absent_count > 0:
                absent_style = "bold red"
            elif "Recommended" in group_name and row.absent_pct > 20.0:
                absent_style = "yellow"

            table.add_row(
                row.tag_key,
                fmt_count(row.present_nonempty_count),
                fmt_pct(row.present_nonempty_pct),
                fmt_count(row.present_empty_count),
                fmt_pct(row.present_empty_pct),
                Text(fmt_count(row.absent_count), style=absent_style),
                Text(fmt_pct(row.absent_pct), style=absent_style),
            )
        console.print(table)

    # Section B: Alias usage.
    if data.alias_usage:
        _subsection_heading(console, "Section B: Alias Usage")
        alias_table = Table(show_header=True, header_style="bold")
        alias_table.add_column("Canonical Key", style="cyan")
        alias_table.add_column("Alias Found")
        alias_table.add_column("Files Using Alias", justify="right")
        alias_table.add_column("%", justify="right")

        for row in data.alias_usage:
            style = "yellow" if row.files_using_alias > 0 else "dim"
            alias_table.add_row(
                row.canonical_key,
                Text(row.alias_key, style=style),
                Text(fmt_count(row.files_using_alias), style=style),
                Text(fmt_pct(row.files_using_alias_pct), style=style),
            )
        console.print(alias_table)

    # Section C: Full tag key inventory.
    _subsection_heading(console, "Section C: Full Tag Key Inventory")
    inv_table = Table(show_header=True, header_style="bold")
    inv_table.add_column("Tag Key", style="cyan")
    inv_table.add_column("File Count", justify="right")
    inv_table.add_column("Coverage %", justify="right")
    inv_table.add_column("Classification")

    for row in data.full_inventory:
        class_style = {
            "required": "bold green",
            "recommended": "green",
            "musicbrainz": "blue",
            "other": "dim",
            "alias": "yellow",
            "standard_optional": "dim",
            "strip": "red",
            "unknown": "bold red",
        }.get(row.classification, "")

        inv_table.add_row(
            row.tag_key_upper,
            fmt_count(row.file_count),
            fmt_pct(row.coverage_pct),
            Text(row.classification, style=class_style),
        )
    console.print(inv_table)

    # Unknown keys — filtered table.
    if data.unknown_keys:
        _subsection_heading(
            console,
            f"Section C: Unknown Keys ({len(data.unknown_keys)} found — stripping candidates)",
        )
        unk_table = Table(show_header=True, header_style="bold red")
        unk_table.add_column("Tag Key", style="red")
        unk_table.add_column("File Count", justify="right")
        unk_table.add_column("Coverage %", justify="right")

        for row in data.unknown_keys:
            note = " ⚠ <0.1%" if row.coverage_pct < 0.1 else ""
            unk_table.add_row(
                row.tag_key_upper,
                fmt_count(row.file_count),
                fmt_pct(row.coverage_pct) + note,
            )
        console.print(unk_table)
    else:
        console.print("[green]No unknown tag keys found.[/green]")


def render_tag_coverage_summary(console: Console, data: TagCoverageSummaryData) -> None:
    """Render tag coverage summary in summary mode."""
    if data.total_files == 0:
        return

    _subsection_heading(console, "🏷  Tag Coverage")

    if data.required_with_missing:
        console.print("[red]  Required tags with missing coverage:[/red]")
        for row in data.required_with_missing:
            console.print(
                f"    [red]• {row.tag_key}[/red]: "
                f"{fmt_count(row.absent_count)} absent ({fmt_pct(row.absent_pct)})"
            )

    if data.recommended_high_missing:
        console.print("[yellow]  Recommended tags with >20% missing:[/yellow]")
        for row in data.recommended_high_missing:
            console.print(
                f"    [yellow]• {row.tag_key}[/yellow]: "
                f"{fmt_pct(row.absent_pct)} absent"
            )

    console.print(f"  Unknown tag keys: [bold]{data.unknown_key_count}[/bold]")
    if data.top_unknown_keys:
        for row in data.top_unknown_keys:
            console.print(
                f"    • {row.tag_key_upper} ({fmt_count(row.file_count)} files)"
            )


# =========================================================================
# Report 5 — Tag format quality
# =========================================================================


def render_tag_formats(console: Console, data: TagFormatsFullData) -> None:
    """Render the full tag format quality report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Tag Format Quality")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")

    for section in data.sections:
        _subsection_heading(console, f"Field: {section.field_name}")

        s = section.summary
        table = Table(show_header=True, header_style="bold")
        table.add_column("Status")
        table.add_column("Count", justify="right")
        table.add_column("%", justify="right")

        table.add_row(
            Text("Valid", style="green"),
            fmt_count(s.valid_count),
            fmt_pct(s.valid_pct),
        )
        inv_style = "red" if s.invalid_count > 0 else ""
        table.add_row(
            Text("Invalid", style=inv_style),
            Text(fmt_count(s.invalid_count), style=inv_style),
            Text(fmt_pct(s.invalid_pct), style=inv_style),
        )
        table.add_row("Absent", fmt_count(s.absent_count), fmt_pct(s.absent_pct))
        console.print(table)

        # Extra distributions (e.g. date precision).
        for extra_name, extra_rows in section.extra.items():
            if extra_rows:
                ext_table = Table(
                    title=extra_name.replace("_", " ").title(),
                    show_header=True,
                    header_style="bold",
                )
                ext_table.add_column("Precision", style="cyan")
                ext_table.add_column("Count", justify="right")
                ext_table.add_column("%", justify="right")
                for er in extra_rows:
                    ext_table.add_row(
                        er.precision,
                        fmt_count(er.count),
                        fmt_pct(er.percentage),
                    )
                console.print(ext_table)

        # Invalid values.
        if section.invalid_values:
            iv_table = Table(
                title=f"Invalid Values (top {len(section.invalid_values)} "
                f"of {section.invalid_values_total})",
                show_header=True,
                header_style="bold",
            )
            iv_table.add_column("Value")
            iv_table.add_column("Count", justify="right")
            for iv in section.invalid_values:
                iv_table.add_row(iv.value, fmt_count(iv.count))
            console.print(iv_table)


def render_tag_formats_summary(console: Console, data: TagFormatsSummaryData) -> None:
    """Render tag formats summary in summary mode."""
    if not data.fields_with_invalid:
        console.print("  [green]All validated fields have correct formats.[/green]")
        return

    _subsection_heading(console, "📝 Tag Format Issues")
    for f in data.fields_with_invalid:
        console.print(
            f"  [red]• {f.field_name}[/red]: "
            f"{fmt_count(f.invalid_count)} invalid ({fmt_pct(f.invalid_pct)})"
        )


# =========================================================================
# Report 6 — Intra-album consistency
# =========================================================================


def render_album_consistency(console: Console, data: AlbumConsistencyFullData) -> None:
    """Render the full album consistency report."""
    if data.total_albums == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Intra-Album Consistency")
    console.print(f"Total albums checked: [bold]{fmt_count(data.total_albums)}[/bold]")
    console.print(
        f"Albums with violations: [bold red]{fmt_count(data.albums_with_violations)}[/bold red]"
    )

    # Table 1: Field consistency violations.
    if data.field_violations:
        _subsection_heading(
            console,
            f"Field Consistency Violations ({len(data.field_violations)} found)",
        )
        fv_table = Table(show_header=True, header_style="bold")
        fv_table.add_column("Album Directory", style="dim", max_width=60)
        fv_table.add_column("Field", style="cyan")
        fv_table.add_column("Distinct Values")
        fv_table.add_column("Null Tracks", justify="right")

        for v in data.field_violations[:100]:
            vals_display = " | ".join(
                f"{val} ({v.track_counts_per_value.get(val, '?')})"
                for val in v.distinct_values[:5]
            )
            if len(v.distinct_values) > 5:
                vals_display += f" … +{len(v.distinct_values) - 5} more"
            fv_table.add_row(
                v.album_dir,
                v.field_name,
                vals_display,
                str(v.null_track_count) if v.null_track_count > 0 else "",
            )
        if len(data.field_violations) > 100:
            console.print(
                f"[dim]  … and {len(data.field_violations) - 100} more "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(fv_table)

    # Table 2: Track numbering violations.
    if data.numbering_violations:
        _subsection_heading(
            console,
            f"Track Numbering Violations ({len(data.numbering_violations)} found)",
        )
        nv_table = Table(show_header=True, header_style="bold")
        nv_table.add_column("Album Directory", style="dim", max_width=60)
        nv_table.add_column("Check Type", style="cyan")
        nv_table.add_column("Description")

        for v in data.numbering_violations[:100]:
            nv_table.add_row(v.album_dir, v.check_type, v.description)
        if len(data.numbering_violations) > 100:
            console.print(
                f"[dim]  … and {len(data.numbering_violations) - 100} more "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(nv_table)

    # Table 3: Summary by violation type.
    if data.summary_by_type:
        _subsection_heading(console, "Summary by Violation Type")
        sv_table = Table(show_header=True, header_style="bold")
        sv_table.add_column("Check Type", style="cyan")
        sv_table.add_column("Albums Affected", justify="right")

        for s in data.summary_by_type:
            sv_table.add_row(s.check_type, fmt_count(s.album_count))
        console.print(sv_table)

    if not data.field_violations and not data.numbering_violations:
        console.print("[green]No consistency violations found.[/green]")


def render_album_consistency_summary(
    console: Console, data: AlbumConsistencySummaryData
) -> None:
    """Render album consistency summary in summary mode."""
    if data.total_albums == 0:
        return

    _subsection_heading(console, "💿 Album Consistency")
    console.print(
        f"  Albums checked: {fmt_count(data.total_albums)}, "
        f"with violations: [{'red' if data.albums_with_violations else 'green'}]"
        f"{fmt_count(data.albums_with_violations)}[/]"
    )
    if data.top_violation_types:
        for v in data.top_violation_types[:5]:
            console.print(f"    • {v.check_type}: {fmt_count(v.album_count)} albums")


# =========================================================================
# Report 7 — External ID coverage and integrity
# =========================================================================


def render_ids(console: Console, data: IdsFullData) -> None:
    """Render the full external IDs report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "External ID Coverage & Integrity")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")

    for section in [data.musicbrainz, data.discogs]:
        _subsection_heading(console, f"{section.source_name} IDs")

        # Coverage table.
        cov_table = Table(
            title=f"{section.source_name} Coverage",
            show_header=True,
            header_style="bold",
        )
        cov_table.add_column("Tag Key", style="cyan")
        cov_table.add_column("Valid", justify="right")
        cov_table.add_column("Valid %", justify="right")
        cov_table.add_column("Malformed", justify="right")
        cov_table.add_column("Malformed %", justify="right")
        cov_table.add_column("Absent", justify="right")
        cov_table.add_column("Absent %", justify="right")

        for row in section.coverage:
            mal_style = "red" if row.malformed_count > 0 else ""
            cov_table.add_row(
                row.tag_key,
                fmt_count(row.valid_count),
                fmt_pct(row.valid_pct),
                Text(fmt_count(row.malformed_count), style=mal_style),
                Text(fmt_pct(row.malformed_pct), style=mal_style),
                fmt_count(row.absent_count),
                fmt_pct(row.absent_pct),
            )
        console.print(cov_table)

        # Partial album coverage.
        if section.partial_albums:
            _subsection_heading(
                console,
                f"{section.source_name} — Partial Album Coverage "
                f"({len(section.partial_albums)} albums)",
            )
            pa_table = Table(show_header=True, header_style="bold")
            pa_table.add_column("Album Directory", style="dim", max_width=60)
            pa_table.add_column("With ID", justify="right")
            pa_table.add_column("Without ID", justify="right")
            pa_table.add_column("Total", justify="right")

            for pa in section.partial_albums[:30]:
                pa_table.add_row(
                    pa.album_dir,
                    str(pa.tracks_with_id),
                    str(pa.tracks_without_id),
                    str(pa.total_tracks),
                )
            if len(section.partial_albums) > 30:
                console.print(
                    f"[dim]  … and {len(section.partial_albums) - 30} more "
                    f"(use --output csv for full list)[/dim]"
                )
            console.print(pa_table)

        # Duplicate IDs.
        if section.duplicate_ids:
            _subsection_heading(
                console,
                f"{section.source_name} — Duplicate IDs "
                f"({len(section.duplicate_ids)} found)",
            )
            dup_table = Table(show_header=True, header_style="bold")
            dup_table.add_column("ID Value", max_width=40)
            dup_table.add_column("Files", justify="right")
            dup_table.add_column("Same Dir?")
            dup_table.add_column("Paths", style="dim")

            for d in section.duplicate_ids[:20]:
                same = (
                    Text("Yes", style="red")
                    if d.same_directory
                    else Text("No", style="yellow")
                )
                paths = "\n".join(d.file_paths[:5])
                if len(d.file_paths) > 5:
                    paths += f"\n… +{len(d.file_paths) - 5} more"
                dup_table.add_row(
                    d.id_value,
                    str(d.file_count),
                    same,
                    paths,
                )
            if len(section.duplicate_ids) > 20:
                console.print(
                    f"[dim]  … and {len(section.duplicate_ids) - 20} more "
                    f"(use --output csv for full list)[/dim]"
                )
            console.print(dup_table)

        # Backfill candidates.
        if section.backfill:
            bf = section.backfill
            console.print(
                f"\n  [green]Quick-win backfill:[/green] "
                f"{fmt_count(bf.affected_tracks)} tracks, "
                f"{fmt_count(bf.distinct_source_ids)} API calls needed"
            )
            console.print(f"  [dim]{bf.description}[/dim]")


def render_ids_summary(console: Console, data: IdsSummaryData) -> None:
    """Render external IDs summary in summary mode."""
    if data.total_files == 0:
        return

    _subsection_heading(console, "🆔 External IDs")
    console.print(
        f"  MusicBrainz: [bold]{fmt_pct(data.mb_overall_coverage_pct)}[/bold] "
        f"fully covered, {fmt_count(data.mb_malformed_count)} malformed, "
        f"{fmt_count(data.mb_partial_album_count)} partial albums"
    )
    if data.mb_backfill_track_count > 0:
        console.print(
            f"    → {fmt_count(data.mb_backfill_track_count)} tracks backfillable"
        )
    console.print(
        f"  Discogs: [bold]{fmt_pct(data.discogs_overall_coverage_pct)}[/bold] "
        f"fully covered, {fmt_count(data.discogs_malformed_count)} malformed, "
        f"{fmt_count(data.discogs_partial_album_count)} partial albums"
    )
    if data.discogs_backfill_track_count > 0:
        console.print(
            f"    → {fmt_count(data.discogs_backfill_track_count)} tracks backfillable"
        )


# =========================================================================
# Report 8 — Duplicates
# =========================================================================


def render_duplicates(console: Console, data: DuplicatesFullData) -> None:
    """Render the full duplicates report."""
    _section_heading(console, "Duplicates")

    # Pass 1: Exact duplicates.
    _subsection_heading(
        console,
        f"Exact Duplicates (SHA-256) — {len(data.exact)} group(s)",
    )
    if data.exact:
        for g in data.exact[:50]:
            console.print(
                f"  [red]Checksum:[/red] {g.checksum[:16]}… ({g.copy_count} copies)"
            )
            for p in g.paths:
                console.print(f"    • {p}")
        if len(data.exact) > 50:
            console.print(f"[dim]  … and {len(data.exact) - 50} more groups[/dim]")
    else:
        console.print("  [green]No exact duplicates found.[/green]")

    # Pass 2: Same recording (MBID).
    _subsection_heading(
        console,
        f"Same Recording Duplicates (MUSICBRAINZ_TRACKID) — {len(data.mbid)} group(s)",
    )
    if data.mbid:
        for g in data.mbid[:30]:
            same_str = (
                "[red]same dir[/red]"
                if g.same_directory
                else "[yellow]different dirs[/yellow]"
            )
            console.print(f"  MBID: {g.mbid[:16]}… ({g.file_count} files, {same_str})")
            for f in g.files:
                console.print(f"    • {f.path}  [{f.album} / {f.date}]")
        if len(data.mbid) > 30:
            console.print(f"[dim]  … and {len(data.mbid) - 30} more groups[/dim]")
    else:
        console.print("  [green]No same-recording duplicates found.[/green]")

    # Pass 3: Probable duplicates.
    _subsection_heading(
        console,
        f"Probable Duplicates (by position) — {len(data.probable)} group(s)",
    )
    if data.probable:
        prob_table = Table(show_header=True, header_style="bold")
        prob_table.add_column("Album Artist")
        prob_table.add_column("Album")
        prob_table.add_column("Disc")
        prob_table.add_column("Track")
        prob_table.add_column("Files", justify="right")
        prob_table.add_column("Paths", style="dim")

        for g in data.probable[:50]:
            paths = "\n".join(g.paths[:5])
            if len(g.paths) > 5:
                paths += f"\n… +{len(g.paths) - 5} more"
            prob_table.add_row(
                g.albumartist,
                g.album,
                g.discnumber,
                g.tracknumber,
                str(g.file_count),
                paths,
            )
        if len(data.probable) > 50:
            console.print(f"[dim]  … and {len(data.probable) - 50} more groups[/dim]")
        console.print(prob_table)
    else:
        console.print("  [green]No probable duplicates found.[/green]")


def render_duplicates_summary(console: Console, data: DuplicatesSummaryData) -> None:
    """Render duplicates summary in summary mode."""
    _subsection_heading(console, "🔁 Duplicates")
    style_exact = "red" if data.exact_count > 0 else "green"
    console.print(
        f"  Exact (SHA-256): [{style_exact}]{data.exact_count} group(s)[/{style_exact}]"
    )
    console.print(f"  Same recording (MBID): {data.mbid_count} group(s)")
    console.print(f"  Probable (by position): {data.probable_count} group(s)")


# =========================================================================
# Report 9 — Scan issues
# =========================================================================


def render_issues(console: Console, data: IssuesFullData) -> None:
    """Render the full scan issues report."""
    _section_heading(console, "Scan Issues")
    console.print(f"Total open issues: [bold]{fmt_count(data.total_open)}[/bold]")

    if data.total_open == 0:
        console.print("[green]No open scan issues.[/green]")
        return

    # Table 1: Pivot (severity × issue_type).
    if data.pivot:
        _subsection_heading(console, "Issues by Severity and Type")
        piv_table = Table(show_header=True, header_style="bold")
        piv_table.add_column("Severity")
        piv_table.add_column("Issue Type", style="cyan")
        piv_table.add_column("Count", justify="right")

        for row in data.pivot:
            piv_table.add_row(
                Text(row.severity.upper(), style=severity_style(row.severity)),
                row.issue_type,
                fmt_count(row.count),
            )
        console.print(piv_table)

    # Table 2: Most-affected albums.
    if data.by_album:
        _subsection_heading(console, "Most-Affected Albums (top 50)")
        album_table = Table(show_header=True, header_style="bold")
        album_table.add_column("Album Directory", style="dim", max_width=60)
        album_table.add_column("Issues", justify="right")

        for row in data.by_album[:50]:
            album_table.add_row(row.album_dir, fmt_count(row.issue_count))
        if len(data.by_album) > 50:
            console.print(
                f"[dim]  … and {len(data.by_album) - 50} more albums "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(album_table)

    # Table 3: Most-affected artists.
    if data.by_artist:
        _subsection_heading(console, "Most-Affected Artists (top 20)")
        art_table = Table(show_header=True, header_style="bold")
        art_table.add_column("Artist")
        art_table.add_column("Issues", justify="right")

        for row in data.by_artist[:20]:
            art_table.add_row(row.artist, fmt_count(row.issue_count))
        if len(data.by_artist) > 20:
            console.print(f"[dim]  … and {len(data.by_artist) - 20} more artists[/dim]")
        console.print(art_table)

    # Table 4: Per-issue-type drill-down.
    for issue_type, detail_rows in sorted(data.by_type.items()):
        _subsection_heading(
            console, f"Issue Type: {issue_type} ({len(detail_rows)} issues)"
        )
        dt_table = Table(show_header=True, header_style="bold")
        dt_table.add_column("Severity")
        dt_table.add_column("File Path", style="dim", max_width=60)
        dt_table.add_column("Description")

        for dr in detail_rows[:30]:
            dt_table.add_row(
                Text(dr.severity.upper(), style=severity_style(dr.severity)),
                dr.file_path or "(collection-wide)",
                dr.description,
            )
        if len(detail_rows) > 30:
            console.print(
                f"[dim]  … and {len(detail_rows) - 30} more "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(dt_table)


def render_issues_summary(console: Console, data: IssuesSummaryData) -> None:
    """Render scan issues summary in summary mode."""
    _subsection_heading(console, "⚠  Scan Issues")

    if data.total_open == 0:
        console.print("  [green]No open scan issues.[/green]")
        return

    console.print(f"  Total open: [bold]{fmt_count(data.total_open)}[/bold]")
    for sev in ("critical", "error", "warning", "info"):
        count = data.by_severity.get(sev, 0)
        if count > 0:
            console.print(
                f"    [{severity_style(sev)}]{sev.upper()}: {fmt_count(count)}[/]"
            )
    if data.top_issue_types:
        console.print("  Top issue types:")
        for t in data.top_issue_types:
            console.print(f"    • {t.issue_type}: {fmt_count(t.count)}")


# =========================================================================
# Report 10 — Artist name consistency
# =========================================================================


def render_artists(console: Console, data: ArtistsFullData) -> None:
    """Render the full artist consistency report."""
    _section_heading(console, "Artist Name Consistency")

    # Table 1: All distinct ALBUMARTIST values.
    if data.albumartist_values:
        _subsection_heading(
            console,
            f"Distinct ALBUMARTIST Values ({len(data.albumartist_values)})",
        )
        aa_table = Table(show_header=True, header_style="bold")
        aa_table.add_column("Value")
        aa_table.add_column("Tracks", justify="right")
        aa_table.add_column("Albums", justify="right")

        for v in data.albumartist_values[:100]:
            aa_table.add_row(
                v.value, fmt_count(v.track_count), fmt_count(v.album_count)
            )
        if len(data.albumartist_values) > 100:
            console.print(
                f"[dim]  Showing top 100 of {len(data.albumartist_values)} "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(aa_table)
    else:
        console.print("[yellow]No ALBUMARTIST values found.[/yellow]")

    # Case variants.
    if data.albumartist_case_variants:
        _subsection_heading(
            console,
            f"ALBUMARTIST Case/Whitespace Variants "
            f"({len(data.albumartist_case_variants)} groups) — [red]ERROR[/red]",
        )
        for g in data.albumartist_case_variants:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            console.print(
                f"  [red]•[/red] {variants_str} "
                f"(total: {fmt_count(g.total_track_count)} tracks)"
            )

    # Table 2: ALBUMARTIST fuzzy pairs.
    if data.albumartist_fuzzy_pairs:
        _subsection_heading(
            console,
            f"ALBUMARTIST Fuzzy Similarity Pairs "
            f"({len(data.albumartist_fuzzy_pairs)} found)",
        )
        fp_table = Table(show_header=True, header_style="bold")
        fp_table.add_column("Name A")
        fp_table.add_column("Name B")
        fp_table.add_column("Similarity", justify="right")
        fp_table.add_column("Tracks A", justify="right")
        fp_table.add_column("Tracks B", justify="right")

        for p in data.albumartist_fuzzy_pairs:
            fp_table.add_row(
                p.name_a,
                p.name_b,
                f"{p.similarity:.2%}",
                fmt_count(p.count_a),
                fmt_count(p.count_b),
            )
        console.print(fp_table)

    # Table 3: All distinct ARTIST values (top 100).
    if data.artist_values:
        _subsection_heading(
            console,
            f"Distinct ARTIST Values ({len(data.artist_values)} total — top 100)",
        )
        a_table = Table(show_header=True, header_style="bold")
        a_table.add_column("Value")
        a_table.add_column("Tracks", justify="right")
        a_table.add_column("Albums", justify="right")

        for v in data.artist_values[:100]:
            a_table.add_row(v.value, fmt_count(v.track_count), fmt_count(v.album_count))
        if len(data.artist_values) > 100:
            console.print(
                f"[dim]  Showing top 100 of {len(data.artist_values)} "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(a_table)

    # Case variants for ARTIST.
    if data.artist_case_variants:
        _subsection_heading(
            console,
            f"ARTIST Case/Whitespace Variants "
            f"({len(data.artist_case_variants)} groups) — [red]ERROR[/red]",
        )
        for g in data.artist_case_variants[:50]:
            variants_str = ", ".join(f'"{v}"' for v in g.variants)
            console.print(
                f"  [red]•[/red] {variants_str} "
                f"(total: {fmt_count(g.total_track_count)} tracks)"
            )
        if len(data.artist_case_variants) > 50:
            console.print(
                f"[dim]  … and {len(data.artist_case_variants) - 50} more groups[/dim]"
            )

    # Table 4: ARTIST fuzzy pairs.
    if data.artist_fuzzy_pairs:
        _subsection_heading(
            console,
            f"ARTIST Fuzzy Similarity Pairs ({len(data.artist_fuzzy_pairs)} found)",
        )
        afp_table = Table(show_header=True, header_style="bold")
        afp_table.add_column("Name A")
        afp_table.add_column("Name B")
        afp_table.add_column("Similarity", justify="right")
        afp_table.add_column("Tracks A", justify="right")
        afp_table.add_column("Tracks B", justify="right")

        for p in data.artist_fuzzy_pairs:
            afp_table.add_row(
                p.name_a,
                p.name_b,
                f"{p.similarity:.2%}",
                fmt_count(p.count_a),
                fmt_count(p.count_b),
            )
        console.print(afp_table)


def render_artists_summary(console: Console, data: ArtistsSummaryData) -> None:
    """Render artist consistency summary in summary mode."""
    _subsection_heading(console, "🎤 Artist Consistency")
    console.print(
        f"  Distinct ALBUMARTIST values: {fmt_count(data.distinct_albumartist_count)}"
    )
    if data.albumartist_case_variant_count > 0:
        console.print(
            f"  [red]Case/whitespace variant groups: "
            f"{data.albumartist_case_variant_count}[/red]"
        )
    console.print(f"  Distinct ARTIST values: {fmt_count(data.distinct_artist_count)}")


# =========================================================================
# Report 11 — Genre analysis
# =========================================================================


def render_genres(console: Console, data: GenresFullData) -> None:
    """Render the full genre analysis report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Genre Analysis")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")
    console.print(f"Distinct genre values: [bold]{len(data.genre_values)}[/bold]")

    # Table 1: Genre values (top 30 with bar chart).
    if data.genre_values:
        _subsection_heading(console, "Genre Values (top 30)")
        gv_table = Table(show_header=True, header_style="bold")
        gv_table.add_column("Genre", style="cyan")
        gv_table.add_column("Files", justify="right")
        gv_table.add_column("%", justify="right")
        gv_table.add_column("Distribution", no_wrap=True)

        max_count = data.genre_values[0].file_count if data.genre_values else 1
        for gv in data.genre_values[:30]:
            gv_table.add_row(
                gv.value,
                fmt_count(gv.file_count),
                fmt_pct(gv.file_pct),
                bar_chart(gv.file_count, max_count, width=25),
            )
        if len(data.genre_values) > 30:
            console.print(
                f"[dim]  Showing top 30 of {len(data.genre_values)} "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(gv_table)

    # Table 2: Genre fuzzy similarity pairs.
    if data.fuzzy_pairs:
        _subsection_heading(
            console, f"Genre Fuzzy Similarity Pairs ({len(data.fuzzy_pairs)} found)"
        )
        fp_table = Table(show_header=True, header_style="bold")
        fp_table.add_column("Genre A")
        fp_table.add_column("Genre B")
        fp_table.add_column("Similarity", justify="right")
        fp_table.add_column("Files A", justify="right")
        fp_table.add_column("Files B", justify="right")

        for p in data.fuzzy_pairs:
            fp_table.add_row(
                p.name_a,
                p.name_b,
                f"{p.similarity:.2%}",
                fmt_count(p.count_a),
                fmt_count(p.count_b),
            )
        console.print(fp_table)

    # Table 3: Files with no GENRE tag.
    console.print(
        f"\nFiles with no genre: [bold]{fmt_count(data.no_genre_count)}[/bold] "
        f"({fmt_pct(data.no_genre_pct)})"
    )
    if data.no_genre_by_artist:
        _subsection_heading(console, "Missing Genre by Artist (top 20)")
        mga_table = Table(show_header=True, header_style="bold")
        mga_table.add_column("Artist")
        mga_table.add_column("Missing Files", justify="right")

        for a in data.no_genre_by_artist[:20]:
            mga_table.add_row(a.artist, fmt_count(a.missing_count))
        if len(data.no_genre_by_artist) > 20:
            console.print(
                f"[dim]  … and {len(data.no_genre_by_artist) - 20} more artists[/dim]"
            )
        console.print(mga_table)

    # Table 4: Multi-genre files.
    if data.multi_genre_count > 0:
        _subsection_heading(
            console,
            f"Files with Multiple GENRE Tags ({fmt_count(data.multi_genre_count)} files)",
        )
        if data.multi_genre_combos:
            mg_table = Table(show_header=True, header_style="bold")
            mg_table.add_column("Genre Combination")
            mg_table.add_column("Files", justify="right")

            for combo in data.multi_genre_combos[:20]:
                mg_table.add_row(
                    " + ".join(combo.values),
                    fmt_count(combo.file_count),
                )
            if len(data.multi_genre_combos) > 20:
                console.print(
                    f"[dim]  … and {len(data.multi_genre_combos) - 20} more combos[/dim]"
                )
            console.print(mg_table)


def render_genres_summary(console: Console, data: GenresSummaryData) -> None:
    """Render genres summary in summary mode."""
    _subsection_heading(console, "🎵 Genres")
    console.print(
        f"  Distinct genres: {fmt_count(data.distinct_genre_count)}, "
        f"fuzzy pairs: {data.fuzzy_pair_count}, "
        f"no genre: {fmt_pct(data.no_genre_pct)}"
    )


# =========================================================================
# Report 12 — Lyrics coverage
# =========================================================================


def render_lyrics(console: Console, data: LyricsFullData) -> None:
    """Render the full lyrics coverage report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "Lyrics Coverage")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")

    # Table 1: Overall coverage.
    o = data.overall
    ov_table = Table(
        title="Overall Lyrics Coverage", show_header=True, header_style="bold"
    )
    ov_table.add_column("Category")
    ov_table.add_column("Count", justify="right")
    ov_table.add_column("%", justify="right")
    ov_table.add_row(
        Text("With lyrics", style="green"),
        fmt_count(o.with_lyrics_count),
        fmt_pct(o.with_lyrics_pct),
    )
    ov_table.add_row(
        "Empty lyrics tag",
        fmt_count(o.empty_lyrics_count),
        fmt_pct(o.empty_lyrics_pct),
    )
    ov_table.add_row(
        "No lyrics tag",
        fmt_count(o.no_lyrics_count),
        fmt_pct(o.no_lyrics_pct),
    )
    console.print(ov_table)

    # Key variant note.
    console.print(
        f"\n  Files using [cyan]LYRICS[/cyan] key: {fmt_count(o.lyrics_key_count)}"
    )
    console.print(
        f"  Files using [cyan]UNSYNCEDLYRICS[/cyan] key: "
        f"{fmt_count(o.unsyncedlyrics_key_count)}"
    )

    # Table 2: Coverage by artist (worst 20).
    if data.by_artist:
        _subsection_heading(console, "Coverage by Artist (worst 20)")
        ba_table = Table(show_header=True, header_style="bold")
        ba_table.add_column("Artist")
        ba_table.add_column("With Lyrics", justify="right")
        ba_table.add_column("Total", justify="right")
        ba_table.add_column("%", justify="right")

        for a in data.by_artist[:20]:
            style = "red" if a.coverage_pct == 0 else ""
            ba_table.add_row(
                a.name,
                fmt_count(a.with_lyrics),
                fmt_count(a.total),
                Text(fmt_pct(a.coverage_pct), style=style),
            )
        if len(data.by_artist) > 20:
            console.print(
                f"[dim]  … and {len(data.by_artist) - 20} more artists "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(ba_table)

    # Table 3: Coverage by album (worst 30).
    if data.by_album:
        _subsection_heading(console, "Coverage by Album (worst 30)")
        alb_table = Table(show_header=True, header_style="bold")
        alb_table.add_column("Album Directory", style="dim", max_width=60)
        alb_table.add_column("With Lyrics", justify="right")
        alb_table.add_column("Total", justify="right")
        alb_table.add_column("%", justify="right")

        for a in data.by_album[:30]:
            style = "red" if a.coverage_pct == 0 else ""
            alb_table.add_row(
                a.name,
                fmt_count(a.with_lyrics),
                fmt_count(a.total),
                Text(fmt_pct(a.coverage_pct), style=style),
            )
        if len(data.by_album) > 30:
            console.print(
                f"[dim]  … and {len(data.by_album) - 30} more albums "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(alb_table)


def render_lyrics_summary(console: Console, data: LyricsSummaryData) -> None:
    """Render lyrics summary in summary mode."""
    _subsection_heading(console, "📝 Lyrics")
    console.print(
        f"  Coverage: [bold]{fmt_pct(data.coverage_pct)}[/bold], "
        f"artists with zero: {data.artists_with_zero_coverage}"
    )


# =========================================================================
# Report 13 — ReplayGain coverage
# =========================================================================


def render_replaygain(console: Console, data: ReplayGainFullData) -> None:
    """Render the full ReplayGain coverage report."""
    if data.total_files == 0:
        _empty_db_message(console)
        return

    _section_heading(console, "ReplayGain Coverage")
    console.print(f"Total files: [bold]{fmt_count(data.total_files)}[/bold]")

    # Table 1: Coverage for all four tags.
    cov_table = Table(
        title="ReplayGain Tag Coverage",
        show_header=True,
        header_style="bold",
    )
    cov_table.add_column("Tag Key", style="cyan")
    cov_table.add_column("Valid", justify="right")
    cov_table.add_column("Valid %", justify="right")
    cov_table.add_column("Malformed", justify="right")
    cov_table.add_column("Malformed %", justify="right")
    cov_table.add_column("Absent", justify="right")
    cov_table.add_column("Absent %", justify="right")

    for row in data.coverage:
        mal_style = "red" if row.malformed_count > 0 else ""
        cov_table.add_row(
            row.tag_key,
            fmt_count(row.valid_count),
            fmt_pct(row.valid_pct),
            Text(fmt_count(row.malformed_count), style=mal_style),
            Text(fmt_pct(row.malformed_pct), style=mal_style),
            fmt_count(row.absent_count),
            fmt_pct(row.absent_pct),
        )
    console.print(cov_table)

    # Table 2: Partially-tagged albums.
    if data.partial_albums:
        _subsection_heading(
            console,
            f"Partially-Tagged Albums ({len(data.partial_albums)} albums)",
        )
        pa_table = Table(show_header=True, header_style="bold")
        pa_table.add_column("Album Directory", style="dim", max_width=60)
        pa_table.add_column("With RG", justify="right")
        pa_table.add_column("Without RG", justify="right")
        pa_table.add_column("Total", justify="right")

        for pa in data.partial_albums[:30]:
            pa_table.add_row(
                pa.album_dir,
                str(pa.tracks_with_rg),
                str(pa.tracks_without_rg),
                str(pa.total_tracks),
            )
        if len(data.partial_albums) > 30:
            console.print(
                f"[dim]  … and {len(data.partial_albums) - 30} more albums "
                f"(use --output csv for full list)[/dim]"
            )
        console.print(pa_table)
    else:
        console.print("[green]No partially-tagged albums found.[/green]")

    # Table 3: Track gain distribution.
    if data.gain_distribution:
        console.print(
            _distribution_table(
                "Track Gain Distribution (REPLAYGAIN_TRACK_GAIN)",
                data.gain_distribution,
            )
        )

    # Table 4: Outliers.
    if data.outliers:
        _subsection_heading(
            console,
            f"Gain Outliers (outside [-20, +10] dB) — {len(data.outliers)} found",
        )
        out_table = Table(show_header=True, header_style="bold")
        out_table.add_column("Path", style="dim", max_width=60)
        out_table.add_column("Tag Key", style="cyan")
        out_table.add_column("Value")
        out_table.add_column("Parsed (dB)", justify="right")

        for o in data.outliers:
            out_table.add_row(
                o.path,
                o.tag_key,
                o.value,
                f"{o.parsed_db:.2f}",
            )
        console.print(out_table)
    else:
        console.print("[green]No gain outliers found.[/green]")


def render_replaygain_summary(console: Console, data: ReplayGainSummaryData) -> None:
    """Render ReplayGain summary in summary mode."""
    _subsection_heading(console, "🔉 ReplayGain")
    console.print(
        f"  Track coverage: [bold]{fmt_pct(data.track_coverage_pct)}[/bold], "
        f"Album coverage: [bold]{fmt_pct(data.album_coverage_pct)}[/bold]"
    )
    flags: list[str] = []
    if data.partial_album_count > 0:
        flags.append(
            f"[yellow]{data.partial_album_count} partially-tagged album(s)[/yellow]"
        )
    if data.outlier_count > 0:
        flags.append(f"[red]{data.outlier_count} outlier(s)[/red]")
    for f in flags:
        console.print(f"    {f}")


# =========================================================================
# Master summary — the combined `report summary` output
# =========================================================================


def render_master_summary(console: Console, summary: MasterSummary) -> None:
    """Render the master summary: a fast health-check dashboard combining
    headline metrics from all reports."""
    console.print()
    console.print(
        Panel(
            f"[bold]storeroon — Collection Health Summary[/bold]\n"
            f"[dim]Generated {now_iso()}[/dim]",
            expand=False,
        )
    )

    if summary.overview:
        render_overview_summary(console, summary.overview)
    if summary.technical:
        render_technical_summary(console, summary.technical)
    if summary.tags:
        render_tag_coverage_summary(console, summary.tags)
    if summary.tag_formats:
        render_tag_formats_summary(console, summary.tag_formats)
    if summary.album_consistency:
        render_album_consistency_summary(console, summary.album_consistency)
    if summary.ids:
        render_ids_summary(console, summary.ids)
    if summary.duplicates:
        render_duplicates_summary(console, summary.duplicates)
    if summary.issues:
        render_issues_summary(console, summary.issues)
    if summary.artists:
        render_artists_summary(console, summary.artists)
    if summary.genres:
        render_genres_summary(console, summary.genres)
    if summary.lyrics:
        render_lyrics_summary(console, summary.lyrics)
    if summary.replaygain:
        render_replaygain_summary(console, summary.replaygain)

    console.print()
