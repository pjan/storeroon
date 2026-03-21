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
    ArtistsFullData,
    ArtistsSummaryData,
    BucketCount,
    GenresFullData,
    GenresSummaryData,
    LyricsFullData,
    LyricsSummaryData,
    MasterSummary,
    Overview2FullData,
    OverviewSummaryData,
    ReplayGainFullData,
    ReplayGainSummaryData,
    TagCoverageFullData,
    TagCoverageSummaryData,
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


def render_overview(console: Console, data: Overview2FullData) -> None:
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
    totals_table.add_row("Tracks", fmt_count(t.total_tracks))
    totals_table.add_row("Duration", fmt_duration_hms(t.total_duration_seconds))
    totals_table.add_row("Size", fmt_size_gb(t.total_size_bytes))
    console.print(totals_table)

    # Hierarchical breakdown as an indented table.
    if data.by_artist:
        _subsection_heading(console, "Collection Breakdown")
        tbl = Table(show_header=True, header_style="bold", pad_edge=False)
        tbl.add_column("Name", min_width=50)
        tbl.add_column("Albums", justify="right")
        tbl.add_column("Tracks", justify="right")
        tbl.add_column("Size", justify="right")
        tbl.add_column("Duration", justify="right")

        for a in data.by_artist:
            tbl.add_row(
                f"[cyan bold]{a.artist}[/cyan bold]",
                fmt_count(a.album_count),
                fmt_count(a.track_count),
                fmt_size_gb(a.total_size_bytes),
                fmt_duration_hms(a.total_duration_seconds),
            )
            for rt in a.release_types:
                tbl.add_row(
                    f"[green]{_indent(rt.release_type, 1)}[/green]",
                    fmt_count(rt.album_count),
                    fmt_count(rt.track_count),
                    fmt_size_gb(rt.total_size_bytes),
                    fmt_duration_hms(rt.total_duration_seconds),
                )
                for alb in rt.albums:
                    tbl.add_row(
                        _indent(alb.display_name, 2),
                        "",
                        fmt_count(alb.track_count),
                        fmt_size_gb(alb.total_size_bytes),
                        fmt_duration_hms(alb.total_duration_seconds),
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

    # Coverage tables — one per group.
    for group_name, group_data, severity in [
        ("Required Tags", data.required_coverage, "required"),
        ("Recommended Tags", data.recommended_coverage, "recommended"),
        ("Other Tracked Tags", data.other_coverage, ""),
    ]:
        _subsection_heading(console, group_name)
        table = Table(show_header=True, header_style="bold")
        table.add_column("Tag Key", style="cyan")
        table.add_column("", min_width=20)
        table.add_column("Coverage", justify="right")
        table.add_column("Present", justify="right")

        for row in group_data:
            bar_color = "green"
            if severity == "required" and row.missing_count > 0:
                bar_color = "red"
            elif severity == "recommended" and row.missing_pct > 20.0:
                bar_color = "yellow"

            chart = bar_chart(row.present_pct, 100.0, width=20)
            table.add_row(
                row.tag_key,
                f"[{bar_color}]{chart}[/{bar_color}]",
                fmt_pct(row.present_pct),
                fmt_count(row.present_count),
            )
        console.print(table)

    # Alias consistency.
    if data.alias_usage:
        _subsection_heading(console, "Alias Consistency")
        alias_table = Table(show_header=True, header_style="bold")
        alias_table.add_column("Canonical Key", style="cyan")
        alias_table.add_column("Alias Key")
        alias_table.add_column("", min_width=20)
        alias_table.add_column("Consistency", justify="right")

        for row in data.alias_usage:
            bar_color = "green" if row.consistency_pct >= 100.0 else "red"
            chart = bar_chart(row.consistency_pct, 100.0, width=20)
            alias_table.add_row(
                row.canonical_key,
                row.alias_key,
                f"[{bar_color}]{chart}[/{bar_color}]",
                fmt_pct(row.consistency_pct),
            )
        console.print(alias_table)

    # Tags to strip (unknown keys).
    if data.unknown_keys:
        _subsection_heading(
            console,
            f"Tags to Strip ({len(data.unknown_keys)})",
        )
        unk_table = Table(show_header=True, header_style="bold")
        unk_table.add_column("Tag Key", style="red")
        unk_table.add_column("", min_width=20)
        unk_table.add_column("Coverage", justify="right")
        unk_table.add_column("Present", justify="right")

        for row in data.unknown_keys:
            chart = bar_chart(row.coverage_pct, 100.0, width=20)
            unk_table.add_row(
                row.tag_key_upper,
                f"[red]{chart}[/red]",
                fmt_pct(row.coverage_pct),
                fmt_count(row.file_count),
            )
        console.print(unk_table)

    # Full tag key inventory.
    _subsection_heading(
        console,
        f"Full Tag Key Inventory ({len(data.full_inventory)} tags)",
    )
    inv_table = Table(show_header=True, header_style="bold")
    inv_table.add_column("Classification")
    inv_table.add_column("Tag Key", style="cyan")
    inv_table.add_column("", min_width=20)
    inv_table.add_column("Coverage", justify="right")
    inv_table.add_column("Present", justify="right")

    for row in data.full_inventory:
        class_style = {
            "required": "bold green",
            "recommended": "green",
            "other": "dim",
            "alias": "yellow",
            "standard_optional": "dim",
            "strip": "red",
            "unknown": "bold red",
        }.get(row.classification, "")

        chart = bar_chart(row.coverage_pct, 100.0, width=20)
        inv_table.add_row(
            Text(row.classification, style=class_style),
            row.tag_key_upper,
            f"[green]{chart}[/green]",
            fmt_pct(row.coverage_pct),
            fmt_count(row.file_count),
        )
    console.print(inv_table)


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
                f"{fmt_count(row.missing_count)} missing ({fmt_pct(row.missing_pct)})"
            )

    if data.recommended_high_missing:
        console.print("[yellow]  Recommended tags with >20% missing:[/yellow]")
        for row in data.recommended_high_missing:
            console.print(
                f"    [yellow]• {row.tag_key}[/yellow]: "
                f"{fmt_pct(row.missing_pct)} missing"
            )

    console.print(f"  Unknown tag keys: [bold]{data.unknown_key_count}[/bold]")
    if data.top_unknown_keys:
        for row in data.top_unknown_keys:
            console.print(
                f"    • {row.tag_key_upper} ({fmt_count(row.file_count)} files)"
            )


# =========================================================================
# Report 9 — Scan issues
# =========================================================================



def render_album_issues(console: Console, data) -> None:
    """Render detailed issues for a specific album."""
    from storeroon.reports.models import AlbumIssuesDetail

    data: AlbumIssuesDetail

    _section_heading(console, "Album Issue Details")

    # Album info header
    console.print(f"[bold cyan]Artist:[/bold cyan] {data.artist}")
    console.print(f"[bold cyan]Album:[/bold cyan] {data.album}")
    if data.catalog_number:
        console.print(f"[bold cyan]Catalog Number:[/bold cyan] {data.catalog_number}")
    console.print(f"[bold cyan]Directory:[/bold cyan] [dim]{data.album_dir}[/dim]")
    console.print()

    # Summary stats
    console.print(f"Total files in album: [bold]{fmt_count(data.total_files)}[/bold]")
    console.print(
        f"Files with issues: [bold]{fmt_count(data.files_with_issues)}[/bold]"
    )
    console.print(
        f"Total issues: [bold]{fmt_count(data.error_count + data.warning_count + data.info_count)}[/bold]"
    )
    console.print()

    # Issue count by severity
    if data.error_count > 0:
        console.print(f"  [red]ERRORS:[/red] {fmt_count(data.error_count)}")
    if data.warning_count > 0:
        console.print(f"  [yellow]WARNINGS:[/yellow] {fmt_count(data.warning_count)}")
    if data.info_count > 0:
        console.print(f"  [blue]INFO:[/blue] {fmt_count(data.info_count)}")
    console.print()

    # Issues table
    _subsection_heading(console, "Issues by File")
    issue_table = Table(show_header=True, header_style="bold")
    issue_table.add_column("Severity", width=8)
    issue_table.add_column("File", style="dim", max_width=40)
    issue_table.add_column("Issue Type", style="cyan", max_width=25)
    issue_table.add_column("Description", max_width=50)

    for issue in data.issues:
        issue_table.add_row(
            Text(issue.severity.upper(), style=severity_style(issue.severity)),
            issue.file_name,
            issue.issue_type,
            issue.description,
        )

    console.print(issue_table)


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
    if summary.album_consistency:
    if summary.duplicates:
    if summary.artists:
        render_artists_summary(console, summary.artists)
    if summary.genres:
        render_genres_summary(console, summary.genres)
    if summary.lyrics:
        render_lyrics_summary(console, summary.lyrics)
    if summary.replaygain:
        render_replaygain_summary(console, summary.replaygain)

    console.print()
