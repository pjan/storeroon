"""
storeroon.reports.cli — CLI layer for reports.

Wires the query and renderer layers together. Handles ``--output``,
``--output-dir``, ``--artist``, ``--album``, ``--min-severity`` flags.
Handles empty database gracefully (print a clear message, exit 0).
Handles ``--artist`` / ``--album`` filter producing no results gracefully.

CLI structure::

    python -m storeroon report summary
    python -m storeroon report overview      [--output terminal|json] [--output-dir PATH]
    python -m storeroon report technical     [--output ...] [--output-dir PATH]
    python -m storeroon report tag-formats   [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report album-consistency [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report ids           [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report issues        [--output ...] [--output-dir PATH] [--min-severity ...]
    python -m storeroon report album-issues  ALBUM_DIR [--output ...] [--output-dir PATH]
    python -m storeroon report artists       [--output ...] [--output-dir PATH]
    python -m storeroon report genres        [--output ...] [--output-dir PATH]
    python -m storeroon report lyrics        [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report replaygain    [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report all           [--output-dir PATH]
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from collections.abc import Callable
from pathlib import Path

from rich.console import Console

from storeroon import config as cfg
from storeroon.db import connect
from storeroon.reports.models import MasterSummary
from storeroon.reports.utils import build_filter_string

log = logging.getLogger("storeroon.reports")

console = Console(stderr=True)
output_console = Console()  # stdout — for data output


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_config(args: argparse.Namespace) -> cfg.Config | None:
    """Load configuration, printing errors to console on failure."""
    try:
        return cfg.load(getattr(args, "config", None))
    except cfg.ConfigError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        return None


def _open_db(conf: cfg.Config) -> sqlite3.Connection | None:
    """Open the database in read-only mode. Returns None on failure."""
    db_path = conf.database.path.expanduser()
    if not db_path.is_file():
        console.print(
            "[yellow]Database file not found. "
            "Run [bold]storeroon scan[/bold] first to import your collection.[/yellow]"
        )
        return None
    try:
        conn = connect(db_path, read_only=True)
        return conn
    except Exception as exc:
        console.print(f"[bold red]Cannot open database:[/bold red] {exc}")
        return None


def _check_empty(conn: sqlite3.Connection) -> bool:
    """Return True if the database has no imported files."""
    row = conn.execute("SELECT COUNT(*) FROM files").fetchone()
    return row is None or row[0] == 0


def _resolve_output_dir(args: argparse.Namespace, conf: cfg.Config) -> Path:
    """Resolve the output directory from CLI args or config."""
    if hasattr(args, "output_dir") and args.output_dir:
        return Path(args.output_dir).expanduser().resolve()
    return conf.reports.output_dir.expanduser().resolve()


def _get_output_format(args: argparse.Namespace) -> str:
    """Get the output format from CLI args, defaulting to 'terminal'."""
    return getattr(args, "output", "terminal") or "terminal"


def _get_artist_filter(args: argparse.Namespace) -> str | None:
    """Get the --artist filter value, or None."""
    return getattr(args, "artist", None)


def _get_album_filter(args: argparse.Namespace) -> str | None:
    """Get the --album filter value, or None."""
    return getattr(args, "album", None)


def _get_min_severity(args: argparse.Namespace) -> str:
    """Get the --min-severity filter value, defaulting to 'info'."""
    return getattr(args, "min_severity", "info") or "info"


def _print_written_files(written: list[Path]) -> None:
    """Print a summary of files that were written."""
    if written:
        console.print(f"\n[green]Wrote {len(written)} file(s):[/green]")
        for p in written:
            console.print(f"  {p}")


def _check_artist_has_results(
    conn: sqlite3.Connection,
    artist_filter: str,
) -> bool:
    """Check if the artist filter matches any files."""
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT f.id)
        FROM files f
        JOIN raw_tags rt ON rt.file_id = f.id
        WHERE f.status = 'ok'
          AND rt.tag_key_upper = 'ALBUMARTIST'
          AND LOWER(rt.tag_value) LIKE '%' || LOWER(?) || '%'
        """,
        (artist_filter,),
    ).fetchone()
    return row is not None and row[0] > 0


def _write_json(
    args: argparse.Namespace,
    conf: cfg.Config,
    report_name: str,
    data: object,
    filters: dict[str, str | None] | None = None,
) -> None:
    """Write a JSON report file and print the result."""
    from storeroon.reports.renderers.json_renderer import write_report

    output_dir = _resolve_output_dir(args, conf)
    written = write_report(output_dir, report_name, data, filters=filters)
    _print_written_files([written])


# ---------------------------------------------------------------------------
# Report dispatch functions
# ---------------------------------------------------------------------------


def _cmd_summary(args: argparse.Namespace) -> int:
    """Execute ``report summary`` — terminal-only fast health check."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — no files have been imported yet. "
            "Run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import (
        artists,
        genres,
        lyrics,
        overview,
        replaygain,
        technical,
    )
    from storeroon.reports.renderers.terminal import render_master_summary

    summary = MasterSummary()

    console.print("[dim]Gathering summary data…[/dim]")

    try:
        summary.overview = overview.summary_data(conn)
    except Exception as exc:
        log.warning("Overview summary failed: %s", exc)

    try:
        summary.technical = technical.summary_data(conn)
    except Exception as exc:
        log.warning("Technical summary failed: %s", exc)

    try:
        summary.artists = artists.summary_data(
            conn, fuzzy_threshold=conf.reports.fuzzy_threshold
        )
    except Exception as exc:
        log.warning("Artists summary failed: %s", exc)

    try:
        summary.genres = genres.summary_data(
            conn, fuzzy_threshold=conf.reports.fuzzy_threshold
        )
    except Exception as exc:
        log.warning("Genres summary failed: %s", exc)

    try:
        summary.lyrics = lyrics.summary_data(conn)
    except Exception as exc:
        log.warning("Lyrics summary failed: %s", exc)

    try:
        summary.replaygain = replaygain.summary_data(conn)
    except Exception as exc:
        log.warning("ReplayGain summary failed: %s", exc)

    render_master_summary(output_console, summary)

    conn.close()
    return 0


def _cmd_overview(args: argparse.Namespace) -> int:
    """Execute ``report overview``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import overview

    _aliases = conf.tags.aliases
    _canonical = frozenset(conf.tags.required + conf.tags.recommended)
    data = overview.full_data(conn, aliases=_aliases, canonical_keys=_canonical)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        from storeroon.reports.renderers.terminal import render_overview
        render_overview(output_console, data)
    else:
        _write_json(args, conf, "overview", data)

    conn.close()
    return 0


def _cmd_collection_issues(args: argparse.Namespace) -> int:
    """Execute ``report collection-issues``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import collection_issues

    data = collection_issues.full_data(conn, conf.tags)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        output_console.print("[yellow]Collection issues overview is HTML-only. Use --output json and storeroon serve.[/yellow]")
    else:
        _write_json(args, conf, "collection_issues", data)

    conn.close()
    return 0


def _cmd_technical(args: argparse.Namespace) -> int:
    """Execute ``report technical``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import technical
    from storeroon.reports.renderers.terminal import render_technical

    data = technical.full_data(conn)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_technical(output_console, data)
    else:
        _write_json(args, conf, "technical", data)

    conn.close()
    return 0


def _cmd_key_inventory(args: argparse.Namespace) -> int:
    """Execute ``report key-inventory``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import key_inventory

    data = key_inventory.full_data(conn, conf.tags)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        output_console.print("[yellow]Key inventory is HTML-only. Use --output json and storeroon serve.[/yellow]")
    else:
        _write_json(args, conf, "key_inventory", data)

    conn.close()
    return 0


def _cmd_album_issues(args: argparse.Namespace) -> int:
    """Execute ``report album-issues``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    album_dir = args.album_dir
    if not album_dir:
        output_console.print("[red]Error: --album-dir is required[/red]")
        conn.close()
        return 1

    from storeroon.reports.queries import issues
    from storeroon.reports.renderers.terminal import render_album_issues

    data = issues.album_detail(conn, album_dir)

    if data is None:
        output_console.print(f"[yellow]No issues found for album: {album_dir}[/yellow]")
        conn.close()
        return 0

    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_album_issues(output_console, data)
    else:
        filters: dict[str, str | None] = {"album_dir": album_dir}
        _write_json(args, conf, "album_issues", data, filters=filters)

    conn.close()
    return 0


def _cmd_artists(args: argparse.Namespace) -> int:
    """Execute ``report artists``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import artists
    from storeroon.reports.renderers.terminal import render_artists

    threshold = conf.reports.fuzzy_threshold
    data = artists.full_data(conn, fuzzy_threshold=threshold)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_artists(output_console, data)
    else:
        _write_json(args, conf, "artists", data)

    conn.close()
    return 0


def _cmd_genres(args: argparse.Namespace) -> int:
    """Execute ``report genres``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    from storeroon.reports.queries import genres
    from storeroon.reports.renderers.terminal import render_genres

    threshold = conf.reports.fuzzy_threshold
    data = genres.full_data(conn, fuzzy_threshold=threshold)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_genres(output_console, data)
    else:
        _write_json(args, conf, "genres", data)

    conn.close()
    return 0


def _cmd_lyrics(args: argparse.Namespace) -> int:
    """Execute ``report lyrics``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    artist_filter = _get_artist_filter(args)
    if artist_filter and not _check_artist_has_results(conn, artist_filter):
        output_console.print("[yellow]No tracks matched the given filters.[/yellow]")
        conn.close()
        return 0

    from storeroon.reports.queries import lyrics
    from storeroon.reports.renderers.terminal import render_lyrics

    data = lyrics.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_lyrics(output_console, data)
    else:
        filters = {"artist": artist_filter}
        _write_json(args, conf, "lyrics", data, filters=filters)

    conn.close()
    return 0


def _cmd_replaygain(args: argparse.Namespace) -> int:
    """Execute ``report replaygain``."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    artist_filter = _get_artist_filter(args)
    if artist_filter and not _check_artist_has_results(conn, artist_filter):
        output_console.print("[yellow]No tracks matched the given filters.[/yellow]")
        conn.close()
        return 0

    from storeroon.reports.queries import replaygain
    from storeroon.reports.renderers.terminal import render_replaygain

    data = replaygain.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        render_replaygain(output_console, data)
    else:
        filters = {"artist": artist_filter}
        _write_json(args, conf, "replaygain", data, filters=filters)

    conn.close()
    return 0


def _cmd_all(args: argparse.Namespace) -> int:
    """Execute ``report all`` — generate all 12 reports as JSON."""
    conf = _load_config(args)
    if conf is None:
        return 1

    conn = _open_db(conf)
    if conn is None:
        return 0

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0

    output_dir = _resolve_output_dir(args, conf)
    threshold = conf.reports.fuzzy_threshold
    all_written: list[Path] = []

    from storeroon.reports.queries import (
        artists,
        genres,
        lyrics,
        collection_issues,
        key_inventory,
        overview,
        replaygain,
        technical,
    )
    from storeroon.reports.renderers.json_renderer import write_report

    # (label, report_name, query_fn_call)
    report_specs: list[tuple[str, str, Callable[[], object]]] = [
        ("Overview", "overview", lambda: overview.full_data(conn, aliases=conf.tags.aliases, canonical_keys=frozenset(conf.tags.required + conf.tags.recommended))),
        ("Collection issues", "collection_issues", lambda: collection_issues.full_data(conn, conf.tags)),
        ("Technical", "technical", lambda: technical.full_data(conn)),
        ("Key inventory", "key_inventory", lambda: key_inventory.full_data(conn, conf.tags)),
        (
            "Artists",
            "artists",
            lambda: artists.full_data(conn, fuzzy_threshold=threshold),
        ),
        ("Genres", "genres", lambda: genres.full_data(conn, fuzzy_threshold=threshold)),
        ("Lyrics", "lyrics", lambda: lyrics.full_data(conn)),
        ("ReplayGain", "replaygain", lambda: replaygain.full_data(conn)),
    ]

    for i, (label, name, query_fn) in enumerate(report_specs, 1):
        console.print(f"[dim]  {i:2d}/{len(report_specs)}  {label}…[/dim]")
        try:
            data = query_fn()
            path = write_report(output_dir, name, data)
            all_written.append(path)
        except Exception as exc:
            console.print(f"[red]  {label} failed: {exc}[/red]")

    conn.close()

    _print_written_files(all_written)
    return 0


# ---------------------------------------------------------------------------
# Sub-command dispatch table
# ---------------------------------------------------------------------------

_REPORT_COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "all": _cmd_all,
    "summary": _cmd_summary,
    "overview": _cmd_overview,
    "collection-issues": _cmd_collection_issues,
    "technical": _cmd_technical,
    "key-inventory": _cmd_key_inventory,
    "album-issues": _cmd_album_issues,
    "artists": _cmd_artists,
    "genres": _cmd_genres,
    "lyrics": _cmd_lyrics,
    "replaygain": _cmd_replaygain,
}


# ---------------------------------------------------------------------------
# Argument parser building
# ---------------------------------------------------------------------------


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    """Add --output and --output-dir arguments to a subcommand parser."""
    parser.add_argument(
        "--output",
        type=str,
        choices=["terminal", "json"],
        default="terminal",
        help="Output format (default: terminal)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory for output files (default: from config)",
    )


def _add_artist_args(parser: argparse.ArgumentParser) -> None:
    """Add --artist and --album arguments to a subcommand parser."""
    parser.add_argument(
        "--artist",
        type=str,
        default=None,
        help="Filter by ALBUMARTIST (case-insensitive substring match)",
    )
    parser.add_argument(
        "--album",
        type=str,
        default=None,
        help="Filter by ALBUM (requires --artist; case-insensitive substring match)",
    )


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    """Add --config argument to a subcommand parser."""
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to the TOML configuration file",
    )


def build_report_parser(subparsers: argparse._SubParsersAction) -> None:
    """Build the ``report`` subcommand and all its nested subcommands.

    Called from the main CLI parser builder.

    Parameters
    ----------
    subparsers:
        The subparsers action from the main argument parser, so that
        ``report`` becomes a top-level subcommand alongside ``scan``.
    """
    report_parser = subparsers.add_parser(
        "report",
        help="Generate analysis reports on the imported collection",
    )
    report_subs = report_parser.add_subparsers(
        dest="report_command",
        help="Available reports",
    )

    # --- all (always JSON, no --output flag) ---
    p_all = report_subs.add_parser(
        "all",
        help="Generate all 12 reports as JSON files",
    )
    p_all.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory for output files (default: from config)",
    )
    _add_config_arg(p_all)

    # --- summary (terminal-only, no --output) ---
    p_summary = report_subs.add_parser(
        "summary",
        help="Fast health-check summary across all report areas (terminal only)",
    )
    _add_config_arg(p_summary)

    # --- overview ---
    p_overview = report_subs.add_parser(
        "overview",
        help="Collection overview: totals, hierarchical artist breakdown",
    )
    _add_output_args(p_overview)
    _add_config_arg(p_overview)

    # --- collection-issues ---
    p_collection_issues = report_subs.add_parser(
        "collection-issues",
        help="Collection issues overview: album health, track health, tag quality bars",
    )
    _add_output_args(p_collection_issues)
    _add_config_arg(p_collection_issues)

    # --- technical ---
    p_technical = report_subs.add_parser(
        "technical",
        help="Audio technical quality: sample rate, bitrate, duration outliers",
    )
    _add_output_args(p_technical)
    _add_config_arg(p_technical)

    # --- key-inventory ---
    p_key_inventory = report_subs.add_parser(
        "key-inventory",
        help="Key inventory: all tag keys with classification",
    )
    _add_output_args(p_key_inventory)
    _add_config_arg(p_key_inventory)

    # --- album-issues ---
    p_album_issues = report_subs.add_parser(
        "album-issues",
        help="Detailed issues for a specific album",
    )
    _add_output_args(p_album_issues)
    p_album_issues.add_argument(
        "album_dir",
        type=str,
        help="Album directory path (parent directory of files)",
    )
    _add_config_arg(p_album_issues)

    # --- artists ---
    p_artists = report_subs.add_parser(
        "artists",
        help="Artist name consistency: case variants, fuzzy matches",
    )
    _add_output_args(p_artists)
    _add_config_arg(p_artists)

    # --- genres ---
    p_genres = report_subs.add_parser(
        "genres",
        help="Genre analysis: values, fuzzy matches, missing tags",
    )
    _add_output_args(p_genres)
    _add_config_arg(p_genres)

    # --- lyrics ---
    p_lyrics = report_subs.add_parser(
        "lyrics",
        help="Lyrics coverage by artist and album",
    )
    _add_output_args(p_lyrics)
    _add_artist_args(p_lyrics)
    _add_config_arg(p_lyrics)

    # --- replaygain ---
    p_replaygain = report_subs.add_parser(
        "replaygain",
        help="ReplayGain tag coverage, partial albums, outliers",
    )
    _add_output_args(p_replaygain)
    _add_artist_args(p_replaygain)
    _add_config_arg(p_replaygain)


# ---------------------------------------------------------------------------
# Entry point (called from the main CLI dispatcher)
# ---------------------------------------------------------------------------


def dispatch_report(args: argparse.Namespace) -> int:
    """Dispatch a ``report`` subcommand.

    Parameters
    ----------
    args:
        The parsed arguments from the main CLI parser. Must have a
        ``report_command`` attribute indicating which report to run.

    Returns
    -------
    int
        Exit code (0 for success).
    """
    report_cmd = getattr(args, "report_command", None)
    if report_cmd is None:
        console.print(
            "[yellow]No report subcommand specified. "
            "Use [bold]storeroon report --help[/bold] for available reports.[/yellow]"
        )
        return 1

    # Validate --album without --artist.
    album_filter = _get_album_filter(args)
    artist_filter = _get_artist_filter(args)
    if album_filter and not artist_filter:
        console.print(
            "[bold red]Error:[/bold red] --album can only be used in combination "
            "with --artist. Please specify --artist as well."
        )
        return 1

    handler = _REPORT_COMMANDS.get(report_cmd)
    if handler is None:
        console.print(
            f"[bold red]Unknown report subcommand:[/bold red] {report_cmd}. "
            f"Use [bold]storeroon report --help[/bold] for available reports."
        )
        return 1

    return handler(args)
