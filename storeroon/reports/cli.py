"""
storeroon.reports.cli — CLI parser, dispatch, and serve command for reports.

Builds the ``report`` subcommand tree and dispatches to either
``storeroon.reports.generate`` (report generation) or the local
web server (``storeroon.reports.serve``).

CLI structure::

    storeroon report generate summary
    storeroon report generate overview      [--output terminal|json] [--report-dir PATH]
    storeroon report generate album-issues  ALBUM_DIR [--output ...] [--report-dir PATH]
    storeroon report generate artists       [--output ...] [--report-dir PATH]
    storeroon report generate genres        [--output ...] [--report-dir PATH]
    storeroon report generate lyrics        [--output ...] [--report-dir PATH] [--artist ARTIST]
    storeroon report generate replaygain    [--output ...] [--report-dir PATH] [--artist ARTIST]
    storeroon report generate all           [--report-dir PATH]
    storeroon report serve                  [--port 8080] [--report-dir PATH] [--generate]
"""

from __future__ import annotations

import argparse

from rich.console import Console

from storeroon import config as cfg
from storeroon.reports.generate import (
    REPORT_COMMANDS,
    generate_all_reports,
    print_written_files,
    resolve_report_dir,
)

console = Console(stderr=True)


# ---------------------------------------------------------------------------
# Argument parser helpers
# ---------------------------------------------------------------------------


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    """Add --output and --report-dir arguments to a subcommand parser."""
    parser.add_argument(
        "--output",
        type=str,
        choices=["terminal", "json"],
        default="terminal",
        help="Output format (default: terminal)",
    )
    parser.add_argument(
        "--report-dir",
        type=str,
        default=None,
        help="Directory for report files (default: from config)",
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


# ---------------------------------------------------------------------------
# Parser builder
# ---------------------------------------------------------------------------


def build_report_parser(subparsers: argparse._SubParsersAction) -> None:
    """Build the ``report`` subcommand and all its nested subcommands.

    Called from the main CLI parser builder.

    Structure::

        report
        ├── generate
        │   ├── all / summary / overview / ... / replaygain
        └── serve  [--port] [--report-dir] [--config] [--generate]
    """
    report_parser = subparsers.add_parser(
        "report",
        help="Generate or serve analysis reports",
    )
    report_subs = report_parser.add_subparsers(
        dest="report_command",
        help="Available commands",
    )

    # =======================================================================
    # report generate
    # =======================================================================
    generate_parser = report_subs.add_parser(
        "generate",
        help="Generate analysis reports on the imported collection",
    )
    gen_subs = generate_parser.add_subparsers(
        dest="generate_command",
        help="Available reports",
    )

    # --- all (always JSON, no --output flag) ---
    p_all = gen_subs.add_parser(
        "all",
        help="Generate all reports as JSON files",
    )
    p_all.add_argument(
        "--report-dir",
        type=str,
        default=None,
        help="Directory for report files (default: from config)",
    )
    _add_config_arg(p_all)

    # --- summary (terminal-only, no --output) ---
    p_summary = gen_subs.add_parser(
        "summary",
        help="Fast health-check summary across all report areas (terminal only)",
    )
    _add_config_arg(p_summary)

    # --- overview ---
    p_overview = gen_subs.add_parser(
        "overview",
        help="Collection overview: totals, hierarchical artist breakdown",
    )
    _add_output_args(p_overview)
    _add_config_arg(p_overview)

    # --- collection-issues ---
    p_collection_issues = gen_subs.add_parser(
        "collection-issues",
        help="Collection issues overview: album health, track health, tag quality bars",
    )
    _add_output_args(p_collection_issues)
    _add_config_arg(p_collection_issues)

    # --- key-inventory ---
    p_key_inventory = gen_subs.add_parser(
        "key-inventory",
        help="Key inventory: all tag keys with classification",
    )
    _add_output_args(p_key_inventory)
    _add_config_arg(p_key_inventory)

    # --- album-issues ---
    p_album_issues = gen_subs.add_parser(
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
    p_artists = gen_subs.add_parser(
        "artists",
        help="Artist name consistency: case variants, fuzzy matches",
    )
    _add_output_args(p_artists)
    _add_config_arg(p_artists)

    # --- genres ---
    p_genres = gen_subs.add_parser(
        "genres",
        help="Genre analysis: values, fuzzy matches, missing tags",
    )
    _add_output_args(p_genres)
    _add_config_arg(p_genres)

    # --- lyrics ---
    p_lyrics = gen_subs.add_parser(
        "lyrics",
        help="Lyrics coverage by artist and album",
    )
    _add_output_args(p_lyrics)
    _add_artist_args(p_lyrics)
    _add_config_arg(p_lyrics)

    # --- replaygain ---
    p_replaygain = gen_subs.add_parser(
        "replaygain",
        help="ReplayGain tag coverage, partial albums, outliers",
    )
    _add_output_args(p_replaygain)
    _add_artist_args(p_replaygain)
    _add_config_arg(p_replaygain)

    # =======================================================================
    # report serve
    # =======================================================================
    serve_parser = report_subs.add_parser(
        "serve",
        help="Start a local web server to browse HTML reports",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to listen on (default: 8080)",
    )
    serve_parser.add_argument(
        "--report-dir",
        type=str,
        default=None,
        help="Directory containing report JSON files (default: from config)",
    )
    serve_parser.add_argument(
        "--generate",
        action="store_true",
        default=False,
        help="Regenerate all reports before serving",
    )
    _add_config_arg(serve_parser)


# ---------------------------------------------------------------------------
# Serve command
# ---------------------------------------------------------------------------


def _cmd_serve(args: argparse.Namespace) -> int:
    """Start the local web server for browsing HTML reports."""
    from storeroon.reports.serve import serve_reports

    try:
        conf = cfg.load(getattr(args, "config", None))
    except cfg.ConfigError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        return 1

    report_dir = resolve_report_dir(args, conf)

    # If --generate is set, regenerate all reports first.
    if getattr(args, "generate", False):
        console.print("[bold cyan]Generating reports…[/bold cyan]")
        exit_code, written = generate_all_reports(conf, report_dir)
        if exit_code != 0:
            return exit_code
        print_written_files(written)
        console.print()

    if not report_dir.is_dir():
        console.print(
            f"[yellow]Report directory does not exist: {report_dir}\n"
            f"Run [bold]storeroon report generate all[/bold] first, "
            f"or use [bold]--generate[/bold] to generate reports automatically.[/yellow]"
        )
        return 1

    db_path = conf.database.path.expanduser().resolve()
    aliases = conf.tags.aliases
    canonical_keys = frozenset(conf.tags.required + conf.tags.recommended)
    serve_reports(
        report_dir, port=args.port, db_path=db_path,
        aliases=aliases, canonical_keys=canonical_keys,
    )
    return 0


# ---------------------------------------------------------------------------
# Dispatch (called from the main CLI)
# ---------------------------------------------------------------------------


def dispatch_report(args: argparse.Namespace) -> int:
    """Dispatch a ``report`` subcommand.

    Routes to either ``report generate <name>`` or ``report serve``.
    """
    report_cmd = getattr(args, "report_command", None)
    if report_cmd is None:
        console.print(
            "[yellow]No report subcommand specified. "
            "Use [bold]storeroon report --help[/bold] for available commands.[/yellow]"
        )
        return 1

    # --- report serve ---
    if report_cmd == "serve":
        return _cmd_serve(args)

    # --- report generate <name> ---
    if report_cmd == "generate":
        gen_cmd = getattr(args, "generate_command", None)
        if gen_cmd is None:
            console.print(
                "[yellow]No report specified. "
                "Use [bold]storeroon report generate --help[/bold] for available reports.[/yellow]"
            )
            return 1

        # Validate --album without --artist.
        album_filter = getattr(args, "album", None)
        artist_filter = getattr(args, "artist", None)
        if album_filter and not artist_filter:
            console.print(
                "[bold red]Error:[/bold red] --album can only be used in combination "
                "with --artist. Please specify --artist as well."
            )
            return 1

        handler = REPORT_COMMANDS.get(gen_cmd)
        if handler is None:
            console.print(
                f"[bold red]Unknown report:[/bold red] {gen_cmd}. "
                f"Use [bold]storeroon report generate --help[/bold] for available reports."
            )
            return 1

        return handler(args)

    console.print(
        f"[bold red]Unknown report subcommand:[/bold red] {report_cmd}. "
        f"Use [bold]storeroon report --help[/bold] for available commands."
    )
    return 1
