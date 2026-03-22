"""
storeroon.reports.generate — report generation logic.

All ``report generate <name>`` handlers live here. The CLI parser and
dispatch layer (``cli.py``) delegates to the functions exported from this
module.

Public API:
    REPORT_COMMANDS  — dispatch table mapping subcommand names to handlers
    generate_all_reports(conf, report_dir) — generate all reports as JSON
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


def resolve_report_dir(args: argparse.Namespace, conf: cfg.Config) -> Path:
    """Resolve the report directory from CLI args or config."""
    if hasattr(args, "report_dir") and args.report_dir:
        return Path(args.report_dir).expanduser().resolve()
    return conf.reports.report_dir.expanduser().resolve()


def _get_output_format(args: argparse.Namespace) -> str:
    """Get the output format from CLI args, defaulting to 'terminal'."""
    return getattr(args, "output", "terminal") or "terminal"


def _get_artist_filter(args: argparse.Namespace) -> str | None:
    """Get the --artist filter value, or None."""
    return getattr(args, "artist", None)


def _get_album_filter(args: argparse.Namespace) -> str | None:
    """Get the --album filter value, or None."""
    return getattr(args, "album", None)


def print_written_files(written: list[Path]) -> None:
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

    output_dir = resolve_report_dir(args, conf)
    written = write_report(output_dir, report_name, data, filters=filters)
    print_written_files([written])


# ---------------------------------------------------------------------------
# Report handlers
# ---------------------------------------------------------------------------


def _cmd_summary(args: argparse.Namespace) -> int:
    """Execute ``report generate summary`` — terminal-only fast health check."""
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
    )
    from storeroon.reports.renderers.terminal import render_master_summary

    summary = MasterSummary()

    console.print("[dim]Gathering summary data…[/dim]")

    try:
        summary.overview = overview.summary_data(conn)
    except Exception as exc:
        log.warning("Overview summary failed: %s", exc)

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
    """Execute ``report generate overview``."""
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
    """Execute ``report generate collection-issues``."""
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
        output_console.print("[yellow]Collection issues overview is HTML-only. Use --output json and storeroon report serve.[/yellow]")
    else:
        _write_json(args, conf, "collection_issues", data)

    conn.close()
    return 0


def _cmd_key_inventory(args: argparse.Namespace) -> int:
    """Execute ``report generate key-inventory``."""
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
        output_console.print("[yellow]Key inventory is HTML-only. Use --output json and storeroon report serve.[/yellow]")
    else:
        _write_json(args, conf, "key_inventory", data)

    conn.close()
    return 0


def _cmd_album_issues(args: argparse.Namespace) -> int:
    """Execute ``report generate album-issues``."""
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
    """Execute ``report generate artists``."""
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
    """Execute ``report generate genres``."""
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
    """Execute ``report generate lyrics``."""
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
    """Execute ``report generate replaygain``."""
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


# ---------------------------------------------------------------------------
# Generate all reports
# ---------------------------------------------------------------------------


def generate_all_reports(conf: cfg.Config, report_dir: Path) -> tuple[int, list[Path]]:
    """Generate all reports as JSON to *report_dir*.

    Returns (exit_code, list_of_written_paths).  Shared by ``report generate all``
    and ``report serve --generate``.
    """
    conn = _open_db(conf)
    if conn is None:
        return 0, []

    if _check_empty(conn):
        output_console.print(
            "[yellow]The database is empty — run [bold]storeroon scan[/bold] first.[/yellow]"
        )
        conn.close()
        return 0, []

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
    )
    from storeroon.reports.renderers.json_renderer import write_report

    # (label, report_name, query_fn_call)
    report_specs: list[tuple[str, str, Callable[[], object]]] = [
        ("Overview", "overview", lambda: overview.full_data(conn, aliases=conf.tags.aliases, canonical_keys=frozenset(conf.tags.required + conf.tags.recommended))),
        ("Collection issues", "collection_issues", lambda: collection_issues.full_data(conn, conf.tags)),
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
            path = write_report(report_dir, name, data)
            all_written.append(path)
        except Exception as exc:
            console.print(f"[red]  {label} failed: {exc}[/red]")

    conn.close()
    return 0, all_written


def _cmd_all(args: argparse.Namespace) -> int:
    """Execute ``report generate all`` — generate all reports as JSON."""
    conf = _load_config(args)
    if conf is None:
        return 1

    report_dir = resolve_report_dir(args, conf)
    exit_code, written = generate_all_reports(conf, report_dir)
    print_written_files(written)
    return exit_code


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

REPORT_COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "all": _cmd_all,
    "summary": _cmd_summary,
    "overview": _cmd_overview,
    "collection-issues": _cmd_collection_issues,
    "key-inventory": _cmd_key_inventory,
    "album-issues": _cmd_album_issues,
    "artists": _cmd_artists,
    "genres": _cmd_genres,
    "lyrics": _cmd_lyrics,
    "replaygain": _cmd_replaygain,
}
