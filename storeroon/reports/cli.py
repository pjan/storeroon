"""
storeroon.reports.cli — CLI layer for Sprint 2 reports.

Wires the query and renderer layers together. Handles ``--output``,
``--output-dir``, ``--artist``, ``--album``, ``--min-severity`` flags.
Handles empty database gracefully (print a clear message, exit 0).
Handles ``--artist`` / ``--album`` filter producing no results gracefully.

CLI structure::

    python -m storeroon report summary
    python -m storeroon report overview      [--output terminal|csv|json|html] [--output-dir PATH]
    python -m storeroon report technical     [--output ...] [--output-dir PATH]
    python -m storeroon report tags          [--output ...] [--output-dir PATH]
    python -m storeroon report tag-formats   [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report album-consistency [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report ids           [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report duplicates    [--output ...] [--output-dir PATH]
    python -m storeroon report issues        [--output ...] [--output-dir PATH] [--min-severity ...]
    python -m storeroon report artists       [--output ...] [--output-dir PATH]
    python -m storeroon report genres        [--output ...] [--output-dir PATH]
    python -m storeroon report lyrics        [--output ...] [--output-dir PATH] [--artist ARTIST]
    python -m storeroon report replaygain    [--output ...] [--output-dir PATH] [--artist ARTIST]
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
from storeroon.reports.utils import now_filename_stamp

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


def _build_filter_string(
    artist: str | None = None,
    album: str | None = None,
    min_severity: str | None = None,
) -> str | None:
    """Build a human-readable filter description for HTML reports."""
    parts: list[str] = []
    if artist:
        parts.append(f"artist={artist!r}")
    if album:
        parts.append(f"album={album!r}")
    if min_severity and min_severity != "info":
        parts.append(f"min_severity={min_severity}")
    return ", ".join(parts) if parts else None


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
        album_consistency,
        artists,
        duplicates,
        genres,
        ids,
        issues,
        lyrics,
        overview,
        replaygain,
        tag_coverage,
        tag_formats,
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
        summary.tags = tag_coverage.summary_data(conn, conf.tags)
    except Exception as exc:
        log.warning("Tag coverage summary failed: %s", exc)

    try:
        summary.tag_formats = tag_formats.summary_data(conn)
    except Exception as exc:
        log.warning("Tag formats summary failed: %s", exc)

    try:
        summary.album_consistency = album_consistency.summary_data(conn)
    except Exception as exc:
        log.warning("Album consistency summary failed: %s", exc)

    try:
        summary.ids = ids.summary_data(conn)
    except Exception as exc:
        log.warning("IDs summary failed: %s", exc)

    try:
        summary.duplicates = duplicates.summary_data(conn)
    except Exception as exc:
        log.warning("Duplicates summary failed: %s", exc)

    try:
        summary.issues = issues.summary_data(conn)
    except Exception as exc:
        log.warning("Issues summary failed: %s", exc)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = overview.full_data(conn)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_overview(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_overview(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_overview(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_overview(output_dir, data, ts)
        _print_written_files(written)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = technical.full_data(conn)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_technical(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_technical(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_technical(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_technical(output_dir, data, ts)
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_tags(args: argparse.Namespace) -> int:
    """Execute ``report tags``."""
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

    from storeroon.reports.queries import tag_coverage
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = tag_coverage.full_data(conn, conf.tags)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_tag_coverage(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_tag_coverage(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_tag_coverage(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_tag_coverage(output_dir, data, ts)
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_tag_formats(args: argparse.Namespace) -> int:
    """Execute ``report tag-formats``."""
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

    from storeroon.reports.queries import tag_formats
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = tag_formats.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)
    filters = _build_filter_string(artist=artist_filter)

    if fmt == "terminal":
        terminal.render_tag_formats(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_tag_formats(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_tag_formats(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_tag_formats(
                output_dir, data, ts, filters=filters
            )
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_album_consistency(args: argparse.Namespace) -> int:
    """Execute ``report album-consistency``."""
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

    from storeroon.reports.queries import album_consistency
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = album_consistency.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)
    filters = _build_filter_string(artist=artist_filter)

    if fmt == "terminal":
        terminal.render_album_consistency(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_album_consistency(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_album_consistency(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_album_consistency(
                output_dir, data, ts, filters=filters
            )
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_ids(args: argparse.Namespace) -> int:
    """Execute ``report ids``."""
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

    from storeroon.reports.queries import ids
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = ids.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)
    filters = _build_filter_string(artist=artist_filter)

    if fmt == "terminal":
        terminal.render_ids(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_ids(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_ids(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_ids(output_dir, data, ts, filters=filters)
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_duplicates(args: argparse.Namespace) -> int:
    """Execute ``report duplicates``."""
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

    from storeroon.reports.queries import duplicates
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = duplicates.full_data(conn)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_duplicates(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_duplicates(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_duplicates(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_duplicates(output_dir, data, ts)
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_issues(args: argparse.Namespace) -> int:
    """Execute ``report issues``."""
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

    from storeroon.reports.queries import issues
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    min_severity = _get_min_severity(args)
    data = issues.full_data(conn, min_severity=min_severity)
    fmt = _get_output_format(args)
    filters = _build_filter_string(min_severity=min_severity)

    if fmt == "terminal":
        terminal.render_issues(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_issues(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_issues(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_issues(output_dir, data, ts, filters=filters)
        _print_written_files(written)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    threshold = conf.reports.fuzzy_threshold
    data = artists.full_data(conn, fuzzy_threshold=threshold)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_artists(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_artists(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_artists(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_artists(output_dir, data, ts)
        _print_written_files(written)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    threshold = conf.reports.fuzzy_threshold
    data = genres.full_data(conn, fuzzy_threshold=threshold)
    fmt = _get_output_format(args)

    if fmt == "terminal":
        terminal.render_genres(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_genres(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_genres(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_genres(output_dir, data, ts)
        _print_written_files(written)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = lyrics.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)
    filters = _build_filter_string(artist=artist_filter)

    if fmt == "terminal":
        terminal.render_lyrics(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_lyrics(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_lyrics(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_lyrics(output_dir, data, ts, filters=filters)
        _print_written_files(written)

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
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
        terminal,
    )

    data = replaygain.full_data(conn, artist_filter=artist_filter)
    fmt = _get_output_format(args)
    filters = _build_filter_string(artist=artist_filter)

    if fmt == "terminal":
        terminal.render_replaygain(output_console, data)
    else:
        output_dir = _resolve_output_dir(args, conf)
        ts = now_filename_stamp()
        written: list[Path] = []
        if fmt == "csv":
            written = csv_renderer.write_replaygain(output_dir, data, ts)
        elif fmt == "json":
            written = json_renderer.write_replaygain(output_dir, data, ts)
        elif fmt == "html":
            written = html_renderer.write_replaygain(
                output_dir, data, ts, filters=filters
            )
        _print_written_files(written)

    conn.close()
    return 0


def _cmd_all(args: argparse.Namespace) -> int:
    """Execute ``report all`` — generate every report in a single run.

    Loads config and opens the database once, then runs all 12 individual
    reports sequentially with a shared timestamp. The ``summary`` report
    is excluded (it is terminal-only and not a file-based report).
    """
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

    fmt = _get_output_format(args)
    if fmt == "terminal":
        console.print(
            "[yellow]The [bold]report all[/bold] command writes files — "
            "please specify [bold]--output csv|json|html[/bold].[/yellow]"
        )
        conn.close()
        return 1

    output_dir = _resolve_output_dir(args, conf)
    ts = now_filename_stamp()
    all_written: list[Path] = []
    threshold = conf.reports.fuzzy_threshold

    from storeroon.reports.queries import (
        album_consistency,
        artists,
        duplicates,
        genres,
        ids,
        issues,
        lyrics,
        overview,
        replaygain,
        tag_coverage,
        tag_formats,
        technical,
    )
    from storeroon.reports.renderers import (
        csv_renderer,
        html_renderer,
        json_renderer,
    )

    # Each entry: (label, query_fn, writer_map)
    # writer_map is {format: callable(output_dir, data, ts, **kw) -> list[Path]}
    reports: list[tuple[str, object, dict[str, object]]] = []

    # --- 1. Overview ---
    console.print("[dim]  1/12  Overview…[/dim]")
    try:
        data_overview = overview.full_data(conn)
        w: list[Path] = []
        if fmt == "csv":
            w = csv_renderer.write_overview(output_dir, data_overview, ts)
        elif fmt == "json":
            w = json_renderer.write_overview(output_dir, data_overview, ts)
        elif fmt == "html":
            w = html_renderer.write_overview(output_dir, data_overview, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Overview failed: {exc}[/red]")

    # --- 2. Technical ---
    console.print("[dim]  2/12  Technical…[/dim]")
    try:
        data_technical = technical.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_technical(output_dir, data_technical, ts)
        elif fmt == "json":
            w = json_renderer.write_technical(output_dir, data_technical, ts)
        elif fmt == "html":
            w = html_renderer.write_technical(output_dir, data_technical, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Technical failed: {exc}[/red]")

    # --- 3. Tags ---
    console.print("[dim]  3/12  Tag coverage…[/dim]")
    try:
        data_tags = tag_coverage.full_data(conn, conf.tags)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_tag_coverage(output_dir, data_tags, ts)
        elif fmt == "json":
            w = json_renderer.write_tag_coverage(output_dir, data_tags, ts)
        elif fmt == "html":
            w = html_renderer.write_tag_coverage(output_dir, data_tags, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Tag coverage failed: {exc}[/red]")

    # --- 4. Tag formats ---
    console.print("[dim]  4/12  Tag formats…[/dim]")
    try:
        data_tag_formats = tag_formats.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_tag_formats(output_dir, data_tag_formats, ts)
        elif fmt == "json":
            w = json_renderer.write_tag_formats(output_dir, data_tag_formats, ts)
        elif fmt == "html":
            w = html_renderer.write_tag_formats(output_dir, data_tag_formats, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Tag formats failed: {exc}[/red]")

    # --- 5. Album consistency ---
    console.print("[dim]  5/12  Album consistency…[/dim]")
    try:
        data_consistency = album_consistency.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_album_consistency(output_dir, data_consistency, ts)
        elif fmt == "json":
            w = json_renderer.write_album_consistency(output_dir, data_consistency, ts)
        elif fmt == "html":
            w = html_renderer.write_album_consistency(output_dir, data_consistency, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Album consistency failed: {exc}[/red]")

    # --- 6. IDs ---
    console.print("[dim]  6/12  External IDs…[/dim]")
    try:
        data_ids = ids.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_ids(output_dir, data_ids, ts)
        elif fmt == "json":
            w = json_renderer.write_ids(output_dir, data_ids, ts)
        elif fmt == "html":
            w = html_renderer.write_ids(output_dir, data_ids, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  External IDs failed: {exc}[/red]")

    # --- 7. Duplicates ---
    console.print("[dim]  7/12  Duplicates…[/dim]")
    try:
        data_duplicates = duplicates.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_duplicates(output_dir, data_duplicates, ts)
        elif fmt == "json":
            w = json_renderer.write_duplicates(output_dir, data_duplicates, ts)
        elif fmt == "html":
            w = html_renderer.write_duplicates(output_dir, data_duplicates, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Duplicates failed: {exc}[/red]")

    # --- 8. Issues ---
    console.print("[dim]  8/12  Scan issues…[/dim]")
    try:
        data_issues = issues.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_issues(output_dir, data_issues, ts)
        elif fmt == "json":
            w = json_renderer.write_issues(output_dir, data_issues, ts)
        elif fmt == "html":
            w = html_renderer.write_issues(output_dir, data_issues, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Scan issues failed: {exc}[/red]")

    # --- 9. Artists ---
    console.print("[dim]  9/12  Artists…[/dim]")
    try:
        data_artists = artists.full_data(conn, fuzzy_threshold=threshold)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_artists(output_dir, data_artists, ts)
        elif fmt == "json":
            w = json_renderer.write_artists(output_dir, data_artists, ts)
        elif fmt == "html":
            w = html_renderer.write_artists(output_dir, data_artists, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Artists failed: {exc}[/red]")

    # --- 10. Genres ---
    console.print("[dim] 10/12  Genres…[/dim]")
    try:
        data_genres = genres.full_data(conn, fuzzy_threshold=threshold)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_genres(output_dir, data_genres, ts)
        elif fmt == "json":
            w = json_renderer.write_genres(output_dir, data_genres, ts)
        elif fmt == "html":
            w = html_renderer.write_genres(output_dir, data_genres, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Genres failed: {exc}[/red]")

    # --- 11. Lyrics ---
    console.print("[dim] 11/12  Lyrics…[/dim]")
    try:
        data_lyrics = lyrics.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_lyrics(output_dir, data_lyrics, ts)
        elif fmt == "json":
            w = json_renderer.write_lyrics(output_dir, data_lyrics, ts)
        elif fmt == "html":
            w = html_renderer.write_lyrics(output_dir, data_lyrics, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  Lyrics failed: {exc}[/red]")

    # --- 12. ReplayGain ---
    console.print("[dim] 12/12  ReplayGain…[/dim]")
    try:
        data_replaygain = replaygain.full_data(conn)
        w = []
        if fmt == "csv":
            w = csv_renderer.write_replaygain(output_dir, data_replaygain, ts)
        elif fmt == "json":
            w = json_renderer.write_replaygain(output_dir, data_replaygain, ts)
        elif fmt == "html":
            w = html_renderer.write_replaygain(output_dir, data_replaygain, ts)
        all_written.extend(w)
    except Exception as exc:
        console.print(f"[red]  ReplayGain failed: {exc}[/red]")

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
    "technical": _cmd_technical,
    "tags": _cmd_tags,
    "tag-formats": _cmd_tag_formats,
    "album-consistency": _cmd_album_consistency,
    "ids": _cmd_ids,
    "duplicates": _cmd_duplicates,
    "issues": _cmd_issues,
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
        choices=["terminal", "csv", "json", "html"],
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

    # --- all ---
    p_all = report_subs.add_parser(
        "all",
        help="Generate all 12 reports at once (requires --output csv|json|html)",
    )
    _add_output_args(p_all)
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
        help="Collection overview: totals, release types, distributions",
    )
    _add_output_args(p_overview)
    _add_config_arg(p_overview)

    # --- technical ---
    p_technical = report_subs.add_parser(
        "technical",
        help="Audio technical quality: sample rate, bitrate, duration outliers",
    )
    _add_output_args(p_technical)
    _add_config_arg(p_technical)

    # --- tags ---
    p_tags = report_subs.add_parser(
        "tags",
        help="Tag coverage and key inventory",
    )
    _add_output_args(p_tags)
    _add_config_arg(p_tags)

    # --- tag-formats ---
    p_tag_formats = report_subs.add_parser(
        "tag-formats",
        help="Tag format quality: date, track number, ISRC, MBID validation",
    )
    _add_output_args(p_tag_formats)
    _add_artist_args(p_tag_formats)
    _add_config_arg(p_tag_formats)

    # --- album-consistency ---
    p_album_consistency = report_subs.add_parser(
        "album-consistency",
        help="Intra-album field consistency and track numbering checks",
    )
    _add_output_args(p_album_consistency)
    _add_artist_args(p_album_consistency)
    _add_config_arg(p_album_consistency)

    # --- ids ---
    p_ids = report_subs.add_parser(
        "ids",
        help="External ID coverage and integrity (MusicBrainz, Discogs)",
    )
    _add_output_args(p_ids)
    _add_artist_args(p_ids)
    _add_config_arg(p_ids)

    # --- duplicates ---
    p_duplicates = report_subs.add_parser(
        "duplicates",
        help="Duplicate detection: exact, MBID, probable",
    )
    _add_output_args(p_duplicates)
    _add_config_arg(p_duplicates)

    # --- issues ---
    p_issues = report_subs.add_parser(
        "issues",
        help="Scan issues from Phase 1 import",
    )
    _add_output_args(p_issues)
    p_issues.add_argument(
        "--min-severity",
        type=str,
        choices=["info", "warning", "error", "critical"],
        default="info",
        help="Minimum severity to include (default: info)",
    )
    _add_config_arg(p_issues)

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
