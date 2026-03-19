"""
Rich CLI for storeroon.

Entry points:
    python -m storeroon scan --root /path/to/collection [--dry-run]
    python -m storeroon scan --config storeroon.toml [--dry-run]
    python -m storeroon report summary
    python -m storeroon report overview [--output terminal|csv|json|html]
    python -m storeroon report technical [--output ...]
    ... (see ``storeroon report --help`` for all subcommands)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from itertools import islice
from pathlib import Path
from typing import Iterator, TypeVar

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from storeroon import config as cfg
from storeroon.db import MigrationError, connect, migrate
from storeroon.reports.cli import build_report_parser, dispatch_report
from storeroon.scanner import (
    DuplicateStats,
    ImportStats,
    detect_duplicates,
    import_batch,
    walk_collection,
)

log = logging.getLogger("storeroon")

T = TypeVar("T")

console = Console(stderr=True)
output = Console()  # stdout — for data output


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _batched(iterable: Iterator[T], n: int) -> Iterator[list[T]]:
    """Yield successive lists of up to *n* items from *iterable*."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


def _setup_logging(level: str) -> None:
    """Configure the root ``storeroon`` logger with a Rich handler."""
    handler = RichHandler(
        console=console,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))


def _print_scan_summary(
    total_import: ImportStats,
    dup_stats: DuplicateStats,
    elapsed: float,
    dry_run: bool,
) -> None:
    """Print a rich summary table after a scan completes."""
    table = Table(
        title="[bold]Scan Summary[/bold]"
        + (" [dim](dry run)[/dim]" if dry_run else ""),
        show_header=False,
        min_width=42,
    )
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    table.add_row("Files processed", f"{total_import.files_processed:,}")
    table.add_row("Files imported", f"{total_import.files_imported:,}")
    table.add_row(
        "Files skipped (existing)", f"{total_import.files_skipped_existing:,}"
    )
    table.add_row("Files unreadable", f"{total_import.files_unreadable:,}")
    table.add_row("Tags imported", f"{total_import.tags_imported:,}")
    table.add_row("Issues raised", f"{total_import.issues_raised:,}")
    table.add_section()
    table.add_row("Duplicate groups", f"{dup_stats.groups_found:,}")
    table.add_row("Duplicate files", f"{dup_stats.files_affected:,}")
    table.add_row("Duplicate issues raised", f"{dup_stats.issues_raised:,}")
    table.add_section()

    mins, secs = divmod(elapsed, 60)
    if mins > 0:
        table.add_row("Elapsed time", f"{int(mins)}m {secs:.1f}s")
    else:
        table.add_row("Elapsed time", f"{secs:.1f}s")

    if total_import.files_processed > 0 and elapsed > 0:
        rate = total_import.files_processed / elapsed
        table.add_row("Rate", f"{rate:.1f} files/s")

    output.print()
    output.print(table)


# ---------------------------------------------------------------------------
# Scan command
# ---------------------------------------------------------------------------


def _cmd_scan(args: argparse.Namespace) -> int:
    """Execute the ``scan`` command."""
    # --- Load configuration -----------------------------------------------
    try:
        conf = cfg.load(args.config)
    except cfg.ConfigError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        return 1

    _setup_logging(conf.logging.level)

    # Allow --root to override the config file.
    collection_root = Path(args.root) if args.root else conf.collection.root
    collection_root = collection_root.expanduser().resolve()

    if not collection_root.is_dir():
        console.print(
            f"[bold red]Collection root does not exist:[/bold red] {collection_root}"
        )
        return 1

    dry_run: bool = args.dry_run

    console.print(f"[bold]Collection root:[/bold] {collection_root}")
    console.print(f"[bold]Database:[/bold]        {conf.database.path}")
    console.print(f"[bold]Checksums:[/bold]       {conf.scan.checksums}")
    console.print(f"[bold]Batch size:[/bold]      {conf.scan.batch_size}")
    if dry_run:
        console.print("[bold yellow]DRY RUN — no database writes[/bold yellow]")
    console.print()

    # --- Database setup ---------------------------------------------------
    if dry_run:
        # In dry-run mode we still need a database for the schema (in-memory
        # would lose the schema between connections, so use a temp file).
        # Actually, we can just open the real DB read-only if it exists,
        # or create an in-memory one if it doesn't.
        db_path = conf.database.path.expanduser()
        if db_path.is_file():
            conn = connect(db_path, read_only=True)
        else:
            conn = connect(":memory:")
            migrate(conn)
    else:
        conn = connect(conf.database.path)
        try:
            applied = migrate(conn)
            if applied:
                console.print(
                    f"[green]Applied {len(applied)} migration(s):[/green] "
                    + ", ".join(applied)
                )
        except MigrationError as exc:
            console.print(f"[bold red]Migration error:[/bold red] {exc}")
            conn.close()
            return 1

    # --- Phase 1: Walk & discover ----------------------------------------
    console.print("[bold cyan]Phase 1:[/bold cyan] Discovering FLAC files…")

    # First pass: count files for progress bar (fast — no checksums).
    # We collect DiscoveredFile objects with checksums in the second pass.
    # For very large collections, we use a two-phase approach:
    #   1. Quick count via os.walk (no checksums, no mutagen).
    #   2. Full walk with checksums + import.
    #
    # But for simplicity and correctness, we do a single walk and buffer
    # the discovery into batches.  The progress bar updates as files are
    # discovered and imported.

    total_stats = ImportStats()
    t0 = time.monotonic()

    discovery_progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )

    # We don't know the total count upfront, so we start with an
    # indeterminate progress bar and update it as we go.
    with discovery_progress:
        discover_task = discovery_progress.add_task(
            "Discovering & importing", total=None
        )

        flac_iter = walk_collection(
            collection_root,
            checksums=conf.scan.checksums,
            flac_only=True,
        )

        for batch in _batched(flac_iter, conf.scan.batch_size):
            batch_stats = import_batch(
                conn,
                batch,
                conf.scan.required_tags,
                dry_run=dry_run,
            )

            total_stats.files_processed += batch_stats.files_processed
            total_stats.files_imported += batch_stats.files_imported
            total_stats.files_skipped_existing += batch_stats.files_skipped_existing
            total_stats.files_unreadable += batch_stats.files_unreadable
            total_stats.tags_imported += batch_stats.tags_imported
            total_stats.issues_raised += batch_stats.issues_raised

            discovery_progress.update(
                discover_task,
                completed=total_stats.files_processed,
                description=(
                    f"Importing ({total_stats.files_imported:,} imported, "
                    f"{total_stats.files_skipped_existing:,} skipped)"
                ),
            )

    console.print()

    # --- Phase 2: Duplicate detection ------------------------------------
    console.print("[bold cyan]Phase 2:[/bold cyan] Detecting duplicates…")

    dup_stats = detect_duplicates(conn, dry_run=dry_run)

    elapsed = time.monotonic() - t0

    # --- Summary ----------------------------------------------------------
    _print_scan_summary(total_stats, dup_stats, elapsed, dry_run)

    conn.close()
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storeroon",
        description="Music collection management toolchain",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- scan -------------------------------------------------------------
    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan a FLAC collection and import metadata into the database",
    )
    scan_parser.add_argument(
        "--root",
        type=str,
        default=None,
        help="Path to the collection root (overrides config file)",
    )
    scan_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to the TOML configuration file",
    )
    scan_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Analyse files without writing to the database",
    )

    # --- report -----------------------------------------------------------
    build_report_parser(subparsers)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def cli() -> None:
    """Main entry point for the ``storeroon`` CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "scan":
        sys.exit(_cmd_scan(args))
    elif args.command == "report":
        sys.exit(dispatch_report(args))
    else:
        parser.print_help()
        sys.exit(1)
