"""
storeroon.scanner.scan — scan orchestration logic.

Handles database setup, collection walking, batch importing, progress
display, and summary output.

Public API:
    run_scan(conf, collection_root, dry_run, rescan) -> int
"""

from __future__ import annotations

import logging
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

from storeroon.config import Config
from storeroon.db import MigrationError, connect, migrate
from storeroon.scanner import ImportStats, import_batch, walk_collection

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
# Public API
# ---------------------------------------------------------------------------


def run_scan(
    conf: Config,
    collection_root: Path,
    *,
    dry_run: bool = False,
    rescan: bool = False,
) -> int:
    """Run the full scan pipeline.

    Returns an exit code (0 for success).
    """
    _setup_logging(conf.logging.level)

    console.print(f"[bold]Collection root:[/bold] {collection_root}")
    console.print(f"[bold]Database:[/bold]        {conf.database.path}")
    console.print(f"[bold]Checksums:[/bold]       {conf.scan.checksums}")
    console.print(f"[bold]Batch size:[/bold]      {conf.scan.batch_size}")
    if dry_run:
        console.print("[bold yellow]DRY RUN — no database writes[/bold yellow]")
    if rescan:
        console.print(
            "[bold yellow]RESCAN MODE — clearing all existing data[/bold yellow]"
        )
    console.print()

    # --- Database setup ---------------------------------------------------
    if dry_run:
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

    # --- Clear existing data for rescan ----------------------------------
    if rescan and not dry_run:
        console.print("[bold cyan]Clearing existing data...[/bold cyan]")
        try:
            deleted_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            conn.execute("DELETE FROM files")
            conn.commit()
            console.print(
                f"[green]Cleared {deleted_files:,} file(s) and all related data[/green]"
            )
            console.print()
        except Exception as exc:
            console.print(f"[bold red]Error clearing data:[/bold red] {exc}")
            conn.close()
            return 1

    # --- Walk & import ----------------------------------------------------
    console.print("[bold cyan]Phase 1:[/bold cyan] Discovering FLAC files…")

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
                conf.tags,
                dry_run=dry_run,
                skip_existing_check=rescan,
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

    elapsed = time.monotonic() - t0

    # --- Summary ----------------------------------------------------------
    _print_scan_summary(total_stats, elapsed, dry_run)

    conn.close()
    return 0
