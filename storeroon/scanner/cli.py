"""
storeroon.scanner.cli — CLI parser and dispatch for the ``scan`` command.

Public API:
    build_scan_parser(subparsers) — add the ``scan`` subparser
    dispatch_scan(args) — run the scan command
"""

from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console

from storeroon import config as cfg

console = Console(stderr=True)


# ---------------------------------------------------------------------------
# Parser builder
# ---------------------------------------------------------------------------


def build_scan_parser(subparsers: argparse._SubParsersAction) -> None:
    """Add the ``scan`` subparser to the top-level CLI."""
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
    scan_parser.add_argument(
        "--rescan",
        action="store_true",
        default=False,
        help="Clear all existing data and perform a full rescan",
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def dispatch_scan(args: argparse.Namespace) -> int:
    """Parse args and delegate to the scan orchestrator."""
    from storeroon.scanner.scan import run_scan

    try:
        conf = cfg.load(args.config)
    except cfg.ConfigError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        return 1

    collection_root = Path(args.root) if args.root else conf.collection.root
    collection_root = collection_root.expanduser().resolve()

    if not collection_root.is_dir():
        console.print(
            f"[bold red]Collection root does not exist:[/bold red] {collection_root}"
        )
        return 1

    return run_scan(
        conf,
        collection_root,
        dry_run=args.dry_run,
        rescan=args.rescan,
    )
