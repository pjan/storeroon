"""
Rich CLI for storeroon.

Entry points:
    storeroon scan [--root PATH] [--config PATH] [--dry-run] [--rescan]
    storeroon report generate <name> [--output terminal|json] [--report-dir PATH]
    storeroon report generate all [--report-dir PATH]
    storeroon report serve [--port 8080] [--report-dir PATH] [--generate]

This module only builds the top-level parser and dispatches to the
subcommand modules:
    storeroon.scanner.cli   — scan command
    storeroon.reports.cli   — report command (generate + serve)
"""

from __future__ import annotations

import argparse
import sys

from storeroon.reports.cli import build_report_parser, dispatch_report
from storeroon.scanner.cli import build_scan_parser, dispatch_scan


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storeroon",
        description="Music collection management toolchain",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    build_scan_parser(subparsers)
    build_report_parser(subparsers)

    return parser


def cli() -> None:
    """Main entry point for the ``storeroon`` CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "scan":
        sys.exit(dispatch_scan(args))
    elif args.command == "report":
        sys.exit(dispatch_report(args))
    else:
        parser.print_help()
        sys.exit(1)
