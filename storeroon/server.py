"""
storeroon.server — local HTTP server for browsing HTML reports.

Reads JSON report files produced by ``storeroon report --output json`` and
renders them as HTML on the fly using the same Jinja2 templates and section
builders used by the static renderer.

Routes:
    GET /                          Dashboard listing all reports
    GET /report/<name>             Individual report rendered as HTML
    GET /report/album-issues?dir=  Album issue detail page (queries DB)
    GET /api/<name>.json           Raw JSON passthrough
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from jinja2 import Template

from storeroon.reports.renderers.html_sections import (
    REPORT_TITLES,
    SECTION_BUILDERS,
    build_album_issues_sections,
)
from storeroon.reports.serialization import REPORT_DATA_CLASSES, from_dict
from storeroon.reports.utils import REPORT_NAMES, build_filter_string

log = logging.getLogger("storeroon.server")


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------


@lru_cache(maxsize=2)
def _load_template(name: str) -> Template:
    """Load a Jinja2 template from the package resources."""
    ref = resources.files("storeroon.reports.templates").joinpath(name)
    return Template(ref.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# JSON file helpers
# ---------------------------------------------------------------------------


def _read_envelope(json_dir: Path, report_name: str) -> dict[str, Any] | None:
    """Read and parse a report JSON envelope. Returns None if missing/invalid."""
    filepath = json_dir / f"{report_name}.json"
    if not filepath.is_file():
        return None
    try:
        with open(filepath, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Failed to read %s: %s", filepath, exc)
        return None


def _build_nav_links(current: str) -> list[dict[str, Any]]:
    """Build navigation link dicts for the report template."""
    return [
        {
            "name": name,
            "title": REPORT_TITLES.get(name, name),
            "active": name == current,
        }
        for name in REPORT_NAMES
    ]


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------


class StoreroonHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the storeroon report server."""

    json_dir: Path  # set on the class before serving
    db_path: Path | None  # set on the class before serving

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "" or path == "/":
            self._serve_index()
        elif path == "/report/album-issues":
            qs = parse_qs(parsed.query)
            dir_list = qs.get("dir", [])
            if dir_list:
                self._serve_album_issues(dir_list[0])
            else:
                self._send_error(
                    400,
                    "Missing 'dir' query parameter. Use: /report/album-issues?dir=ALBUM_DIR",
                )
        elif path.startswith("/report/"):
            self._serve_report(path[8:])
        elif path.startswith("/api/") and path.endswith(".json"):
            self._serve_json(path[5:-5])  # strip /api/ and .json
        else:
            self._send_error(404, "Not Found")

    def _serve_index(self) -> None:
        """Render the dashboard page."""
        reports: list[dict[str, Any]] = []
        for name in REPORT_NAMES:
            envelope = _read_envelope(self.json_dir, name)
            if envelope:
                filters_raw = envelope.get("filters", {})
                filters_display = build_filter_string(
                    artist=filters_raw.get("artist"),
                    album=filters_raw.get("album"),
                    min_severity=filters_raw.get("min_severity"),
                )
                reports.append(
                    {
                        "name": name,
                        "title": REPORT_TITLES.get(name, name),
                        "available": True,
                        "generated_at": envelope.get("generated_at", ""),
                        "filters_display": filters_display or "",
                    }
                )
            else:
                reports.append(
                    {
                        "name": name,
                        "title": REPORT_TITLES.get(name, name),
                        "available": False,
                        "generated_at": None,
                        "filters_display": "",
                    }
                )

        template = _load_template("index.html")
        html = template.render(
            json_dir=str(self.json_dir),
            reports=reports,
        )
        self._send_html(html)

    def _serve_report(self, report_name: str) -> None:
        """Render a single report as HTML."""
        if report_name not in REPORT_DATA_CLASSES:
            self._send_error(404, f"Unknown report: {report_name}")
            return

        envelope = _read_envelope(self.json_dir, report_name)
        if envelope is None:
            self._send_not_generated(report_name)
            return

        cls = REPORT_DATA_CLASSES[report_name]
        try:
            data = from_dict(cls, envelope["data"])
        except Exception as exc:
            self._send_error(500, f"Failed to deserialize {report_name}: {exc}")
            return

        builder = SECTION_BUILDERS.get(report_name)
        if builder is None:
            self._send_error(500, f"No section builder for {report_name}")
            return

        sections = builder(data)
        title = REPORT_TITLES.get(report_name, report_name)

        filters_raw = envelope.get("filters", {})
        filters_str = build_filter_string(
            artist=filters_raw.get("artist"),
            album=filters_raw.get("album"),
            min_severity=filters_raw.get("min_severity"),
        )

        template = _load_template("report.html")
        html = template.render(
            title=title,
            generated_at=envelope.get("generated_at", ""),
            filters=filters_str or "",
            sections=sections,
            nav_links=_build_nav_links(report_name),
        )
        self._send_html(html)

    def _serve_album_issues(self, album_dir: str) -> None:
        """Render the album issue detail page by querying the database."""
        if not self.db_path or not self.db_path.is_file():
            self._send_error(
                500, "Database not available. Start the server with --config."
            )
            return

        try:
            from storeroon.db import connect
            from storeroon.reports.queries.issues import album_detail

            conn = connect(self.db_path, read_only=True)
            data = album_detail(conn, album_dir)
            conn.close()
        except Exception as exc:
            self._send_error(500, f"Failed to query album issues: {exc}")
            return

        if data is None:
            self._send_error(404, f"No issues found for album directory: {album_dir}")
            return

        sections = build_album_issues_sections(data)
        title = f"Issues — {data.artist} — {data.album}"

        template = _load_template("report.html")
        html = template.render(
            title=title,
            generated_at="",
            filters="",
            sections=sections,
            nav_links=_build_nav_links(""),
        )
        self._send_html(html)

    def _serve_json(self, report_name: str) -> None:
        """Serve the raw JSON file."""
        filepath = self.json_dir / f"{report_name}.json"
        if not filepath.is_file():
            self._send_error(404, f"JSON file not found: {report_name}.json")
            return
        try:
            content = filepath.read_bytes()
        except OSError as exc:
            self._send_error(500, f"Cannot read file: {exc}")
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_html(self, html: str) -> None:
        content = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_error(self, code: int, message: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        html = f"<html><body><h1>{code}</h1><p>{message}</p></body></html>"
        self.wfile.write(html.encode("utf-8"))

    def _send_not_generated(self, report_name: str) -> None:
        title = REPORT_TITLES.get(report_name, report_name)
        html = (
            f"<html><body>"
            f"<h1>Report not generated</h1>"
            f"<p>The <strong>{title}</strong> report has not been generated yet.</p>"
            f"<p>Run: <code>storeroon report {report_name.replace('_', '-')} --output json</code></p>"
            f'<p><a href="/">Back to dashboard</a></p>'
            f"</body></html>"
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        """Route HTTP access logs through the storeroon logger."""
        log.info(format, *args)


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------


def run_server(json_dir: Path, port: int = 8080, db_path: Path | None = None) -> None:
    """Start the HTTP server and block until interrupted."""
    StoreroonHandler.json_dir = json_dir
    StoreroonHandler.db_path = db_path
    server = HTTPServer(("127.0.0.1", port), StoreroonHandler)
    print(f"Serving storeroon reports at http://127.0.0.1:{port}/")
    print(f"JSON directory: {json_dir}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()
