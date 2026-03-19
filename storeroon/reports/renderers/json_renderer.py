"""
storeroon.reports.renderers.json_renderer — JSON output for reports.

Serialises report *FullData models to a single JSON file per report with
a stable filename (no timestamp) and an envelope containing metadata.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from storeroon.reports.utils import now_iso


def write_report(
    output_dir: Path,
    report_name: str,
    data: Any,
    filters: dict[str, str | None] | None = None,
) -> Path:
    """Serialise a report *FullData model to ``output_dir/{report_name}.json``.

    The JSON envelope contains:
    - ``report`` — the report identifier
    - ``generated_at`` — ISO-8601 timestamp
    - ``filters`` — active filter values (or all-null)
    - ``data`` — the complete dataclass tree via ``dataclasses.asdict``

    Returns the written file path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    envelope: dict[str, Any] = {
        "report": report_name,
        "generated_at": now_iso(),
        "filters": filters or {},
        "data": asdict(data),
    }

    filepath = output_dir / f"{report_name}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False, default=str)

    return filepath
