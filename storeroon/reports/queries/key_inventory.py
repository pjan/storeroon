"""
storeroon.reports.queries.key_inventory — Key inventory report.

Lists all tag keys found across all files with their classification.

Public API:
    full_data(conn, tags_config) -> KeyInventoryFullData
"""

from __future__ import annotations

import sqlite3

from storeroon.config import TagsConfig
from storeroon.reports.models import KeyInventoryFullData, TagInventoryRow
from storeroon.reports.utils import TOTAL_OK_FILES_SQL, safe_pct

_FULL_INVENTORY_SQL = """
SELECT
    tag_key_upper,
    COUNT(DISTINCT file_id) AS file_count
FROM raw_tags
WHERE file_id IN (SELECT id FROM files WHERE status = 'ok')
GROUP BY tag_key_upper
ORDER BY file_count DESC
"""


def full_data(
    conn: sqlite3.Connection,
    tags_config: TagsConfig,
) -> KeyInventoryFullData:
    """Return the key inventory."""
    row = conn.execute(TOTAL_OK_FILES_SQL).fetchone()
    total_files = row[0] if row else 0

    rows = conn.execute(_FULL_INVENTORY_SQL).fetchall()
    inventory: list[TagInventoryRow] = []

    for r in rows:
        key_upper = r["tag_key_upper"]
        file_count = r["file_count"]
        classification = tags_config.classify(key_upper)

        inventory.append(TagInventoryRow(
            tag_key_upper=key_upper,
            file_count=file_count,
            coverage_pct=safe_pct(file_count, total_files),
            classification=classification,
        ))

    return KeyInventoryFullData(total_files=total_files, inventory=inventory)
