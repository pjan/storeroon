"""
storeroon.scanner — collection scanning pipeline.

Convenience re-exports so callers can write::

    from storeroon.scanner import walk_collection, import_batch
"""

from storeroon.scanner.importer import ImportStats, import_batch, import_file
from storeroon.scanner.walker import DiscoveredFile, FileKind, walk_collection

__all__ = [
    "DiscoveredFile",
    "FileKind",
    "ImportStats",
    "import_batch",
    "import_file",
    "walk_collection",
]
