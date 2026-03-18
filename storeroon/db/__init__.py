"""
storeroon.db — database layer.

Convenience re-exports so callers can write::

    from storeroon.db import connect, migrate
"""

from storeroon.db.connection import connect
from storeroon.db.migrations import MigrationError, migrate

__all__ = ["MigrationError", "connect", "migrate"]
