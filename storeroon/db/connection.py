"""
Database connection factory for storeroon.

Every connection returned by ``connect()`` has:
- WAL journal mode enabled  (better concurrent-read performance)
- Foreign-key enforcement turned on  (off by default in SQLite)
- A busy timeout so writers don't immediately fail under contention
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

_BUSY_TIMEOUT_MS = 5000


def connect(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open (or create) a SQLite database and apply storeroon pragmas.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Parent directories are created
        automatically if they don't exist.
    read_only:
        When *True*, open the database in read-only mode via a ``file:`` URI.
        Useful for reporting scripts that should never accidentally mutate
        the database.

    Returns
    -------
    sqlite3.Connection
        A connection with WAL mode, foreign keys, and a busy timeout
        already configured.
    """
    path = Path(db_path).expanduser()

    if read_only:
        # SQLite URI mode for true read-only access.
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        # Ensure the parent directory exists so that SQLite can create the
        # database file (and its WAL / SHM sidecars).
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))

    # Return rows as sqlite3.Row so callers can access columns by name.
    conn.row_factory = sqlite3.Row

    _apply_pragmas(conn)

    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Configure SQLite pragmas on an open connection."""
    conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    # Synchronous NORMAL is safe with WAL and significantly faster than FULL.
    conn.execute("PRAGMA synchronous = NORMAL;")
