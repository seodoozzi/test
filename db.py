"""SQLite database layer for the automation system."""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Optional


class DB:
    """SQLite database wrapper with minimal helpers."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Open the SQLite connection and apply safe pragmas."""
        connection = sqlite3.connect(self._path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        self._connection = connection

    def close(self) -> None:
        """Close the database connection if it exists."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def init_schema(self) -> None:
        """Create required tables if they do not exist."""
        connection = self._require_connection()
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                level TEXT NOT NULL,
                msg TEXT NOT NULL
            )
            """
        )
        connection.commit()

    def set_meta(self, key: str, value: str) -> None:
        """Set a metadata value."""
        connection = self._require_connection()
        connection.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        connection.commit()

    def get_meta(self, key: str, default: str = "") -> str:
        """Get a metadata value with a default fallback."""
        connection = self._require_connection()
        cursor = connection.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row is None:
            return default
        return str(row[0])

    def log_event(self, level: str, msg: str) -> None:
        """Record an event in the log table."""
        connection = self._require_connection()
        connection.execute(
            "INSERT INTO log (ts, level, msg) VALUES (?, ?, ?)",
            (time.time(), level, msg),
        )
        connection.commit()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not initialized.")
        return self._connection

    @classmethod
    def initialize(cls, path: str) -> Optional["DB"]:
        """Create and initialize the database with safe error handling."""
        db = cls(path)
        try:
            db.connect()
            db.init_schema()
        except sqlite3.Error:
            logging.getLogger(__name__).exception("Failed to initialize database.")
            db.close()
            return None
        return db
