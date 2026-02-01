"""Entry point for automation system."""

from __future__ import annotations

import logging
import signal
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

from db import DB


def load_config() -> dict[str, Any]:
    """Load configuration for the automation system."""
    return {}


def init_db(db_path: str) -> Optional[sqlite3.Connection]:
    """Initialize the SQLite database safely."""
    try:
        connection = sqlite3.connect(db_path)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_updated TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute("INSERT OR IGNORE INTO system_state (id) VALUES (1)")
        connection.commit()
        return connection
    except sqlite3.Error:
        logging.getLogger(__name__).exception("Failed to initialize SQLite database.")
        return None


def setup_logging() -> None:
    """Configure application logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def _handle_shutdown(signum: int, _frame: object) -> None:
    logging.getLogger(__name__).info("Received signal %s. Shutting down.", signum)


def main() -> None:
    """Main entry point for the automation system."""
    setup_logging()
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    _ = load_config()
    db_path = Path(__file__).resolve().parent / "ig.sqlite"
    db = DB.initialize(str(db_path))
    if db is None:
        logging.getLogger(__name__).error("Startup aborted due to database failure.")
        sys.exit(1)
    connection = init_db("automation.db")
    if connection is None:
        logging.getLogger(__name__).error("Startup aborted due to database failure.")
        return
    # TODO: implement automation system logic.


if __name__ == "__main__":
    main()
