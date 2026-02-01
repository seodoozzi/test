"""Entry point for automation system."""

from __future__ import annotations

import atexit
import logging
import signal
import sys
from pathlib import Path
from typing import Any

from db import DB


def load_config() -> dict[str, Any]:
    """Load configuration for the automation system."""
    return {}


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
    db_path = Path(__file__).with_name("ig.sqlite")
    db = DB.initialize(str(db_path))
    if db is None:
        logging.getLogger(__name__).error("Startup aborted due to database failure.")
        sys.exit(1)
    atexit.register(db.close)
    # TODO: implement automation system logic.


if __name__ == "__main__":
    main()
