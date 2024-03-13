"""There are some utility functions for SQL operations."""

import sqlite3
from pathlib import Path
from typing import Any

from spider import logger


def execute_sql_command(sql: str, path: Path, values: list | None = None) -> Any:  # noqa: ANN401
    """Execute a SQL command on the database."""
    try:
        with sqlite3.connect(path) as connect:
            cursor = connect.cursor()
            if values:
                if sql.strip().upper().startswith("INSERT") and isinstance(
                    values, list
                ):
                    cursor.executemany(sql, values)
                    logger.info(f"Insert {len(values)} records")
                else:
                    cursor.execute(sql, values)
                    logger.info(f"Execute SQL command: {sql[:20]}.")
            else:
                cursor.execute(sql)
                logger.info(f"Execute SQL command: {sql[:20]}.")

            # Fetch results for SELECT queries, otherwise return None for now
            if sql.strip().upper().startswith("SELECT"):
                return cursor.fetchall()

    except sqlite3.IntegrityError:
        logger.warning("SQL integrity error, not unique value")

    except sqlite3.Error as e:
        logger.warning(f"SQL execution failure of SQLite: {e!s}")
        raise
