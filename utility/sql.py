"""There are some utility functions for SQL operations."""

import sqlite3
from pathlib import Path
from typing import Any

from spider import logger
from utility.path import JOBOSS_SQLITE_FILE_PATH


def execute_sql_command(
    sql: str, path: str | Path, values: list[Any] | None | dict = None
) -> None | list[tuple]:
    """Execute a SQL command on the database."""
    try:
        with sqlite3.connect(str(path)) as conn:
            cursor = conn.cursor()

            if values is None:
                cursor.execute(sql)
                return (
                    cursor.fetchall()
                    if sql.strip().upper().startswith("SELECT")
                    else None
                )

            if sql.strip().upper().startswith("INSERT") and isinstance(values, list):
                cursor.executemany(sql, values)
                logger.info(f"Inserted {len(values)} records")
            elif sql.strip().upper().startswith("INSERT") and isinstance(values, dict):
                cursor.execute(sql, values)
            else:
                cursor.execute(sql, values)

            return (
                cursor.fetchall() if sql.strip().upper().startswith("SELECT") else None
            )

    except sqlite3.IntegrityError:
        logger.warning("SQL integrity error, not unique value")
    except sqlite3.Error as e:
        logger.warning(f"SQL execution failure of SQLite: {e}")
        raise

    return None


def create_joboss_job_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS joboss (
        job_name TEXT,
        area TEXT,
        salary TEXT,
        edu_exp TEXT,
        company_name TEXT,
        company_tag TEXT,
        skill_tags TEXT,
        job_other_tags TEXT,
        PRIMARY KEY (job_name, company_name, area, salary, skill_tags)
    );
    """
    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)


def create_joboss_max_page_table() -> None:
    """Create the table in the database."""
    sql_table = """
    CREATE TABLE IF NOT EXISTS joboss_max_page (
        keyword TEXT,
        area_code TEXT,
        max_page INTEGER,
        PRIMARY KEY (keyword, area_code)
    );
    """
    execute_sql_command(sql_table, JOBOSS_SQLITE_FILE_PATH)
