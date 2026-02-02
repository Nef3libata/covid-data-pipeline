import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_to_sqlite(
    df: pd.DataFrame,
    db_path: Path | str = Path("data/processed/covid.db"),
    table_name: str = "covid_daily",
) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Loading dataset into SQLite database at %s", db_path)

    with sqlite3.connect(db_path) as conn:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False,
        )

    logger.info(
        "Successfully loaded %d rows into table '%s'",
        len(df),
        table_name,
    )


def create_country_daily_summary(
    db_path: Path | str = Path("data/processed/covid.db"),
    source_table: str = "covid_daily",
    summary_table: str = "country_daily_summary",
) -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS {summary_table} AS
    SELECT
        countries_and_territories,
        date,
        SUM(cases)  AS total_cases,
        SUM(deaths) AS total_deaths
    FROM {source_table}
    GROUP BY
        countries_and_territories,
        date
    ;
    """

    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {summary_table}")
        conn.execute(query)

    logger.info("Created SQL summary table '%s'", summary_table)
