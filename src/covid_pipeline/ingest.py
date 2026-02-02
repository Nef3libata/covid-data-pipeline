import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def ingest_covid_data(
    file_path: Path | str = Path("data/raw/data.csv"),
) -> pd.DataFrame:

    file_path = Path(file_path)

    logger.info("Starting data ingestion from %s", file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Raw data file not found: {file_path}")

    df = pd.read_csv(file_path)

    logger.info("Ingestion completed successfully")
    logger.info("Row count: %d", len(df))
    logger.info("Column count: %d", df.shape[1])
    logger.info("Columns: %s", list(df.columns))

    return df
