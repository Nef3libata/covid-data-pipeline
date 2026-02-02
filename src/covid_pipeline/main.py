import logging
from covid_pipeline.ingest import ingest_covid_data
from covid_pipeline.load import load_to_sqlite, create_country_daily_summary
from covid_pipeline.transform import transform_covid_data
from pathlib import Path


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


logger = logging.getLogger(__name__)


def main():
    df = ingest_covid_data()

    df_clean = transform_covid_data(df)

    out = Path("data/processed")
    out.mkdir(parents=True, exist_ok=True)

    parquet_path = out / "covid_clean.parquet"
    df_clean.to_parquet(parquet_path, index=False)
    logger.info("Wrote cleaned dataset to %s", parquet_path)

    load_to_sqlite(df_clean)
    create_country_daily_summary()


if __name__ == "__main__":
    setup_logging()
    main()
