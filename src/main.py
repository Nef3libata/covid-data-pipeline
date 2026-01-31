import logging
from ingest import ingest_covid_data


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main():
    df = ingest_covid_data()


if __name__ == "__main__":
    setup_logging()
    main()
