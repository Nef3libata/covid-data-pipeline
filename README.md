# COVID Data Engineering Pipeline

A small, production-style data pipeline that takes a public COVID-19 dataset, cleans and validates it, and produces a reliable, ready-to-use dataset for analysis.

This project focuses on showing how a real-world data pipeline is structured with clear steps, built-in data quality checks, and simple, visible logging so you can follow what happens at each stage.

## Project overview

This pipeline takes a raw COVID-19 CSV file, cleans and validates it, and produces a ready-to-use dataset for analysis.

It focuses on practical data engineering patterns such as clear pipeline stages, strong data quality checks, and transparent execution through logging.

The pipeline performs the following steps:

- reads a raw CSV dataset
- standardizes column names and schema
- validates data quality (null checks, date validation, non-negative constraints)
- handles known corrections in the source data
- computes a small set of derived metrics
- writes a cleaned dataset in Parquet format

This project is meant to demonstrate engineering practices, not to build a full analytics system.

## Dataset

The pipeline uses the public dataset published by the European Centre for Disease Prevention and Control (ECDC):

**Data on the daily number of new reported COVID-19 cases and deaths by EU/EEA country**

[https://www.ecdc.europa.eu/en/publications-data/data-daily-new-cases-covid-19-eueea-country](https://www.ecdc.europa.eu/en/publications-data/data-daily-new-cases-covid-19-eueea-country)

The expected CSV schema is:

```
dateRep,day,month,year,cases,deaths,countriesAndTerritories,geoId,countryterritoryCode,popData2020,continentExp
```

In the original dataset, negative daily values for `cases` and `deaths` may appear due to retrospective reporting corrections.

## Project structure

```
.
├── src
│   └── covid_pipeline
│       ├── ingest.py
│       ├── transform.py
│       └── main.py
├── data
│   ├── raw
│   │   └── data.csv
│   └── processed
│       └── covid_clean.parquet
├── tests
│   └── test_transform.py
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

## Pipeline steps

### 1. Ingestion

Reads the raw CSV file and logs basic information, including:

- number of rows
- number of columns
- column names

### 2. Transformation and validation

The transformation step:

- standardizes column names
- parses the reporting date and validates it
- enforces appropriate data types for numeric fields
- drops rows with missing values in required fields
- removes duplicate records per country and date
- clips negative daily values for cases and deaths (to account for official data corrections)
- validates that population values are non-negative
- computes derived metrics:
  - cases per 100,000 population
  - deaths per 100,000 population

All checks and corrections are logged.

### 3. Output

The cleaned dataset is written to:

```
data/processed/covid_clean.parquet
```

## Data quality rules

The following rules are enforced:

- required fields must not be null:
  - date
  - countries and territories
  - cases
  - deaths
  - population

- dates must be parseable and consistent with the provided day, month, and year columns
- population must be non-negative
- duplicate records per (country, date) are removed
- negative daily values for cases and deaths are treated as reporting corrections and clipped to zero

## Requirements

- Python 3.11
- pandas
- pyarrow

## Local execution

Place the raw dataset in:

```
data/raw/data.csv
```

Install dependencies:

```bash
pip install -e .
```

Run the pipeline:

```bash
python -m covid_pipeline.main
```

After a successful run, the output will be available at:

```
data/processed/covid_clean.parquet
```

## Running tests

```bash
pytest
```

## Running with Docker

Build the image:

```bash
docker compose build
```

Run the pipeline:

```bash
docker compose run --rm pipeline
```

The generated Parquet file will be written to the local `data/processed` directory through the mounted volume.

## Design goals

This project demonstrates:

- modular ETL components with clear responsibilities
- explicit and observable data quality checks
- reproducible execution in both local and containerized environments
- a clean, maintainable project structure suitable for production-style data pipelines
