from __future__ import annotations

import logging

import pandas as pd

from covid_pipeline.quality import (
    DataValidationError,
    assert_no_negative,
    check_required_non_null,
)


logger = logging.getLogger(__name__)


def _lower_snake(col: str) -> str:
    return col.strip().lower().replace(" ", "_")


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "dateRep": "date_rep",
        "day": "day",
        "month": "month",
        "year": "year",
        "cases": "cases",
        "deaths": "deaths",
        "countriesAndTerritories": "countries_and_territories",
        "geoId": "geo_id",
        "countryterritoryCode": "country_territory_code",
        "popData2020": "pop_data_2020",
        "continentExp": "continent_exp",
    }

    new_cols = {c: mapping.get(c, _lower_snake(c)) for c in df.columns}
    return df.rename(columns=new_cols)


def _to_nullable_int(series: pd.Series, col_name: str) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.isna().any():
        logger.debug("Column %s has %d coercions to NaN", col_name, s.isna().sum())
    return s.astype("Int64")


def transform_covid_data(
    df: pd.DataFrame,
    drop_invalid_rows: bool = True,
    compute_per_100k: bool = True,
) -> pd.DataFrame:

    df = df.copy()

    logger.info(
        "Starting transform on input with %d rows and %d columns",
        len(df),
        df.shape[1],
    )

    df = _rename_columns(df)

    if "date_rep" not in df.columns:
        raise DataValidationError("Expected column `dateRep` / `date_rep` is missing")

    df["date"] = pd.to_datetime(df["date_rep"], dayfirst=True, errors="coerce")

    n_bad_dates = int(df["date"].isna().sum())
    if n_bad_dates:
        msg = f"Found {n_bad_dates} rows with unparseable dates in 'date_rep'."
        logger.error(msg)
        raise DataValidationError(msg)

    for comp in ("day", "month", "year"):
        if comp in df.columns:
            df[comp] = pd.to_numeric(df[comp], errors="coerce").astype("Int64")

    if {"day", "month", "year"}.intersection(df.columns):
        mismatch_mask = (
            (df.get("day").notna() & (df["day"].astype("Int64") != df["date"].dt.day))
            | (
                df.get("month").notna()
                & (df["month"].astype("Int64") != df["date"].dt.month)
            )
            | (
                df.get("year").notna()
                & (df["year"].astype("Int64") != df["date"].dt.year)
            )
        )

        n_mismatch = int(mismatch_mask.sum())
        if n_mismatch:
            logger.warning(
                "Found %d rows where day/month/year differ from parsed date. "
                "Overwriting with parsed date.",
                n_mismatch,
            )

            df.loc[:, "day"] = df["date"].dt.day.astype("Int64")
            df.loc[:, "month"] = df["date"].dt.month.astype("Int64")
            df.loc[:, "year"] = df["date"].dt.year.astype("Int64")

    for col in ("cases", "deaths", "pop_data_2020"):
        if col in df.columns:
            df[col] = _to_nullable_int(df[col], col)
        else:
            logger.error("Required numeric column missing: %s", col)
            raise DataValidationError(f"Missing required numeric column: {col}")

    required = [
        "date",
        "countries_and_territories",
        "cases",
        "deaths",
        "pop_data_2020",
    ]

    rows_with_null = pd.Series(
        check_required_non_null(df, required),
        index=df.index,
    )

    if rows_with_null.any():
        n = int(rows_with_null.sum())
        msg = f"{n} rows have nulls in required fields."

        if drop_invalid_rows:
            logger.info("%s Dropping those rows.", msg)
            df = df.loc[~rows_with_null].reset_index(drop=True)
        else:
            logger.error(msg)
            raise DataValidationError(msg)

    for col in ("cases", "deaths"):
        neg_mask = df[col] < 0
        n = int(neg_mask.sum())

        if n:
            logger.warning(
                "Found %d negative values in %s. Clipping to zero (data corrections).",
                n,
                col,
            )
            df.loc[neg_mask, col] = 0

    assert_no_negative(df, ["pop_data_2020"])

    before = len(df)

    df = df.drop_duplicates(
        subset=["countries_and_territories", "date"],
        keep="first",
    ).reset_index(drop=True)

    after = len(df)

    logger.info(
        "Dropped %d duplicate rows based on (countries_and_territories, date).",
        before - after,
    )

    if compute_per_100k:
        zero_pop_mask = df["pop_data_2020"] == 0

        if zero_pop_mask.any():
            indices = df.index[zero_pop_mask].tolist()
            msg = (
                f"Found {int(zero_pop_mask.sum())} rows with population == 0 "
                f"at indices {indices}."
            )
            logger.error(msg)
            raise DataValidationError(msg)

        df["cases_per_100k"] = (
            df["cases"].astype(float) / df["pop_data_2020"].astype(float)
        ) * 100_000

        df["deaths_per_100k"] = (
            df["deaths"].astype(float) / df["pop_data_2020"].astype(float)
        ) * 100_000

    desired_order = [
        "date",
        "countries_and_territories",
        "geo_id",
        "country_territory_code",
        "continent_exp",
        "cases",
        "deaths",
        "pop_data_2020",
    ]

    if compute_per_100k:
        desired_order += ["cases_per_100k", "deaths_per_100k"]

    for extra in ("day", "month", "year"):
        if extra in df.columns:
            desired_order.append(extra)

    rest = [c for c in df.columns if c not in desired_order]
    final_cols = [c for c in desired_order if c in df.columns] + rest

    df = df[final_cols]

    logger.info(
        "Transformation completed. Output rows: %d, columns: %d",
        len(df),
        df.shape[1],
    )

    return df
