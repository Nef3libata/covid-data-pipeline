from __future__ import annotations

import logging
from typing import Iterable, List

import pandas as pd

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    pass


def assert_no_negative(df: pd.DataFrame, cols: Iterable[str]) -> None:
    negatives = {}
    for c in cols:
        if (df[c] < 0).any(skipna=True):
            negatives[c] = int((df[c] < 0).sum())

    if negatives:
        msg = f"Negative values found: {negatives}"
        logger.error(msg)
        raise DataValidationError(msg)


def check_required_non_null(
    df: pd.DataFrame,
    required: Iterable[str],
) -> List[bool]:
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise DataValidationError(f"Missing required columns: {missing_cols}")

    rows_with_nulls = df[list(required)].isna().any(axis=1)
    n_bad = int(rows_with_nulls.sum())

    if n_bad:
        logger.info("Found %d rows with nulls in required columns", n_bad)

    return rows_with_nulls.tolist()
