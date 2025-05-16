"""Example transformations for the Daily Excel Reports application."""

import numpy as np
import pandas as pd

from .transformations import (
    ColumnTransformation,
    ComputedColumnTransformation,
    RowTransformation,
    TransformationPipeline,
)

# Column transformations


def clean_string(series: pd.Series) -> pd.Series:
    """Clean string data by stripping spaces and converting to lowercase.

    Args:
        series: Series to clean

    Returns:
        Cleaned series
    """
    if pd.api.types.is_string_dtype(series):
        return series.str.strip().str.lower()
    return series


def remove_outliers(series: pd.Series, std_dev: float = 3.0) -> pd.Series:
    """Remove outliers beyond specified standard deviations from the mean.

    Args:
        series: Series to process
        std_dev: Number of standard deviations to use as threshold

    Returns:
        Series with outliers replaced by NaN
    """
    if pd.api.types.is_numeric_dtype(series):
        mean = series.mean()
        std = series.std()
        threshold = std_dev * std
        return series.where((series >= mean - threshold) & (series <= mean + threshold))
    return series


def format_date(series: pd.Series, date_format: str = "%Y-%m-%d") -> pd.Series:
    """Format dates in a consistent way.

    Args:
        series: Series containing date values
        date_format: Output date format

    Returns:
        Formatted date series
    """
    if pd.api.types.is_datetime64_dtype(series):
        return series.dt.strftime(date_format)

    # Try to convert to datetime first
    try:
        return pd.to_datetime(series).dt.strftime(date_format)
    except Exception:  # noqa: BLE001
        return series


# Row transformations


def filter_incomplete_rows(df: pd.DataFrame) -> pd.Series:
    """Filter for rows with incomplete data.

    Args:
        df: DataFrame to filter

    Returns:
        Boolean mask for rows to keep (True = keep, False = filter out)
    """
    return df.notna().all(axis=1)


def filter_by_date_range(
    df: pd.DataFrame,
    date_col: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Filter rows by date range.

    Args:
        df: DataFrame to filter
        date_col: Name of date column
        start_date: Start date (inclusive)
        end_date: End date (inclusive)

    Returns:
        Filtered DataFrame
    """
    result = df.copy()

    if date_col in result.columns:
        # Convert column to datetime if needed
        if not pd.api.types.is_datetime64_dtype(result[date_col]):
            result[date_col] = pd.to_datetime(result[date_col], errors="coerce")

        # Apply date filters
        if start_date is not None:
            start = pd.to_datetime(start_date)
            result = result[result[date_col] >= start]

        if end_date is not None:
            end = pd.to_datetime(end_date)
            result = result[result[date_col] <= end]

    return result


# Computed columns


def compute_moving_average(
    df: pd.DataFrame,
    value_col: str,
    window: int = 3,
) -> pd.Series:
    """Compute moving average for a numeric column.

    Args:
        df: Input DataFrame
        value_col: Column to compute moving average for
        window: Window size for moving average

    Returns:
        Series with moving average values
    """
    if value_col in df.columns and pd.api.types.is_numeric_dtype(df[value_col]):
        return df[value_col].rolling(window=window).mean()
    return pd.Series(np.nan, index=df.index)


def compute_percentage_change(df: pd.DataFrame, value_col: str) -> pd.Series:
    """Compute percentage change between consecutive values.

    Args:
        df: Input DataFrame
        value_col: Column to compute percentage change for

    Returns:
        Series with percentage change values
    """
    if value_col in df.columns and pd.api.types.is_numeric_dtype(df[value_col]):
        return df[value_col].pct_change() * 100
    return pd.Series(np.nan, index=df.index)


def compute_year_quarter(df: pd.DataFrame, date_col: str) -> pd.Series:
    """Compute year-quarter from date column.

    Args:
        df: Input DataFrame
        date_col: Date column name

    Returns:
        Series with year-quarter values (e.g., "2023-Q1")
    """
    if date_col in df.columns:
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_dtype(df[date_col]):
            dates = pd.to_datetime(df[date_col], errors="coerce")
        else:
            dates = df[date_col]

        # Extract year and quarter
        year = dates.dt.year
        quarter = dates.dt.quarter

        # Format as "YYYY-Q#"
        return year.astype(str) + "-Q" + quarter.astype(str)

    return pd.Series("", index=df.index)


# Example pipeline creation function


def create_financial_report_pipeline() -> TransformationPipeline:
    """Create a transformation pipeline for financial reports.

    Returns:
        Configured TransformationPipeline
    """
    pipeline = TransformationPipeline()

    # Clean string columns
    pipeline.add_transformation(
        ColumnTransformation(
            "Clean Strings",
            ["Category", "Description"],
            clean_string,
        ),
    )

    # Format date columns
    pipeline.add_transformation(
        ColumnTransformation(
            "Format Dates",
            ["Date", "TransactionDate"],
            lambda s: format_date(s, "%Y-%m-%d"),
        ),
    )

    # Remove outliers from numeric columns
    pipeline.add_transformation(
        ColumnTransformation(
            "Remove Outliers",
            ["Amount", "Balance"],
            lambda s: remove_outliers(s, 3.0),
        ),
    )

    # Filter incomplete rows
    pipeline.add_transformation(
        RowTransformation(
            "Filter Incomplete",
            lambda df: df,
            filter_func=filter_incomplete_rows,
        ),
    )

    # Add computed columns
    pipeline.add_transformation(
        ComputedColumnTransformation(
            "Add Computed Columns",
            {
                "Amount_MA_3": lambda df: compute_moving_average(df, "Amount", 3),
                "Balance_Change_Pct": lambda df: compute_percentage_change(
                    df,
                    "Balance",
                ),
                "YearQuarter": lambda df: compute_year_quarter(df, "Date"),
            },
        ),
    )

    return pipeline
