"""Filter utilities for CSV data."""

import re

import polars as pl


def apply_filters_to_lazyframe(
    lazy_df: pl.LazyFrame, df_sample: pl.DataFrame, filters: dict[str, str]
) -> pl.LazyFrame:
    """
    Apply filters to a LazyFrame.

    Args:
        lazy_df: The lazy frame to filter
        df_sample: A sample DataFrame with schema information
        filters: Dictionary mapping column names to filter values

    Returns:
        Filtered LazyFrame
    """
    filtered = lazy_df

    # Apply each column filter
    for col, filter_value in filters.items():
        filter_value = filter_value.strip()

        if not filter_value:
            continue

        try:
            # Check if column exists
            if col not in df_sample.columns:
                continue

            # Case-insensitive literal substring search for all columns
            # All columns are treated as strings
            escaped_filter = re.escape(filter_value.lower())
            filtered = filtered.filter(
                pl.col(col).str.to_lowercase().str.contains(escaped_filter)
            )
        except Exception:
            # If filter fails, skip this column
            pass

    return filtered
