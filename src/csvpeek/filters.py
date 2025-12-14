"""Filter utilities for CSV data."""

import re

import polars as pl


def apply_filters_to_lazyframe(
    lazy_df: pl.LazyFrame, df_sample: pl.DataFrame, filters: dict[str, str]
) -> pl.LazyFrame:
    """
    Apply filters to a LazyFrame.

    Filters starting with '/' are treated as case-insensitive regex patterns.
    Other filters are treated as literal substring searches.

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

            # Detect regex mode (starts with /)
            if filter_value.startswith("/"):
                # Regex mode: remove leading / and use as pattern
                pattern = filter_value[1:]
                if not pattern:  # Empty pattern after /
                    continue

                # Validate regex pattern
                try:
                    re.compile(pattern, re.IGNORECASE)
                except re.error:
                    # Invalid regex, skip this filter
                    continue

                # Use Polars regex with case-insensitive flag
                # Note: (?i) makes the pattern case-insensitive
                filtered = filtered.filter(pl.col(col).str.contains(f"(?i){pattern}"))
            else:
                # Literal mode: escape and do case-insensitive substring search
                escaped_filter = re.escape(filter_value.lower())
                filtered = filtered.filter(
                    pl.col(col).str.to_lowercase().str.contains(escaped_filter)
                )
        except Exception:
            # If filter fails, skip this column
            pass

    return filtered
