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

            # Check if column is string type
            col_dtype = df_sample[col].dtype
            is_string = col_dtype in (pl.Utf8, pl.String) or str(col_dtype).lower() in (
                "utf8",
                "string",
            )

            if is_string:
                # Case-insensitive literal substring search
                # We escape the filter_value and lowercase both sides
                escaped_filter = re.escape(filter_value.lower())
                filtered = filtered.filter(
                    pl.col(col).str.to_lowercase().str.contains(escaped_filter)
                )
            else:
                # For numeric columns, try exact match or range
                if "-" in filter_value and not filter_value.startswith("-"):
                    # Range filter: "10-20"
                    parts = filter_value.split("-")
                    if len(parts) == 2:
                        try:
                            min_val = float(parts[0].strip())
                            max_val = float(parts[1].strip())
                            filtered = filtered.filter(
                                (pl.col(col) >= min_val) & (pl.col(col) <= max_val)
                            )
                        except ValueError:
                            pass
                else:
                    # Exact match for numbers
                    try:
                        num_val = float(filter_value)
                        filtered = filtered.filter(pl.col(col) == num_val)
                    except ValueError:
                        # If can't convert to number, try literal string contains
                        filtered = filtered.filter(
                            pl.col(col)
                            .cast(pl.Utf8)
                            .str.contains(filter_value, literal=True)
                        )
        except Exception:
            # If filter fails, skip this column
            pass

    return filtered
