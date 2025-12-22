"""Selection utilities for csvpeek."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    from csvpeek.csvpeek import CSVViewerApp


def get_single_cell_value(app: "CSVViewerApp") -> str:
    """Return the current cell value as a string."""
    if app.cached_page_df is None:
        return ""
    cell = app.cached_page_df.row(app.cursor_row)[app.cursor_col]
    return "" if cell is None else str(cell)


def get_selection_bounds(app: "CSVViewerApp") -> tuple[int, int, int, int]:
    """Get selection bounds as (row_start, row_end, col_start, col_end)."""
    if app.selection_start_row is None or app.selection_end_row is None:
        return app.cursor_row, app.cursor_row, app.cursor_col, app.cursor_col
    row_start = min(app.selection_start_row, app.selection_end_row)
    row_end = max(app.selection_start_row, app.selection_end_row)
    col_start = min(app.selection_start_col, app.selection_end_col)
    col_end = max(app.selection_start_col, app.selection_end_col)
    return row_start, row_end, col_start, col_end


def create_selected_dataframe(app: "CSVViewerApp") -> pl.DataFrame:
    """Create a DataFrame containing only the selected cells."""
    row_start, row_end, col_start, col_end = get_selection_bounds(app)
    return app.cached_page_df.slice(row_start, row_end - row_start + 1).select(
        app.cached_page_df.columns[col_start : col_end + 1]
    )


def clear_selection_and_update(app: "CSVViewerApp") -> None:
    """Clear selection and refresh visuals."""
    app.selection_active = False
    app.selection_start_row = None
    app.selection_start_col = None
    app.selection_end_row = None
    app.selection_end_col = None
    app._refresh_rows()


def get_selection_dimensions(
    app: "CSVViewerApp", as_bounds: bool = False
) -> tuple[int, int] | tuple[int, int, int, int]:
    """Get selection dimensions or bounds.

    If `as_bounds` is True, returns (row_start, row_end, col_start, col_end).
    Otherwise returns (num_rows, num_cols).
    """

    row_start, row_end, col_start, col_end = get_selection_bounds(app)
    if as_bounds:
        return row_start, row_end, col_start, col_end
    return row_end - row_start + 1, col_end - col_start + 1
