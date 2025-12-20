"""Selection utilities for csvpeek."""

from typing import TYPE_CHECKING

import polars as pl
from textual.widgets import DataTable

if TYPE_CHECKING:
    from csvpeek.csvpeek import CSVViewerApp


def get_single_cell_value(app: "CSVViewerApp") -> str:
    """Get the value of the current cell as a string."""
    table = app.query_one("#data-table", DataTable)
    col_idx = table.cursor_column
    row_idx = table.cursor_row
    cell = app.cached_page_df.row(row_idx)[col_idx]
    return "" if cell is None else str(cell)


def get_selection_bounds(app: "CSVViewerApp") -> tuple[int, int, int, int]:
    """Get selection bounds as (row_start, row_end, col_start, col_end)."""
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
    """Clear selection and update the UI efficiently."""
    app.selection_active = False
    # Just update styling instead of rebuilding entire table
    app.update_selection_styling()
    app.update_status()


def get_selection_dimensions(app: "CSVViewerApp") -> tuple[int, int]:
    """Get selection dimensions as (num_rows, num_cols)."""
    row_start, row_end, col_start, col_end = get_selection_bounds(app)
    return row_end - row_start + 1, col_end - col_start + 1
