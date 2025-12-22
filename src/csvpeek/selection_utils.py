"""Selection utilities for csvpeek (DuckDB backend)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from csvpeek.csvpeek import CSVViewerApp


def get_single_cell_value(app: "CSVViewerApp") -> str:
    """Return the current cell value as a string."""
    if not app.cached_rows:
        return ""
    row = app.cached_rows[app.cursor_row]
    cell = row[app.cursor_col] if app.cursor_col < len(row) else None
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


def create_selected_dataframe(app: "CSVViewerApp") -> list[list]:
    """Return selected rows for CSV export."""
    row_start, row_end, col_start, col_end = get_selection_bounds(app)
    if not app.cached_rows:
        return []
    selected_rows = [
        row[col_start : col_end + 1] for row in app.cached_rows[row_start : row_end + 1]
    ]
    return selected_rows


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
