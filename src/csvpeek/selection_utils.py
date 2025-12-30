"""Selection utilities for csvpeek (DuckDB backend)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence


class Selection:
    """Tracks an anchored selection in absolute row/column coordinates."""

    def __init__(self) -> None:
        self.active = False
        self.anchor_row: int | None = None
        self.anchor_col: int | None = None
        self.focus_row: int | None = None
        self.focus_col: int | None = None

    def clear(self) -> None:
        self.active = False
        self.anchor_row = None
        self.anchor_col = None
        self.focus_row = None
        self.focus_col = None

    def start(self, row: int, col: int) -> None:
        self.active = True
        self.anchor_row = row
        self.anchor_col = col
        self.focus_row = row
        self.focus_col = col

    def extend(self, row: int, col: int) -> None:
        if not self.active or self.anchor_row is None or self.anchor_col is None:
            self.start(row, col)
            return
        self.focus_row = row
        self.focus_col = col

    def bounds(self, fallback_row: int, fallback_col: int) -> tuple[int, int, int, int]:
        """Return (row_start, row_end, col_start, col_end).

        If inactive, falls back to the provided cursor position.
        """

        if not self.active or None in (
            self.anchor_row,
            self.anchor_col,
            self.focus_row,
            self.focus_col,
        ):
            return fallback_row, fallback_row, fallback_col, fallback_col

        row_start = min(self.anchor_row, self.focus_row)
        row_end = max(self.anchor_row, self.focus_row)
        col_start = min(self.anchor_col, self.focus_col)
        col_end = max(self.anchor_col, self.focus_col)
        return row_start, row_end, col_start, col_end

    def dimensions(self, fallback_row: int, fallback_col: int) -> tuple[int, int]:
        row_start, row_end, col_start, col_end = self.bounds(fallback_row, fallback_col)
        return row_end - row_start + 1, col_end - col_start + 1

    def contains(
        self, row: int, col: int, *, fallback_row: int, fallback_col: int
    ) -> bool:
        row_start, row_end, col_start, col_end = self.bounds(fallback_row, fallback_col)
        return row_start <= row <= row_end and col_start <= col <= col_end


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

    cursor_abs_row = app.row_offset + app.cursor_row
    return app.selection.bounds(cursor_abs_row, app.cursor_col)


def create_selected_dataframe(app: "CSVViewerApp") -> Sequence[Sequence]:
    """Return selected rows for CSV export."""
    if not app.db:
        return []

    row_start, row_end, col_start, col_end = get_selection_bounds(app)
    fetch_count = row_end - row_start + 1

    rows = app.db.fetch_rows(
        app.filter_where,
        list(app.filter_params),
        app.sorted_column,
        app.sorted_descending,
        fetch_count,
        row_start,
    )

    return [row[col_start : col_end + 1] for row in rows]


def clear_selection_and_update(app: "CSVViewerApp") -> None:
    """Clear selection and refresh visuals."""
    app.selection.clear()
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
