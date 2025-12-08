#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using Polars and Textual.
"""

from pathlib import Path
from typing import Optional

import polars as pl
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Header, Static

from csvpeek.filter_modal import FilterModal
from csvpeek.filters import apply_filters_to_lazyframe
from csvpeek.styles import APP_CSS
from csvpeek.styling import style_cell


class CSVViewerApp(App):
    """A Textual app to view and filter CSV files."""

    CSS = APP_CSS

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("r", "reset_filters", "Reset Filters"),
        Binding("slash", "show_filters", "Filters"),
        Binding("ctrl+d", "next_page", "Next Page", priority=True),
        Binding("ctrl+u", "prev_page", "Prev Page", priority=True),
        Binding("c", "copy_selection", "Copy", priority=True),
        Binding("left", "move_left", "Move left", priority=True, show=False),
        Binding("right", "move_right", "Move right", priority=True, show=False),
        Binding("up", "move_up", "Move up", priority=True, show=False),
        Binding("down", "move_down", "Move down", priority=True, show=False),
        Binding("shift+left", "select_left", "Select", priority=True),
        Binding("shift+right", "select_right", "Select", priority=True, show=False),
        Binding("shift+up", "select_up", "Select", priority=True, show=False),
        Binding("shift+down", "select_down", "Select", priority=True, show=False),
        Binding("s", "sort_column", "Sort column", priority=True, show=False),
    ]

    PAGE_SIZE = 50  # Number of rows to load per page

    def __init__(self, csv_path: str) -> None:
        super().__init__()
        self.csv_path = Path(csv_path)
        self.df: Optional[pl.DataFrame] = None
        self.lazy_df: Optional[pl.LazyFrame] = None
        self.filtered_lazy: Optional[pl.LazyFrame] = None
        self.current_page: int = 0
        self.total_filtered_rows: int = 0
        self.current_filters: dict[str, str] = {}
        self.cached_page_df: Optional[pl.DataFrame] = None
        # Selection tracking
        self.selection_active: bool = False
        self.selection_start_row: Optional[int] = None
        self.selection_start_col: Optional[int] = None
        self.selection_end_row: Optional[int] = None
        self.selection_end_col: Optional[int] = None
        self.sort_order_descending: bool = False
        self.sorted_column: Optional[str] = None
        self.sorted_descending: bool = False

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()
        yield DataTable(id="data-table")
        yield Static("", id="status")
        yield Footer()

    def on_mount(self) -> None:
        """Load CSV and populate table."""
        self.load_csv()
        self.populate_table()
        self.update_status()

    def load_csv(self) -> None:
        """Load CSV file using Polars."""
        try:
            # Use Polars lazy API for efficient filtering
            self.lazy_df = pl.scan_csv(self.csv_path)
            # Only load a small sample for metadata (columns and dtypes)
            self.df = self.lazy_df.head(1).collect()
            # Start with no filters applied
            self.filtered_lazy = self.lazy_df
            self.total_filtered_rows = self.lazy_df.select(pl.len()).collect().item()
            self.current_page = 0
            self.title = f"csvpeek - {self.csv_path.name}"
        except Exception as e:
            self.exit(message=f"Error loading CSV: {e}")

    def populate_table(self) -> None:
        """Populate the data table with current page of data."""
        if self.filtered_lazy is None:
            return

        table = self.query_one("#data-table", DataTable)

        # Save cursor position
        cursor_row = table.cursor_row
        cursor_col = table.cursor_column

        table.clear(columns=True)

        # Add columns with sort indicators
        if self.df is not None:
            for col in self.df.columns:
                # Add sort arrow if this column is sorted
                if col == self.sorted_column:
                    arrow = " ▼" if self.sorted_descending else " ▲"
                    col_label = f"{col}{arrow}"
                else:
                    col_label = col
                table.add_column(col_label, key=col)

        # Load only the current page of data
        offset = self.current_page * self.PAGE_SIZE
        page_df = self.filtered_lazy.slice(offset, self.PAGE_SIZE).collect()

        # Replace None/null values with empty strings
        page_df = page_df.fill_null("")

        self.cached_page_df = page_df  # Cache for yank operation

        # Pre-compute which columns have active filters
        active_filters = {
            col: val.strip().lower()
            for col, val in self.current_filters.items()
            if val.strip()
        }

        # Calculate selection bounds if selection is active
        sel_row_start = sel_row_end = sel_col_start = sel_col_end = None
        if self.selection_active:
            sel_row_start = min(self.selection_start_row, self.selection_end_row)
            sel_row_end = max(self.selection_start_row, self.selection_end_row)
            sel_col_start = min(self.selection_start_col, self.selection_end_col)
            sel_col_end = max(self.selection_start_col, self.selection_end_col)

        # Add rows with highlighted matches
        for row_idx, row in enumerate(page_df.iter_rows()):
            styled_row = []
            for col_idx, cell in enumerate(row):
                if col_idx >= len(self.df.columns):
                    break  # Safety check
                col_name = self.df.columns[col_idx]
                # Convert to string, handling None explicitly
                cell_str = "" if cell is None else str(cell)

                # Check if this cell is in the selection
                is_selected = (
                    self.selection_active
                    and sel_row_start is not None
                    and sel_col_start is not None
                    and sel_row_start <= row_idx <= sel_row_end
                    and sel_col_start <= col_idx <= sel_col_end
                )

                # Get filter value for this column if it exists
                filter_value = active_filters.get(col_name)

                # Style the cell
                text = style_cell(cell_str, is_selected, filter_value)
                styled_row.append(text)

            table.add_row(*styled_row)

        # Restore cursor position
        if cursor_row is not None and cursor_col is not None:
            try:
                table.move_cursor(row=cursor_row, column=cursor_col, animate=False)
            except Exception:
                pass

    def update_selection_styling(self) -> None:
        """Update cell styling for selection without rebuilding the table."""
        if self.cached_page_df is None or self.df is None:
            return

        table = self.query_one("#data-table", DataTable)

        # Calculate old and new selection bounds
        old_selection = getattr(self, "_last_selection_cells", set())
        new_selection = set()

        if self.selection_active:
            sel_row_start = min(self.selection_start_row, self.selection_end_row)
            sel_row_end = max(self.selection_start_row, self.selection_end_row)
            sel_col_start = min(self.selection_start_col, self.selection_end_col)
            sel_col_end = max(self.selection_start_col, self.selection_end_col)

            for row_idx in range(sel_row_start, sel_row_end + 1):
                for col_idx in range(sel_col_start, sel_col_end + 1):
                    new_selection.add((row_idx, col_idx))

        # Find cells that changed state
        cells_to_update = old_selection.symmetric_difference(new_selection)

        # Pre-compute active filters
        active_filters = {
            col: val.strip().lower()
            for col, val in self.current_filters.items()
            if val.strip()
        }

        # Update only changed cells
        for row_idx, col_idx in cells_to_update:
            if row_idx >= self.cached_page_df.height or col_idx >= len(self.df.columns):
                continue

            cell_value = self.cached_page_df.row(row_idx)[col_idx]
            cell_str = str(cell_value)
            col_name = self.df.columns[col_idx]

            # Check if this cell is selected
            is_selected = (row_idx, col_idx) in new_selection

            # Get filter value for this column if it exists
            filter_value = active_filters.get(col_name)

            # Style the cell
            text = style_cell(cell_str, is_selected, filter_value)

            # Update the cell
            try:
                table.update_cell_at((row_idx, col_idx), text, update_width=False)
            except Exception:
                pass

        # Store current selection for next comparison
        self._last_selection_cells = new_selection

    def apply_filters(self, filters: Optional[dict[str, str]] = None) -> None:
        """Apply filters from the filters dict."""
        if self.lazy_df is None or self.df is None:
            return

        # Update current filters if provided
        if filters is not None:
            self.current_filters = filters
            # Debug: show what we're filtering
            active = {k: v for k, v in filters.items() if v.strip()}
            if active:
                self.notify(f"Filtering: {active}", timeout=2)

        # Apply filters using the standalone function
        self.filtered_lazy = apply_filters_to_lazyframe(
            self.lazy_df, self.df, self.current_filters
        )

        # Re-apply sort if a column is currently sorted
        if self.sorted_column is not None:
            self.filtered_lazy = self.filtered_lazy.sort(
                self.sorted_column, descending=self.sorted_descending, nulls_last=True
            )

        # Update filtered lazy frame and reset to first page
        self.current_page = 0
        self.total_filtered_rows = self.filtered_lazy.select(pl.len()).collect().item()
        self.populate_table()
        self.update_status()

    def action_reset_filters(self) -> None:
        """Reset all filters."""
        self.current_filters = {}
        self.filtered_lazy = self.lazy_df
        self.current_page = 0
        self.sorted_column = None
        self.sorted_descending = False
        if self.lazy_df is not None:
            self.total_filtered_rows = self.lazy_df.select(pl.len()).collect().item()
        self.populate_table()
        self.update_status()

    def action_show_filters(self) -> None:
        """Show the filter modal."""
        if self.df is None:
            return

        def handle_filter_result(result: Optional[dict[str, str]]) -> None:
            """Handle the result from the filter modal."""
            if result is not None:
                self.apply_filters(result)

        self.push_screen(
            FilterModal(self.df.columns.copy(), self.current_filters.copy()),
            handle_filter_result,
        )

    def action_next_page(self) -> None:
        """Load next page of data."""
        max_page = (self.total_filtered_rows - 1) // self.PAGE_SIZE
        if self.current_page < max_page:
            self.current_page += 1
            self.populate_table()
            self.update_status()

    def action_prev_page(self) -> None:
        """Load previous page of data."""
        if self.current_page > 0:
            self.current_page -= 1
            self.populate_table()
            self.update_status()

    def action_move_left(self) -> None:
        """Move cursor left and clear selection."""
        table = self.query_one("#data-table", DataTable)
        if self.selection_active:
            self.selection_active = False
            self.update_selection_styling()
            self.update_status()
        table.action_cursor_left()

    def action_move_right(self) -> None:
        """Move cursor right and clear selection."""
        table = self.query_one("#data-table", DataTable)
        if self.selection_active:
            self.selection_active = False
            self.update_selection_styling()
            self.update_status()
        table.action_cursor_right()

    def action_move_up(self) -> None:
        """Move cursor up and clear selection."""
        table = self.query_one("#data-table", DataTable)
        if self.selection_active:
            self.selection_active = False
            self.update_selection_styling()
            self.update_status()
        table.action_cursor_up()

    def action_move_down(self) -> None:
        """Move cursor down and clear selection."""
        table = self.query_one("#data-table", DataTable)
        if self.selection_active:
            self.selection_active = False
            self.update_selection_styling()
            self.update_status()
        table.action_cursor_down()

    def action_select_left(self) -> None:
        """Start/extend selection and move left."""
        table = self.query_one("#data-table", DataTable)
        if not self.selection_active:
            # Start new selection
            self.selection_active = True
            self.selection_start_row = table.cursor_row
            self.selection_start_col = table.cursor_column
        table.action_cursor_left()
        self.selection_end_row = table.cursor_row
        self.selection_end_col = table.cursor_column
        self.update_selection_styling()
        self.update_status()

    def action_select_right(self) -> None:
        """Start/extend selection and move right."""
        table = self.query_one("#data-table", DataTable)
        if not self.selection_active:
            # Start new selection
            self.selection_active = True
            self.selection_start_row = table.cursor_row
            self.selection_start_col = table.cursor_column
        table.action_cursor_right()
        self.selection_end_row = table.cursor_row
        self.selection_end_col = table.cursor_column
        self.update_selection_styling()
        self.update_status()

    def action_select_up(self) -> None:
        """Start/extend selection and move up."""
        table = self.query_one("#data-table", DataTable)
        if not self.selection_active:
            # Start new selection
            self.selection_active = True
            self.selection_start_row = table.cursor_row
            self.selection_start_col = table.cursor_column
        table.action_cursor_up()
        self.selection_end_row = table.cursor_row
        self.selection_end_col = table.cursor_column
        self.update_selection_styling()
        self.update_status()

    def action_select_down(self) -> None:
        """Start/extend selection and move down."""
        table = self.query_one("#data-table", DataTable)
        if not self.selection_active:
            # Start new selection
            self.selection_active = True
            self.selection_start_row = table.cursor_row
            self.selection_start_col = table.cursor_column
        table.action_cursor_down()
        self.selection_end_row = table.cursor_row
        self.selection_end_col = table.cursor_column
        self.update_selection_styling()
        self.update_status()

    def action_copy_selection(self) -> None:
        """Copy selected cells to clipboard."""
        if self.cached_page_df is None:
            return

        import pyperclip

        if not self.selection_active:
            table = self.query_one("#data-table", DataTable)
            col_idx = table.cursor_column
            row_idx = table.cursor_row
            cell = self.cached_page_df.row(row_idx)[col_idx]
            cell_str = "" if cell is None else str(cell)
            pyperclip.copy(cell_str)
            self.notify(
                f"Copied {cell_str if cell_str else '(empty)'} to clipboard", timeout=2
            )
            self.update_status()
            return

        # Calculate selection bounds
        row_start = min(self.selection_start_row, self.selection_end_row)
        row_end = max(self.selection_start_row, self.selection_end_row)
        col_start = min(self.selection_start_col, self.selection_end_col)
        col_end = max(self.selection_start_col, self.selection_end_col)

        # Build CSV content from selection
        lines = []
        column_headers = [
            str(col_name)
            for col_name in self.cached_page_df.columns[col_start : col_end + 1]
        ]
        lines.append("\t".join(column_headers))
        for row_idx in range(row_start, row_end + 1):
            if row_idx < self.cached_page_df.height:
                row_data = self.cached_page_df.row(row_idx)

                selected_cells = [
                    "" if row_data[col_idx] is None else str(row_data[col_idx])
                    for col_idx in range(col_start, min(col_end + 1, len(row_data)))
                ]
                lines.append("\t".join(selected_cells))

        # Copy to clipboard

        pyperclip.copy("\n".join(lines))

        # Clear selection and notify
        self.selection_active = False
        self.populate_table()
        self.notify(
            f"Copied {len(lines)} rows, {col_end - col_start + 1} columns", timeout=2
        )
        self.update_status()

    def action_sort_column(self) -> None:
        """Sort the current column."""
        if self.df is None or self.filtered_lazy is None:
            return

        table = self.query_one("#data-table", DataTable)
        col_idx = table.cursor_column

        if col_idx is not None and col_idx < len(self.df.columns):
            col_name = self.df.columns[col_idx]

            # Toggle sort direction if sorting same column, otherwise start with ascending
            if self.sorted_column == col_name:
                self.sorted_descending = not self.sorted_descending
            else:
                self.sorted_column = col_name
                self.sorted_descending = False

            # Sort the filtered lazy frame with nulls last
            self.filtered_lazy = self.filtered_lazy.sort(
                col_name, descending=self.sorted_descending, nulls_last=True
            )
            # Reset to first page and refresh
            self.current_page = 0
            self.populate_table()
            self.update_status()
            self.notify(
                f"Sorted by {col_name} {'descending ▼' if self.sorted_descending else 'ascending ▲'}",
                timeout=2,
            )

    def update_status(self) -> None:
        """Update the status bar."""
        if self.lazy_df is None:
            return

        # Get total rows from lazy frame (fast metadata operation)
        total_rows = self.lazy_df.select(pl.len()).collect().item()
        total_cols = len(self.lazy_df.columns)

        start_row = self.current_page * self.PAGE_SIZE + 1
        end_row = min(
            (self.current_page + 1) * self.PAGE_SIZE, self.total_filtered_rows
        )
        max_page = max(0, (self.total_filtered_rows - 1) // self.PAGE_SIZE)

        status = self.query_one("#status", Static)

        # Build status message
        selection_text = ""
        if self.selection_active:
            row_start = min(self.selection_start_row, self.selection_end_row)
            row_end = max(self.selection_start_row, self.selection_end_row)
            col_start = min(self.selection_start_col, self.selection_end_col)
            col_end = max(self.selection_start_col, self.selection_end_col)
            num_rows = row_end - row_start + 1
            num_cols = col_end - col_start + 1
            selection_text = f"-- SELECT ({num_rows}x{num_cols}) -- | "

        if self.total_filtered_rows < total_rows:
            status.update(
                f"{selection_text}{self.total_filtered_rows:,} matches | "
                f"Page {self.current_page + 1}/{max_page + 1} "
                f"({start_row:,}-{end_row:,} of {self.total_filtered_rows:,} records) | "
                f"{total_cols} columns"
            )
        else:
            status.update(
                f"{selection_text}Page {self.current_page + 1}/{max_page + 1} "
                f"({start_row:,}-{end_row:,} of {total_rows:,} records) | "
                f"{total_cols} columns"
            )


if __name__ == "__main__":
    from csvpeek.main import main

    main()
