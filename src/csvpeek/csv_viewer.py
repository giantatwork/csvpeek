#!/usr/bin/env python3
"""
csvpeek TUI - A snappy, memory-efficient CSV viewer using Polars and Textual.
"""

import re
import sys
from pathlib import Path
from typing import Optional

import polars as pl
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Header, Static

from filter_modal import FilterModal
from styles import APP_CSS
from styling import style_cell


class CSVPeekApp(App):
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

    PAGE_SIZE = 100  # Number of rows to load per page

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
            self.title = f"CSV Viewer - {self.csv_path.name}"
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

        # Add columns
        if self.df is not None:
            for col in self.df.columns:
                table.add_column(col, key=col)

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
                col_name = self.df.columns[col_idx]
                cell_str = str(cell)

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

        # Start with lazy frame for efficient query building
        filtered = self.lazy_df

        # Apply each column filter
        for col, filter_value in self.current_filters.items():
            filter_value = filter_value.strip()

            if filter_value:
                # Apply literal text filter (no regex)
                try:
                    # Check if column is string type (check on eager df)
                    col_dtype = self.df[col].dtype
                    is_string = col_dtype in (pl.Utf8, pl.String) or str(
                        col_dtype
                    ).lower() in ("utf8", "string")

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
                                        (pl.col(col) >= min_val)
                                        & (pl.col(col) <= max_val)
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

        # Update filtered lazy frame and reset to first page
        self.filtered_lazy = filtered
        self.current_page = 0
        self.total_filtered_rows = filtered.select(pl.len()).collect().item()
        self.populate_table()
        self.update_status()

    def action_reset_filters(self) -> None:
        """Reset all filters."""
        self.current_filters = {}
        self.filtered_lazy = self.lazy_df
        self.current_page = 0
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
        if not self.selection_active or self.cached_page_df is None:
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
                    str(row_data[col_idx])
                    for col_idx in range(col_start, min(col_end + 1, len(row_data)))
                ]
                lines.append("\t".join(selected_cells))

        # Copy to clipboard
        import pyperclip

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
            # Sort the filtered lazy frame
            self.filtered_lazy = self.filtered_lazy.sort(
                col_name, descending=self.sort_order_descending
            )
            # Reset to first page and refresh
            self.current_page = 0
            self.populate_table()
            self.update_status()
            self.notify(
                f"Sorted by {col_name} {'descending' if self.sort_order_descending else 'ascending'}",
                timeout=2,
            )
            self.sort_order_descending = not self.sort_order_descending

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


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python csv_viewer.py <path_to_csv>")
        sys.exit(1)

    csv_path = sys.argv[1]

    if not Path(csv_path).exists():
        print(f"Error: File '{csv_path}' not found.")
        sys.exit(1)

    app = CSVPeekApp(csv_path)
    app.run()


if __name__ == "__main__":
    main()
