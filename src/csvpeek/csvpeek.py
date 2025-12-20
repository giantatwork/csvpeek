#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using Polars and Textual.
"""

from pathlib import Path
from typing import Optional

import polars as pl
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import DataTable, Footer, Header, Input, Static

from csvpeek.filter_modal import FilterModal
from csvpeek.filters import apply_filters_to_lazyframe
from csvpeek.selection_utils import (
    clear_selection_and_update,
    create_selected_dataframe,
    get_selection_dimensions,
    get_single_cell_value,
)
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
        Binding("w", "save_selection", "Save", priority=True),
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

    PAGE_SIZE = 50  # Number of rows to load per page (base size)

    def __init__(self, csv_path: str) -> None:
        super().__init__()
        self.csv_path = Path(csv_path)
        self.df: Optional[pl.DataFrame] = None
        self.lazy_df: Optional[pl.LazyFrame] = None
        self.filtered_lazy: Optional[pl.LazyFrame] = None
        self.current_page: int = 0
        self.total_filtered_rows: int = 0
        self.current_filters: dict[str, str] = {}
        self.filter_patterns: dict[
            str, tuple[str, bool]
        ] = {}  # col -> (pattern, is_regex)
        self.cached_page_df: Optional[pl.DataFrame] = None
        # Selection tracking
        self.selection_active: bool = False
        self.selection_start_row: Optional[int] = None
        self.selection_start_col: Optional[int] = None
        self.selection_end_row: Optional[int] = None
        self.selection_end_col: Optional[int] = None
        # Filename input tracking
        self.filename_input_active: bool = False
        self.sort_order_descending: bool = False
        self.sorted_column: Optional[str] = None
        self.sorted_descending: bool = False
        self.column_widths: Optional[dict[str, int]] = None

    def _get_adaptive_page_size(self) -> int:
        """Get adaptive page size based on data complexity."""
        if self.df is None:
            return self.PAGE_SIZE

        # Reduce page size for very wide tables to improve performance
        num_cols = len(self.df.columns)
        if num_cols > 20:
            return max(25, self.PAGE_SIZE // 2)
        elif num_cols > 10:
            return max(35, int(self.PAGE_SIZE * 0.8))
        else:
            return self.PAGE_SIZE

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()
        yield DataTable(id="data-table")
        yield Static("", id="status")
        yield Input(
            placeholder="Enter filename to save (e.g., output.csv)", id="filename-input"
        )
        yield Footer()

    def on_mount(self) -> None:
        """Load CSV and populate table."""
        # Hide filename input initially
        input_field = self.query_one("#filename-input", Input)
        input_field.display = False

        self.load_csv()
        self.populate_table()
        self.update_status()

    def on_key(self, event) -> None:
        """Handle key events."""
        if event.key == "escape" and self.filename_input_active:
            # Hide the filename input field
            input_field = self.query_one("#filename-input", Input)
            input_field.display = False
            input_field.value = ""
            self.filename_input_active = False
            event.prevent_default()

    def load_csv(self) -> None:
        """Load CSV file using Polars."""
        try:
            # Use Polars lazy API for efficient filtering
            # Load all columns as strings to avoid type inference issues
            self.lazy_df = pl.scan_csv(
                self.csv_path, schema_overrides={}, infer_schema_length=0
            )
            # Only load a small sample for metadata (columns and dtypes)
            self.df = self.lazy_df.head(1).collect()
            # Start with no filters applied
            self.filtered_lazy = self.lazy_df
            self.total_filtered_rows = self.lazy_df.select(pl.len()).collect().item()
            self.current_page = 0
            self.title = f"csvpeek - {self.csv_path.name}"
            # Calculate column widths from a sample of data
            self._calculate_column_widths()
        except Exception as e:
            self.exit(message=f"Error loading CSV: {e}")

    def _calculate_column_widths(self) -> None:
        """Calculate optimal column widths based on a sample of data."""
        if self.lazy_df is None or self.df is None:
            return

        try:
            # Sample data from different parts of the file for better width estimation
            sample_size = min(1000, self.total_filtered_rows)
            sample_df = self.lazy_df.head(sample_size).collect()

            self.column_widths = {}
            for col in self.df.columns:
                # Calculate max width needed for this column
                # Consider column header length + arrow space
                header_len = len(col) + 3  # +3 for potential sort arrow

                # Get max value length in the sample
                if col in sample_df.columns:
                    # All columns are already strings, no need to cast
                    max_val_len = sample_df[col].str.len_chars().max()
                    if max_val_len is not None:
                        content_len = max_val_len
                    else:
                        content_len = 0
                else:
                    content_len = 0

                # Use the larger of header or content, with reasonable bounds
                width = max(header_len, content_len)
                width = max(10, min(width, 50))  # Between 10 and 50 characters

                self.column_widths[col] = width
        except Exception:
            # If calculation fails, don't use fixed widths
            self.column_widths = None

    def populate_table(self) -> None:
        """Populate the data table with current page of data."""
        if self.filtered_lazy is None:
            return

        table = self.query_one("#data-table", DataTable)

        # Save cursor position
        cursor_row = table.cursor_row
        cursor_col = table.cursor_column

        table.clear(columns=True)

        # Add columns with sort indicators and fixed widths
        if self.df is not None:
            for col in self.df.columns:
                # Add sort arrow if this column is sorted
                if col == self.sorted_column:
                    arrow = " ▼" if self.sorted_descending else " ▲"
                    col_label = f"{col}{arrow}"
                else:
                    col_label = col

                # Use cached column width if available
                if self.column_widths and col in self.column_widths:
                    table.add_column(col_label, key=col, width=self.column_widths[col])
                else:
                    table.add_column(col_label, key=col)

        # Load only the current page of data with adaptive sizing
        page_size = self._get_adaptive_page_size()
        offset = self.current_page * page_size
        page_df = self.filtered_lazy.slice(offset, page_size).collect()

        # Replace None/null values with empty strings
        page_df = page_df.fill_null("")

        self.cached_page_df = page_df  # Cache for yank operation

        # Pre-calculate selection bounds if selection is active
        sel_row_start = sel_row_end = sel_col_start = sel_col_end = None
        if self.selection_active:
            sel_row_start = min(self.selection_start_row, self.selection_end_row)
            sel_row_end = max(self.selection_start_row, self.selection_end_row)
            sel_col_start = min(self.selection_start_col, self.selection_end_col)
            sel_col_end = max(self.selection_start_col, self.selection_end_col)

        # Pre-compile filter patterns for performance
        filter_patterns = {}
        for col_name, (pattern, is_regex) in self.filter_patterns.items():
            if pattern:
                filter_patterns[col_name] = (pattern, is_regex)

        # Add rows with highlighted matches - batch process for better performance
        rows_data = []
        for row_idx, row in enumerate(page_df.iter_rows()):
            styled_row = []
            for col_idx, cell in enumerate(row):
                if col_idx >= len(self.df.columns):
                    break  # Safety check
                col_name = self.df.columns[col_idx]
                cell_str = cell

                # Check if this cell is in the selection
                is_selected = (
                    self.selection_active
                    and sel_row_start is not None
                    and sel_col_start is not None
                    and sel_row_start <= row_idx <= sel_row_end
                    and sel_col_start <= col_idx <= sel_col_end
                )

                # Get filter info for this column if it exists
                filter_info = filter_patterns.get(col_name)
                if filter_info:
                    filter_pattern, is_regex = filter_info
                else:
                    filter_pattern, is_regex = None, False

                # Style the cell
                text = style_cell(cell_str, is_selected, filter_pattern, is_regex)
                styled_row.append(text)
            rows_data.append(styled_row)

        # Add all rows at once for better performance
        for styled_row in rows_data:
            table.add_row(*styled_row)

        # Restore cursor position
        if cursor_row is not None and cursor_col is not None:
            try:
                table.move_cursor(row=cursor_row, column=cursor_col, animate=False)
            except Exception:
                pass
                is_selected = (
                    self.selection_active
                    and sel_row_start is not None
                    and sel_col_start is not None
                    and sel_row_start <= row_idx <= sel_row_end
                    and sel_col_start <= col_idx <= sel_col_end
                )

                # Get filter info for this column if it exists
                filter_info = self.filter_patterns.get(col_name)
                if filter_info:
                    filter_pattern, is_regex = filter_info
                else:
                    filter_pattern, is_regex = None, False

                # Style the cell
                text = style_cell(cell_str, is_selected, filter_pattern, is_regex)
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

        # Cache filter patterns for performance
        filter_patterns = {}
        for col_name, (pattern, is_regex) in self.filter_patterns.items():
            if pattern:
                filter_patterns[col_name] = (pattern, is_regex)

        # Update only changed cells - batch updates for better performance
        updates = []
        for row_idx, col_idx in cells_to_update:
            if row_idx >= self.cached_page_df.height or col_idx >= len(self.df.columns):
                continue

            cell_value = self.cached_page_df.row(row_idx)[col_idx]
            cell_str = cell_value
            col_name = self.df.columns[col_idx]
            is_selected = (row_idx, col_idx) in new_selection

            # Get filter info for this column
            filter_info = filter_patterns.get(col_name)
            if filter_info:
                filter_pattern, is_regex = filter_info
            else:
                filter_pattern, is_regex = None, False

            # Style the cell
            text = style_cell(cell_str, is_selected, filter_pattern, is_regex)
            updates.append((row_idx, col_idx, text))

        # Apply all updates at once
        for row_idx, col_idx, text in updates:
            try:
                table.update_cell_at((row_idx, col_idx), text, update_width=False)
            except Exception:
                # Cell might not exist or be out of bounds
                pass

        # Cache current selection for next update
        self._last_selection_cells = new_selection

    def apply_filters(self, filters: Optional[dict[str, str]] = None) -> None:
        """Apply filters from the filters dict."""
        if self.lazy_df is None or self.df is None:
            return

        # Update current filters if provided
        if filters is not None:
            self.current_filters = filters
            # Build filter patterns dict with regex detection
            self.filter_patterns = {}
            for col, val in filters.items():
                val_stripped = val.strip()
                if val_stripped:
                    if val_stripped.startswith("/"):
                        # Regex mode: store pattern without leading /
                        pattern = val_stripped[1:]
                        if pattern:
                            self.filter_patterns[col] = (pattern, True)
                    else:
                        # Literal mode: store as-is
                        self.filter_patterns[col] = (val_stripped, False)
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
        self.filter_patterns = {}  # Clear filter patterns to remove highlighting
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

        # Get the currently selected column
        table = self.query_one("#data-table", DataTable)
        selected_column = None
        if table.cursor_column is not None and table.cursor_column < len(
            self.df.columns
        ):
            selected_column = self.df.columns[table.cursor_column]

        def handle_filter_result(result: Optional[dict[str, str]]) -> None:
            """Handle the result from the filter modal."""
            if result is not None:
                self.apply_filters(result)

        self.push_screen(
            FilterModal(
                self.df.columns.copy(), self.current_filters.copy(), selected_column
            ),
            handle_filter_result,
        )

    def action_next_page(self) -> None:
        """Load next page of data."""
        page_size = self._get_adaptive_page_size()
        max_page = (self.total_filtered_rows - 1) // page_size
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
            # Copy single cell
            cell_str = get_single_cell_value(self)
            pyperclip.copy(cell_str)
            self.notify(
                f"Copied {cell_str if cell_str else '(empty)'} to clipboard", timeout=2
            )
            self.update_status()
            return

        # Build CSV content from selection using Polars
        from io import StringIO

        # Create a subset DataFrame with just the selected range
        selected_df = create_selected_dataframe(self)

        # Use Polars to write properly escaped CSV
        csv_buffer = StringIO()
        selected_df.write_csv(csv_buffer, include_header=True)
        csv_content = csv_buffer.getvalue()

        # Copy to clipboard
        pyperclip.copy(csv_content)

        # Clear selection and notify
        num_rows, num_cols = get_selection_dimensions(self)
        clear_selection_and_update(self)
        self.notify(
            f"Copied {num_rows} rows, {num_cols} columns",
            timeout=2,
        )

    def action_save_selection(self) -> None:
        """Show filename input field to save selected cells."""
        if self.cached_page_df is None:
            return

        if not self.selection_active:
            self.notify("Please select a range of cells first", timeout=2)
            return

        # Show the filename input field and focus it
        input_field = self.query_one("#filename-input", Input)
        input_field.display = True
        input_field.focus()
        self.filename_input_active = True

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle filename input submission."""
        if event.input.id != "filename-input" or not self.filename_input_active:
            return

        filename = event.value.strip()
        if not filename:
            self.notify("Please enter a filename", timeout=2)
            return

        # Add .csv extension if not present
        if not filename.lower().endswith(".csv"):
            filename += ".csv"

        # Hide the input field
        event.input.display = False
        event.input.value = ""
        self.filename_input_active = False

        # Save the file
        self._save_to_file(filename)

    def _save_to_file(self, file_path: str) -> None:
        """Save selected cells to the specified CSV file."""
        if not self.selection_active:
            return

        try:
            # Create a subset DataFrame with just the selected range
            selected_df = create_selected_dataframe(self)
            # Save to file using Polars
            selected_df.write_csv(file_path, include_header=True)
            # Clear selection and notify
            num_rows, num_cols = get_selection_dimensions(self)
            clear_selection_and_update(self)
            self.notify(
                f"Saved {num_rows} rows, {num_cols} columns to {Path(file_path).name}",
                timeout=2,
            )
        except Exception as e:
            self.notify(f"Error saving file: {e}", timeout=3)

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

        page_size = self._get_adaptive_page_size()
        start_row = self.current_page * page_size + 1
        end_row = min((self.current_page + 1) * page_size, self.total_filtered_rows)
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)

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
