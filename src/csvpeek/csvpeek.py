#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using DuckDB and Urwid.
"""

from __future__ import annotations

import csv
import re
from copy import deepcopy
from pathlib import Path
from typing import Sequence

import pyperclip
import urwid

from csvpeek.duck import DuckBackend
from csvpeek.filters import build_where_clause
from csvpeek.screen_buffer import ScreenBuffer
from csvpeek.selection_utils import Selection
from csvpeek.ui import (
    ConfirmDialog,
    FilenameDialog,
    FilterDialog,
    FlowColumns,
    HelpDialog,
    PagingListBox,
    _truncate,
    build_header_row,
    build_ui,
    current_screen_width,
    visible_column_names,
)


class CSVViewerApp:
    """Urwid-based CSV viewer with filtering, sorting, and selection."""

    PAGE_SIZE = 50
    BASE_PALETTE = [
        ("header", "black", "light gray"),
        ("status", "light gray", "dark gray"),
        ("cell_selected", "black", "yellow"),
        ("cell_selected_filter", "light red", "yellow"),
        ("filter", "light red", "default"),
        ("focus", "white", "dark blue"),
    ]
    DEFAULT_COLUMN_COLORS = [
        "light cyan",
        "light magenta",
        "light green",
        "yellow",
        "light blue",
    ]

    def __init__(
        self,
        csv_path: str,
        *,
        color_columns: bool = False,
        column_colors: list[str] | None = None,
    ) -> None:
        self.csv_path = Path(csv_path)
        self.db: DuckBackend | None = None
        self.cached_rows: list[tuple] = []
        self.column_names: list[str] = []

        self.current_page = 0
        self.total_rows = 0
        self.total_filtered_rows = 0

        self.current_filters: dict[str, str] = {}
        self.filter_patterns: dict[str, tuple[str, bool]] = {}
        self.filter_where: str = ""
        self.filter_params: list = []
        self.sorted_column: str | None = None
        self.sorted_descending = False
        self.column_widths: dict[str, int] = {}
        self.col_offset = 0  # horizontal scroll offset (column index)
        self.row_offset = 0  # vertical scroll offset (row index)
        self.color_columns = color_columns or bool(column_colors)
        self.column_colors = column_colors or []
        self.column_color_attrs: list[str] = []
        self.screen_buffer = ScreenBuffer(self._fetch_rows)

        # Selection and cursor state
        self.selection = Selection()
        self.prev_selection = Selection()
        self.cursor_row = 0
        self.cursor_col = 0
        self.total_columns = 0

        # UI state
        self.loop: urwid.MainLoop | None = None
        self.table_walker = urwid.SimpleFocusListWalker([])
        self.table_header = urwid.Columns([])
        self.listbox = PagingListBox(self, self.table_walker)
        self.status_widget = urwid.Text("")
        self.overlaying = False
        self.page_redraw_needed = True
        self.cursor_direction = ""

    # ------------------------------------------------------------------
    # Data loading and preparation
    # ------------------------------------------------------------------
    def load_csv(self) -> None:
        try:
            self.db = DuckBackend(self.csv_path)
            self.db.load()
            self.column_names = list(self.db.column_names)
            self.total_columns = len(self.column_names)
            self.total_rows = self.db.total_rows
            self.total_filtered_rows = self.total_rows
            self.column_widths = self.db.column_widths()
            self.screen_buffer.reset()
            self.selection.clear()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Error loading CSV: {exc}") from exc

    def _column_attr(self, col_idx: int) -> str | None:
        if not self.color_columns or not self.column_color_attrs:
            return None
        if col_idx < len(self.column_color_attrs):
            return self.column_color_attrs[col_idx]
        return None

    def _build_palette(self) -> list[tuple]:
        palette = list(self.BASE_PALETTE)
        if not self.color_columns:
            self.column_color_attrs = []
            return palette

        self.column_color_attrs = []
        colors = self.column_colors or self.DEFAULT_COLUMN_COLORS
        if not colors:
            return palette

        for idx, _col in enumerate(self.column_names):
            attr = f"col{idx}"
            color = colors[idx % len(colors)]
            palette.append((attr, color, "default"))
            self.column_color_attrs.append(attr)
        return palette

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def build_ui(self) -> urwid.Widget:
        return build_ui(self)

    def _fetch_rows(self, start_row: int, fetch_size: int) -> list[tuple]:
        if not self.db:
            return []
        return self.db.fetch_rows(
            self.filter_where,
            list(self.filter_params),
            self.sorted_column,
            self.sorted_descending,
            fetch_size,
            start_row,
        )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------
    def _refresh_rows(self) -> None:
        if not self.db:
            return
        if not self.selection.active:
            self.cached_rows = []
        page_size = self.available_body_rows()
        fetch_size = self.buffer_size()
        rows, used_offset = self.screen_buffer.get_page_rows(
            desired_start=self.row_offset,
            page_size=page_size,
            total_rows=self.total_filtered_rows,
            fetch_size=fetch_size,
        )
        self.row_offset = used_offset
        self.cached_rows = rows
        # gc.collect()
        max_width = current_screen_width(self)
        # Clamp cursor within available data
        self.cursor_row = min(self.cursor_row, max(0, len(self.cached_rows) - 1))
        self.cursor_col = min(self.cursor_col, max(0, len(self.column_names) - 1))

        visible_cols = visible_column_names(self, max_width)
        vis_indices = [self.column_names.index(c) for c in visible_cols]

        if self.page_redraw_needed:
            self.table_walker.clear()
            for row_idx, row in enumerate(self.cached_rows):
                row_widget = self._build_row_widget(row_idx, row, vis_indices)
                self.table_walker.append(row_widget)
            self.table_header = build_header_row(self, max_width)
        else:

            def vis_index(col_idx: int) -> int | None:
                try:
                    return vis_indices.index(col_idx)
                except ValueError:
                    return None

            def refresh_cell(row_idx: int, col_idx: int, *, selected: bool) -> bool:
                vis_idx = vis_index(col_idx)
                if vis_idx is None:
                    return False
                if not (0 <= row_idx < len(self.cached_rows)):
                    return False
                row_data = self.cached_rows[row_idx]
                _width, text = self._build_cell_widget(
                    row_data,
                    row_idx,
                    col_idx,
                    selected_override=selected,
                )
                row_widget = self.table_walker[row_idx]
                _cur_widget, opts = row_widget.contents[vis_idx]
                row_widget.contents[vis_idx] = (text, opts)
                return True

            ok_current = refresh_cell(self.cursor_row, self.cursor_col, selected=True)

            prev_cursor_row = self.cursor_row
            prev_cursor_col = self.cursor_col
            if self.cursor_direction == "L":
                prev_cursor_col += 1
            elif self.cursor_direction == "R":
                prev_cursor_col -= 1
            elif self.cursor_direction == "D":
                prev_cursor_row -= 1
            elif self.cursor_direction == "U":
                prev_cursor_row += 1

            ok_prev = True
            if self.cursor_direction:
                ok_prev = refresh_cell(prev_cursor_row, prev_cursor_col, selected=False)

            if not (ok_current and ok_prev):
                self.page_redraw_needed = True
                return self._refresh_rows()

            def refresh_selection(selection: Selection, selected: bool = False):
                if not selection.active:
                    return
                if None in (
                    selection.anchor_row,
                    selection.focus_row,
                    selection.anchor_col,
                    selection.focus_col,
                ):
                    return

                start_row_abs = min(selection.anchor_row, selection.focus_row)
                end_row_abs = max(selection.anchor_row, selection.focus_row)
                start_col = min(selection.anchor_col, selection.focus_col)
                end_col = max(selection.anchor_col, selection.focus_col)

                start_row_rel = start_row_abs - self.row_offset
                end_row_rel = end_row_abs - self.row_offset

                for row_idx in range(start_row_rel, end_row_rel + 1):
                    if not (0 <= row_idx < len(self.cached_rows)):
                        continue
                    for col_idx in range(start_col, end_col + 1):
                        refresh_cell(row_idx, col_idx, selected=selected)

            refresh_selection(self.prev_selection, selected=False)
            refresh_selection(self.selection, selected=True)

        if self.loop:
            frame_widget = self.loop.widget
            if isinstance(frame_widget, urwid.Overlay):
                frame_widget = frame_widget.bottom_w
            if isinstance(frame_widget, urwid.Frame):
                frame_widget.body.contents[0] = (
                    self.table_header,
                    frame_widget.body.options("pack"),
                )
        self._update_status()
        self.page_redraw_needed = False

    def _build_cell_widget(
        self,
        row: tuple,
        row_idx: int,
        col_idx: int,
        *,
        selected_override: bool | None = None,
    ) -> tuple[int, urwid.Widget]:
        col_name = self.column_names[col_idx]
        width = self.column_widths.get(col_name, 12)
        cell_val = row[col_idx]
        is_selected = (
            selected_override
            if selected_override is not None
            else self._cell_selected(row_idx, col_idx)
        )
        filter_info = self.filter_patterns.get(col_name)
        markup = self._cell_markup(str(cell_val or ""), width, filter_info, is_selected)
        text = urwid.Text(markup, wrap="clip")
        attr = self._column_attr(col_idx)
        if attr:
            text = urwid.AttrMap(text, attr)
        return width, text

    def _build_row_widget(
        self, row_idx: int, row: tuple, vis_indices: list[int]
    ) -> urwid.Widget:
        if not self.column_names:
            return urwid.Text("")
        cells = []
        for col_idx in vis_indices:
            width, widget = self._build_cell_widget(row, row_idx, col_idx)
            cells.append((width, widget))
        return FlowColumns(cells, dividechars=1)

    def _cell_selected(self, row_idx: int, col_idx: int) -> bool:
        abs_row = self.row_offset + row_idx
        if self.selection.active and self.selection.contains(
            abs_row,
            col_idx,
            fallback_row=self.row_offset + self.cursor_row,
            fallback_col=self.cursor_col,
        ):
            return True

        return (
            abs_row == self.row_offset + self.cursor_row and col_idx == self.cursor_col
        )

    def _cell_markup(
        self,
        cell_str: str,
        width: int,
        filter_info: tuple[str, bool] | None,
        is_selected: bool,
    ):
        truncated = _truncate(cell_str, width)

        if not filter_info:
            if is_selected:
                return [("cell_selected", truncated)]
            return truncated

        pattern, is_regex = filter_info
        matches = []
        if is_regex:
            try:
                for m in re.finditer(pattern, truncated, re.IGNORECASE):
                    matches.append((m.start(), m.end()))
            except re.error:
                matches = []
        else:
            lower_cell = truncated.lower()
            lower_filter = pattern.lower()
            start = 0
            while True:
                pos = lower_cell.find(lower_filter, start)
                if pos == -1:
                    break
                matches.append((pos, pos + len(lower_filter)))
                start = pos + 1

        if not matches:
            if is_selected:
                return [("cell_selected", truncated)]
            return truncated

        segments = []
        last = 0
        for start, end in matches:
            if start > last:
                slice = truncated[last:start]
                part = ("cell_selected", slice) if is_selected else slice
                segments.append(part)
            slice = truncated[start:end]
            part = ("cell_selected_filter", slice) if is_selected else ("filter", slice)
            segments.append(part)
            last = end

        if last < len(truncated):
            slice = truncated[last:]
            part = ("cell_selected", slice) if is_selected else slice
            segments.append(slice)

        return segments

    # ------------------------------------------------------------------
    # Interaction handlers
    # ------------------------------------------------------------------
    def handle_input(self, key: str) -> None:
        if self.overlaying:
            return
        if key in ("q", "Q"):
            self.confirm_quit()
            return
        if key in ("r", "R"):
            self.reset_filters()
            return
        if key == "s":
            self.sort_current_column()
            return
        if key in ("/",):
            self.open_filter_dialog()
            return
        if key in ("ctrl d", "page down"):
            self.next_page()
            return
        if key in ("ctrl u", "page up"):
            self.prev_page()
            return
        if key in ("c", "C"):
            self.copy_selection()
            return
        if key in ("w", "W"):
            self.save_selection_dialog()
            return
        if key == "?":
            self.open_help_dialog()
            return
        if key in (
            "left",
            "right",
            "up",
            "down",
            "shift left",
            "shift right",
            "shift up",
            "shift down",
        ):
            self.move_cursor(key)

    def confirm_quit(self) -> None:
        if self.loop is None:
            raise urwid.ExitMainLoop()

        def _yes() -> None:
            raise urwid.ExitMainLoop()

        def _no() -> None:
            from csvpeek.ui import close_overlay

            close_overlay(self)

        dialog = ConfirmDialog("Quit csvpeek?", _yes, _no)
        from csvpeek.ui import show_overlay

        show_overlay(self, dialog, width=("relative", 35))

    def next_page(self) -> None:
        page_size = self.available_body_rows()
        max_start = max(0, self.total_filtered_rows - page_size)
        if self.row_offset < max_start:
            self.row_offset = min(self.row_offset + page_size, max_start)
            self.cursor_row = 0
            self.page_redraw_needed = True
            self._refresh_rows()

    def prev_page(self) -> None:
        if self.row_offset > 0:
            self.row_offset = max(0, self.row_offset - self.available_body_rows())
            self.cursor_row = 0
            self.page_redraw_needed = True
            self._refresh_rows()

    # ------------------------------------------------------------------
    # Filtering and sorting
    # ------------------------------------------------------------------
    def open_filter_dialog(self) -> None:
        if not self.column_names or self.loop is None:
            return

        def _on_submit(filters: dict[str, str]) -> None:
            from csvpeek.ui import close_overlay

            close_overlay(self)
            self.apply_filters(filters)

        def _on_cancel() -> None:
            from csvpeek.ui import close_overlay

            close_overlay(self)

        dialog = FilterDialog(
            list(self.column_names), self.current_filters.copy(), _on_submit, _on_cancel
        )
        from csvpeek.ui import show_overlay

        show_overlay(self, dialog, height=("relative", 80))

    def open_help_dialog(self) -> None:
        if self.loop is None:
            return

        def _on_close() -> None:
            from csvpeek.ui import close_overlay

            close_overlay(self)

        dialog = HelpDialog(_on_close)
        # Use relative height to avoid urwid sizing warnings on box widgets
        from csvpeek.ui import show_overlay

        show_overlay(self, dialog, height=("relative", 80))

    def apply_filters(self, filters: dict[str, str] | None = None) -> None:
        if not self.db:
            return
        if filters is not None:
            self.current_filters = filters
            self.filter_patterns = {}
            for col, val in filters.items():
                cleaned = val.strip()
                if not cleaned:
                    continue
                if cleaned.startswith("/") and len(cleaned) > 1:
                    self.filter_patterns[col] = (cleaned[1:], True)
                else:
                    self.filter_patterns[col] = (cleaned, False)

        where, params = build_where_clause(self.current_filters, self.column_names)
        self.filter_where = where
        self.filter_params = params
        self.total_filtered_rows = self.db.count_filtered(where, params)
        self.current_page = 0
        self.row_offset = 0
        self.screen_buffer.reset()
        self.selection.clear()
        self.cursor_row = 0
        self.page_redraw_needed = True
        self._refresh_rows()

    def reset_filters(self) -> None:
        self.current_filters = {}
        self.filter_patterns = {}
        self.sorted_column = None
        self.sorted_descending = False
        self.filter_where = ""
        self.filter_params = []
        self.current_page = 0
        self.row_offset = 0
        self.screen_buffer.reset()
        self.selection.clear()
        self.prev_selection.clear()
        self.cached_rows = []
        self.cursor_row = 0
        self.total_filtered_rows = self.total_rows
        self.page_redraw_needed = True
        self._refresh_rows()
        self.notify("Filters cleared")

    def sort_current_column(self) -> None:
        if not self.column_names or not self.db:
            return
        col_name = self.column_names[self.cursor_col]
        if self.sorted_column == col_name:
            self.sorted_descending = not self.sorted_descending
        else:
            self.sorted_column = col_name
            self.sorted_descending = False
        self.current_page = 0
        self.row_offset = 0
        self.screen_buffer = ScreenBuffer(self._fetch_rows)
        self.selection = Selection()
        self.prev_selection = Selection()
        self.cached_rows = []
        self.cursor_row = 0
        self.cursor_col = min(self.cursor_col, max(0, len(self.column_names) - 1))
        self.col_offset = 0
        self.cursor_direction = ""
        self.page_redraw_needed = True
        self._refresh_rows()
        direction = "descending" if self.sorted_descending else "ascending"
        self.notify(f"Sorted by {col_name} ({direction})")

    # ------------------------------------------------------------------
    # Selection, copy, save
    # ------------------------------------------------------------------
    def get_single_cell_value(self) -> str:
        """Return the current cell value as a string."""
        if not self.cached_rows:
            return ""
        row = self.cached_rows[self.cursor_row]
        cell = row[self.cursor_col] if self.cursor_col < len(row) else None
        return "" if cell is None else str(cell)

    def _selection_bounds(self) -> tuple[int, int, int, int]:
        """Selection bounds as (row_start, row_end, col_start, col_end)."""

        cursor_abs_row = self.row_offset + self.cursor_row
        return self.selection.bounds(cursor_abs_row, self.cursor_col)

    def create_selected_dataframe(self) -> Sequence[Sequence]:
        """Return selected rows for CSV export."""
        if not self.db:
            return []

        row_start, row_end, col_start, col_end = self._selection_bounds()
        fetch_count = row_end - row_start + 1

        rows = self.db.fetch_rows(
            self.filter_where,
            list(self.filter_params),
            self.sorted_column,
            self.sorted_descending,
            fetch_count,
            row_start,
        )

        return [row[col_start : col_end + 1] for row in rows]

    def clear_selection_and_update(self) -> None:
        """Clear selection and refresh visuals."""
        self.selection.clear()
        self.page_redraw_needed = True
        self._refresh_rows()

    def get_selection_dimensions(
        self, as_bounds: bool = False
    ) -> tuple[int, int] | tuple[int, int, int, int]:
        """Get selection dimensions or bounds.

        If `as_bounds` is True, returns (row_start, row_end, col_start, col_end).
        Otherwise returns (num_rows, num_cols).
        """

        row_start, row_end, col_start, col_end = self._selection_bounds()
        if as_bounds:
            return row_start, row_end, col_start, col_end
        return row_end - row_start + 1, col_end - col_start + 1

    def copy_selection(self) -> None:
        if not self.cached_rows:
            return
        if not self.selection.active:
            cell_str = self.get_single_cell_value()
            try:
                pyperclip.copy(cell_str)
            except Exception as _ex:
                self.notify("Failed to copy cell")
                return
            self.notify("Cell copied")
            return
        selected_rows = self.create_selected_dataframe()
        num_rows, num_cols = self.get_selection_dimensions()
        _row_start, _row_end, col_start, col_end = self.get_selection_dimensions(
            as_bounds=True
        )
        headers = self.column_names[col_start : col_end + 1]
        from io import StringIO

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(headers)
        writer.writerows(selected_rows)
        try:
            pyperclip.copy(buffer.getvalue())
        except Exception as _ex:
            self.notify("Failed to copy selection")
            return
        self.clear_selection_and_update()
        self.notify(f"Copied {num_rows}x{num_cols}")

    def save_selection_dialog(self) -> None:
        if not self.cached_rows or self.loop is None:
            return

        def _on_submit(filename: str) -> None:
            if not filename:
                self.notify("Filename required")
                return
            from csvpeek.ui import close_overlay

            close_overlay(self)
            self._save_to_file(filename)

        def _on_cancel() -> None:
            from csvpeek.ui import close_overlay

            close_overlay(self)

        dialog = FilenameDialog("Save as", _on_submit, _on_cancel)
        from csvpeek.ui import show_overlay

        show_overlay(self, dialog)

    def _save_to_file(self, file_path: str) -> None:
        if not self.cached_rows:
            self.notify("No data to save")
            return
        target = Path(file_path)
        if target.exists():
            self.notify(f"File {target} exists")
            return
        try:
            selected_rows = self.create_selected_dataframe()
            num_rows, num_cols = self.get_selection_dimensions()
            _row_start, _row_end, col_start, col_end = self.get_selection_dimensions(
                as_bounds=True
            )
            headers = self.column_names[col_start : col_end + 1]
            with target.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(selected_rows)
            self.clear_selection_and_update()
            self.notify(f"Saved {num_rows}x{num_cols} to {target.name}")
        except Exception as exc:  # noqa: BLE001
            self.notify(f"Error saving file: {exc}")

    # ------------------------------------------------------------------
    # Overlay helpers
    # ------------------------------------------------------------------
    def notify(self, message: str, duration: float = 2.0) -> None:
        self.status_widget.set_text(message)
        if self.loop:
            self.loop.set_alarm_in(duration, lambda *_: self._update_status())

    def _update_status(self, *_args) -> None:  # noqa: ANN002, D401
        self.update_status(*_args)

    # ------------------------------------------------------------------
    # Layout and sizing helpers
    # ------------------------------------------------------------------
    def current_screen_width(self) -> int:
        if self.loop and self.loop.screen:
            cols, _rows = self.loop.screen.get_cols_rows()
            return max(cols, 40)
        return 80

    def available_body_rows(self) -> int:
        if not self.loop or not self.loop.screen:
            return self.PAGE_SIZE
        _cols, rows = self.loop.screen.get_cols_rows()
        reserved = 4  # header, divider, footer
        return max(5, rows - reserved)

    def buffer_size(self) -> int:
        body = self.available_body_rows()
        return max(body * 4, body + 5)

    # ------------------------------------------------------------------
    # Cursor and selection helpers
    # ------------------------------------------------------------------
    def ensure_cursor_visible(self, max_width: int, widths: list[int]) -> None:
        if not widths:
            return
        divide = 1
        col = min(self.cursor_col, len(widths) - 1)
        prev_offset = self.col_offset
        if col < self.col_offset:
            self.col_offset = col
        else:
            for _ in range(len(widths)):
                total = 0
                for idx in range(self.col_offset, col + 1):
                    total += widths[idx]
                    if idx > self.col_offset:
                        total += divide
                if total <= max_width or self.col_offset == col:
                    break
                self.col_offset += 1
        if self.col_offset != prev_offset:
            self.page_redraw_needed = True

    def move_cursor(self, key: str) -> None:
        extend = key.startswith("shift")

        if extend and not self.selection.active:
            self.selection.start(self.row_offset + self.cursor_row, self.cursor_col)

        cols = len(self.column_names)
        rows = len(self.cached_rows)

        cursor_row = self.cursor_row
        cursor_col = self.cursor_col
        row_offset = self.row_offset

        new_cursor_row = cursor_row
        new_cursor_col = cursor_col

        if key.endswith("left"):
            new_cursor_col = max(0, cursor_col - 1)
        if key.endswith("right"):
            new_cursor_col = min(cols - 1, cursor_col + 1)
        if key.endswith("up"):
            if cursor_row > 0:
                new_cursor_row -= 1
            elif row_offset > 0:
                self.page_redraw_needed = True
                row_offset -= 1
        if key.endswith("down"):
            if cursor_row < rows - 1:
                new_cursor_row += 1
            elif row_offset + cursor_row + 1 < self.total_filtered_rows:
                self.page_redraw_needed = True
                row_offset += 1

        self.cursor_row = new_cursor_row
        self.cursor_col = new_cursor_col
        self.row_offset = row_offset

        direction = ""
        if new_cursor_row == cursor_row:
            if new_cursor_col < cursor_col:
                direction = "L"
            else:
                direction = "R"
        elif new_cursor_col == cursor_col:
            if new_cursor_row > cursor_row:
                direction = "D"
            else:
                direction = "U"

        self.cursor_direction = direction

        abs_row = self.row_offset + self.cursor_row
        self.prev_selection = deepcopy(self.selection)
        if extend:
            self.selection.extend(abs_row, self.cursor_col)
        else:
            self.selection.clear()

        widths = [self.column_widths.get(c, 12) for c in self.column_names]
        self.ensure_cursor_visible(self.current_screen_width(), widths)
        self._refresh_rows()

    # ------------------------------------------------------------------
    # Status helper
    # ------------------------------------------------------------------
    def update_status(self, *_args) -> None:  # noqa: ANN002
        if not self.db:
            return
        page_size = self.available_body_rows()
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)
        row_number = self.row_offset + self.cursor_row
        page_idx = row_number // page_size
        selection_info = ""

        if self.selection.active:
            rows, cols = self.get_selection_dimensions()
            selection_info = f"SELECT {rows}x{cols} | "

        page_info = f"Page {page_idx + 1}/{max_page + 1}"
        row_info = f"Row: {row_number + 1}/{self.total_filtered_rows}"
        col_info = f"Col: {self.cursor_col + 1}/{self.total_columns}"

        status = f"{page_info} | {row_info}, {col_info} | {selection_info} | Press ? for help"
        self.status_widget.set_text(status)

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.load_csv()
        root = self.build_ui()
        screen = urwid.raw_display.Screen()
        palette = self._build_palette()
        self.loop = urwid.MainLoop(
            root,
            palette=palette,
            screen=screen,
            handle_mouse=False,
            unhandled_input=self.handle_input,
        )
        # Disable mouse reporting so terminal selection works
        self.loop.screen.set_mouse_tracking(False)
        self._refresh_rows()

        try:
            self.loop.run()
        finally:
            # Ensure terminal modes are restored even on errors/interrupts
            try:
                self.loop.screen.clear()
                self.loop.screen.reset_default_terminal_colors()
            except Exception:
                pass


def main() -> None:
    from csvpeek.main import parse_args

    args, csv_path, colors = parse_args()

    app = CSVViewerApp(
        csv_path,
        color_columns=args.color_columns or bool(colors),
        column_colors=colors,
    )
    app.run()


if __name__ == "__main__":
    main()
