#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using DuckDB and Urwid.
"""

from __future__ import annotations

import csv
import gc
import re
from pathlib import Path

import pyperclip
import urwid

from csvpeek.duck import DuckBackend
from csvpeek.filters import build_where_clause
from csvpeek.screen_buffer import ScreenBuffer
from csvpeek.selection_utils import (
    Selection,
    clear_selection_and_update,
    create_selected_dataframe,
    get_selection_dimensions,
    get_single_cell_value,
)
from csvpeek.ui import (
    ConfirmDialog,
    FilenameDialog,
    FilterDialog,
    FlowColumns,
    HelpDialog,
    PagingListBox,
    _truncate,
    available_body_rows,
    buffer_size,
    build_header_row,
    build_ui,
    current_screen_width,
    update_status,
    visible_column_names,
)


class CSVViewerApp:
    """Urwid-based CSV viewer with filtering, sorting, and selection."""

    PAGE_SIZE = 50
    BASE_PALETTE = [
        ("header", "black", "light gray"),
        ("status", "light gray", "dark gray"),
        ("cell_selected", "black", "yellow"),
        ("filter", "light red", "default"),
        ("focus", "white", "dark blue"),
    ]
    DEFAULT_COLUMN_COLORS = [
        "light cyan",
        "light magenta",
        "light green",
        "yellow",
        "light blue",
        "light red",
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
        page_size = available_body_rows(self)
        fetch_size = buffer_size(self)
        rows, used_offset = self.screen_buffer.get_page_rows(
            desired_start=self.row_offset,
            page_size=page_size,
            total_rows=self.total_filtered_rows,
            fetch_size=fetch_size,
        )
        self.row_offset = used_offset
        self.cached_rows = rows
        gc.collect()
        max_width = current_screen_width(self)
        self.table_walker.clear()
        # Clamp cursor within available data
        self.cursor_row = min(self.cursor_row, max(0, len(self.cached_rows) - 1))
        self.cursor_col = min(self.cursor_col, max(0, len(self.column_names) - 1))

        visible_cols = visible_column_names(self, max_width)
        vis_indices = [self.column_names.index(c) for c in visible_cols]

        for row_idx, row in enumerate(self.cached_rows):
            row_widget = self._build_row_widget(row_idx, row, vis_indices)
            self.table_walker.append(row_widget)

        if self.table_walker:
            self.table_walker.set_focus(self.cursor_row)
        self.table_header = build_header_row(self, max_width)
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

    def _build_row_widget(
        self, row_idx: int, row: tuple, vis_indices: list[int]
    ) -> urwid.Widget:
        if not self.column_names:
            return urwid.Text("")
        cells = []
        for col_idx in vis_indices:
            col_name = self.column_names[col_idx]
            width = self.column_widths.get(col_name, 12)
            cell = row[col_idx]
            is_selected = self._cell_selected(row_idx, col_idx)
            filter_info = self.filter_patterns.get(col_name)
            markup = self._cell_markup(str(cell or ""), width, filter_info, is_selected)
            text = urwid.Text(markup, wrap="clip")
            attr = self._column_attr(col_idx)
            if attr:
                text = urwid.AttrMap(text, attr)
            cells.append((width, text))
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
        if is_selected:
            return [("cell_selected", truncated)]

        if not filter_info:
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
            return truncated

        segments = []
        last = 0
        for start, end in matches:
            if start > last:
                segments.append(truncated[last:start])
            segments.append(("filter", truncated[start:end]))
            last = end
        if last < len(truncated):
            segments.append(truncated[last:])
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

    def move_cursor(self, key: str) -> None:
        from csvpeek.ui import move_cursor

        move_cursor(self, key)

    def next_page(self) -> None:
        page_size = available_body_rows(self)
        max_start = max(0, self.total_filtered_rows - page_size)
        if self.row_offset < max_start:
            self.row_offset = min(self.row_offset + page_size, max_start)
            self.cursor_row = 0
            self._refresh_rows()

    def prev_page(self) -> None:
        if self.row_offset > 0:
            self.row_offset = max(0, self.row_offset - available_body_rows(self))
            self.cursor_row = 0
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
        self.cursor_row = 0
        self.total_filtered_rows = self.total_rows
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
        self.screen_buffer.reset()
        self.selection.clear()
        self.cursor_row = 0
        self._refresh_rows()
        direction = "descending" if self.sorted_descending else "ascending"
        self.notify(f"Sorted by {col_name} ({direction})")

    # ------------------------------------------------------------------
    # Selection, copy, save
    # ------------------------------------------------------------------
    def copy_selection(self) -> None:
        if not self.cached_rows:
            return
        if not self.selection.active:
            cell_str = get_single_cell_value(self)
            try:
                pyperclip.copy(cell_str)
            except Exception as _ex:
                self.notify("Failed to copy cell")
                return
            self.notify("Cell copied")
            return
        selected_rows = create_selected_dataframe(self)
        num_rows, num_cols = get_selection_dimensions(self)
        _row_start, _row_end, col_start, col_end = get_selection_dimensions(
            self, as_bounds=True
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
        clear_selection_and_update(self)
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
            selected_rows = create_selected_dataframe(self)
            num_rows, num_cols = get_selection_dimensions(self)
            _row_start, _row_end, col_start, col_end = get_selection_dimensions(
                self, as_bounds=True
            )
            headers = self.column_names[col_start : col_end + 1]
            with target.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(selected_rows)
            clear_selection_and_update(self)
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
        update_status(self, *_args)

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
