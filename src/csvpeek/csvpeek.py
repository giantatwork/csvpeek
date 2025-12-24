#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using DuckDB and Urwid.
"""

from __future__ import annotations

import csv
import gc
import re
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pyperclip
import urwid

from csvpeek.filters import build_where_clause
from csvpeek.selection_utils import (
    clear_selection_and_update,
    create_selected_dataframe,
    get_selection_dimensions,
    get_single_cell_value,
)


def _truncate(text: str, width: int) -> str:
    """Truncate text to a fixed width without padding."""
    if len(text) > width:
        return text[: width - 1] + "…"
    return text


class FlowColumns(urwid.Columns):
    """Columns that behave as a 1-line flow widget for ListBox rows."""

    sizing = frozenset(["flow"])

    def rows(self, size, focus=False):  # noqa: ANN001, D401
        return 1


class PagingListBox(urwid.ListBox):
    """ListBox that routes page keys to app-level pagination."""

    def __init__(self, app: "CSVViewerApp", body):
        self.app = app
        super().__init__(body)

    def keypress(self, size, key):  # noqa: ANN001
        if getattr(self.app, "overlaying", False):
            return super().keypress(size, key)
        if key in ("page down", "ctrl d"):
            self.app.next_page()
            return None
        if key in ("page up", "ctrl u"):
            self.app.prev_page()
            return None
        return super().keypress(size, key)


class FilterDialog(urwid.WidgetWrap):
    """Modal dialog to collect per-column filters."""

    def __init__(
        self,
        columns: list[str],
        current_filters: dict[str, str],
        on_submit: Callable[[dict[str, str]], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.columns = columns
        self.current_filters = current_filters
        self.on_submit = on_submit
        self.on_cancel = on_cancel

        self.edits: list[urwid.Edit] = []
        edit_rows = []
        pad_width = max((len(c) for c in self.columns), default=0) + 1
        for col in self.columns:
            label = f"{col.ljust(pad_width)}: "
            edit = urwid.Edit(label, current_filters.get(col, ""))
            self.edits.append(edit)
            edit_rows.append(urwid.AttrMap(edit, None, focus_map="focus"))
        self.walker = urwid.SimpleFocusListWalker(edit_rows)
        listbox = urwid.ListBox(self.walker)
        instructions = urwid.Padding(
            urwid.Text("Tab to move, Enter to apply, Esc to cancel"), left=1, right=1
        )
        frame = urwid.Frame(body=listbox, header=instructions)
        boxed = urwid.LineBox(frame, title="Filters")
        super().__init__(boxed)

    def keypress(self, size, key):  # noqa: ANN001
        if key == "tab":
            self._move_focus(1)
            return None
        if key == "shift tab":
            self._move_focus(-1)
            return None
        if key in ("enter",):
            filters = {
                col: edit.edit_text for col, edit in zip(self.columns, self.edits)
            }
            self.on_submit(filters)
            return None
        if key in ("esc", "ctrl g"):
            self.on_cancel()
            return None
        return super().keypress(size, key)

    def _move_focus(self, delta: int) -> None:
        if not self.walker:
            return
        focus = self.walker.focus or 0
        self.walker.focus = (focus + delta) % len(self.walker)


class FilenameDialog(urwid.WidgetWrap):
    """Modal dialog for choosing a filename."""

    def __init__(
        self,
        prompt: str,
        on_submit: Callable[[str], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.edit = urwid.Edit(f"{prompt}: ")
        self.on_submit = on_submit
        self.on_cancel = on_cancel
        pile = urwid.Pile(
            [
                urwid.Text("Enter filename and press Enter"),
                urwid.Divider(),
                urwid.AttrMap(self.edit, None, focus_map="focus"),
            ]
        )
        boxed = urwid.LineBox(pile, title="Save Selection")
        super().__init__(urwid.Filler(boxed, valign="top"))

    def keypress(self, size, key):  # noqa: ANN001
        if key in ("enter",):
            self.on_submit(self.edit.edit_text.strip())
            return None
        if key in ("esc", "ctrl g"):
            self.on_cancel()
            return None
        return super().keypress(size, key)


class HelpDialog(urwid.WidgetWrap):
    """Modal dialog listing keyboard shortcuts."""

    def __init__(self, on_close: Callable[[], None]) -> None:
        shortcuts = [
            ("?", "Show this help"),
            ("q", "Quit"),
            ("r", "Reset filters"),
            ("/", "Open filter dialog"),
            ("s", "Sort by current column (toggle asc/desc)"),
            ("c", "Copy cell or selection"),
            ("w", "Save selection to CSV"),
            ("←/→/↑/↓", "Move cursor"),
            ("Shift + arrows", "Extend selection"),
            ("PgUp / Ctrl+U", "Previous page"),
            ("PgDn / Ctrl+D", "Next page"),
        ]
        rows = [urwid.Text("Keyboard Shortcuts", align="center"), urwid.Divider()]
        for key, desc in shortcuts:
            rows.append(urwid.Columns([(12, urwid.Text(key)), urwid.Text(desc)]))
        body = urwid.ListBox(urwid.SimpleFocusListWalker(rows))
        boxed = urwid.LineBox(body)
        self.on_close = on_close
        super().__init__(boxed)

    def keypress(self, size, key):  # noqa: ANN001
        if key in ("esc", "enter", "q", "?", "ctrl g"):
            self.on_close()
            return None
        return super().keypress(size, key)


class ConfirmDialog(urwid.WidgetWrap):
    """Simple yes/no confirmation dialog."""

    def __init__(
        self, message: str, on_yes: Callable[[], None], on_no: Callable[[], None]
    ) -> None:
        yes_btn = urwid.Button("Yes", on_press=lambda *_: on_yes())
        no_btn = urwid.Button("No", on_press=lambda *_: on_no())
        buttons = urwid.Columns(
            [
                urwid.Padding(
                    urwid.AttrMap(yes_btn, None, focus_map="focus"), left=1, right=1
                ),
                urwid.Padding(
                    urwid.AttrMap(no_btn, None, focus_map="focus"), left=1, right=1
                ),
            ]
        )
        pile = urwid.Pile([urwid.Text(message), urwid.Divider(), buttons])
        boxed = urwid.LineBox(pile, title="Confirm")
        self.on_yes = on_yes
        self.on_no = on_no
        super().__init__(boxed)

    def keypress(self, size, key):  # noqa: ANN001
        if key in ("y", "Y"):
            self.on_yes()
            return None
        if key in ("n", "N", "esc", "ctrl g", "q", "Q"):
            self.on_no()
            return None
        return super().keypress(size, key)


class CSVViewerApp:
    """Urwid-based CSV viewer with filtering, sorting, and selection."""

    PAGE_SIZE = 50

    def __init__(self, csv_path: str) -> None:
        self.csv_path = Path(csv_path)
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        self.table_name = "data"
        self.cached_rows: list[tuple] = []
        self.column_names: list[str] = []

        self.current_page = 0
        self.total_rows = 0
        self.total_filtered_rows = 0

        self.current_filters: dict[str, str] = {}
        self.filter_patterns: dict[str, tuple[str, bool]] = {}
        self.filter_where: str = ""
        self.filter_params: list = []
        self.sorted_column: Optional[str] = None
        self.sorted_descending = False
        self.column_widths: dict[str, int] = {}
        self.col_offset = 0  # horizontal scroll offset (column index)

        # Selection and cursor state
        self.selection_active = False
        self.selection_start_row: Optional[int] = None
        self.selection_start_col: Optional[int] = None
        self.selection_end_row: Optional[int] = None
        self.selection_end_col: Optional[int] = None
        self.cursor_row = 0
        self.cursor_col = 0

        # UI state
        self.loop: Optional[urwid.MainLoop] = None
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
            self.con = duckdb.connect(database=":memory:")
            if str(self.csv_path) == "__demo__":
                size = 50_000
                self.con.execute(
                    f"""
                    CREATE TABLE {self.table_name} AS
                    SELECT
                        CAST(i AS VARCHAR) AS id,
                        CAST(i % 10 AS VARCHAR) AS "group",
                        CAST(i % 5 AS VARCHAR) AS category,
                        CAST(i * 11 AS VARCHAR) AS value,
                        'row ' || CAST(i AS VARCHAR) AS text
                    FROM range(?) t(i)
                    """,
                    [size],
                )
            else:
                self.con.execute(
                    f"""
                    CREATE TABLE {self.table_name} AS
                    SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE)
                    """,
                    [str(self.csv_path)],
                )

            info = self.con.execute(
                f"PRAGMA table_info('{self.table_name}')"
            ).fetchall()
            self.column_names = [row[1] for row in info]
            self.total_rows = self.con.execute(
                f"SELECT count(*) FROM {self.table_name}"
            ).fetchone()[0]  # type: ignore
            self.total_filtered_rows = self.total_rows
            self._calculate_column_widths()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Error loading CSV: {exc}") from exc

    def _calculate_column_widths(self) -> None:
        if not self.con or not self.column_names:
            return
        # Use DuckDB to compute max string length per column across the table
        selects = [
            f"max(length({self._quote_ident(col)})) AS len_{idx}"
            for idx, col in enumerate(self.column_names)
        ]
        query = f"SELECT {', '.join(selects)} FROM {self.table_name}"
        lengths = self.con.execute(query).fetchone()
        if lengths is None:
            lengths = [0] * len(self.column_names)

        self.column_widths = {}
        for idx, col in enumerate(self.column_names):
            header_len = len(col) + 2
            data_len = lengths[idx] or 0  # length() returns None if column is empty
            max_len = max(header_len, int(data_len))
            width = max(8, min(max_len, 40))
            self.column_widths[col] = width

    def _quote_ident(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def _available_body_rows(self) -> int:
        """Estimate usable body rows based on terminal height."""
        if not self.loop or not self.loop.screen:
            return self.PAGE_SIZE
        _cols, rows = self.loop.screen.get_cols_rows()
        # Reserve lines for header (1), divider (1), footer/status (1).
        reserved = 4
        return max(5, rows - reserved)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def build_ui(self) -> urwid.Widget:
        header_text = urwid.Text(f"csvpeek - {self.csv_path.name}", align="center")
        header = urwid.AttrMap(header_text, "header")
        self.table_header = self._build_header_row(self._current_screen_width())
        body = urwid.Pile(
            [
                ("pack", self.table_header),
                ("pack", urwid.Divider("─")),
                self.listbox,
            ]
        )
        footer = urwid.AttrMap(self.status_widget, "status")
        return urwid.Frame(body=body, header=header, footer=footer)

    def _build_header_row(self, max_width: Optional[int] = None) -> urwid.Columns:
        if not self.column_names:
            return urwid.Columns([])
        if max_width is None:
            max_width = self._current_screen_width()
        cols = []
        for col in self._visible_column_names(max_width):
            label = col
            if self.sorted_column == col:
                label = f"{col} {'▼' if self.sorted_descending else '▲'}"
            width = self.column_widths.get(col, 12)
            cols.append((width, urwid.Text(_truncate(label, width), wrap="clip")))
        return urwid.Columns(cols, dividechars=1)

    def _current_screen_width(self) -> int:
        if self.loop and self.loop.screen:
            cols, _rows = self.loop.screen.get_cols_rows()
            return max(cols, 40)
        return 80

    def _visible_column_names(self, max_width: int) -> list[str]:
        if not self.column_names:
            return []
        names = list(self.column_names)
        widths = [self.column_widths.get(c, 12) for c in names]
        divide = 1
        start = min(self.col_offset, len(names) - 1 if names else 0)

        # Ensure the current cursor column is within view
        self._ensure_cursor_visible(max_width, widths)
        start = self.col_offset

        chosen: list[str] = []
        used = 0
        for idx in range(start, len(names)):
            w = widths[idx]
            extra = w if not chosen else w + divide
            if used + extra > max_width and chosen:
                break
            chosen.append(names[idx])
            used += extra
        if not chosen and names:
            chosen.append(names[start])
        return chosen

    def _ensure_cursor_visible(self, max_width: int, widths: list[int]) -> None:
        if not widths:
            return
        divide = 1
        col = min(self.cursor_col, len(widths) - 1)
        # Adjust left boundary when cursor is left of offset
        if col < self.col_offset:
            self.col_offset = col
            return

        # If cursor is off to the right, shift offset until it fits
        while True:
            total = 0
            for idx in range(self.col_offset, col + 1):
                total += widths[idx]
                if idx > self.col_offset:
                    total += divide
            if total <= max_width or self.col_offset == col:
                break
            self.col_offset += 1

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------
    def _build_base_query(self) -> tuple[str, list]:
        where, params = self.filter_where, list(self.filter_params)
        order = ""
        if self.sorted_column:
            direction = "DESC" if self.sorted_descending else "ASC"
            order = f" ORDER BY {self._quote_ident(self.sorted_column)} {direction}"
        return where + order, params

    def _get_page_rows(self) -> list[tuple]:
        if not self.con:
            return []
        page_size = self._available_body_rows()
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)
        self.current_page = min(self.current_page, max_page)
        offset = self.current_page * page_size
        order_where, params = self._build_base_query()
        query = f"SELECT * FROM {self.table_name}{order_where} LIMIT ? OFFSET ?"
        return self.con.execute(query, params + [page_size, offset]).fetchall()

    def _refresh_rows(self) -> None:
        if not self.con:
            return
        if not self.selection_active:
            self.cached_rows = []
        self.cached_rows = self._get_page_rows()
        gc.collect()
        max_width = self._current_screen_width()
        self.table_walker.clear()
        # Clamp cursor within available data
        self.cursor_row = min(self.cursor_row, max(0, len(self.cached_rows) - 1))
        self.cursor_col = min(self.cursor_col, max(0, len(self.column_names) - 1))

        visible_cols = self._visible_column_names(max_width)
        vis_indices = [self.column_names.index(c) for c in visible_cols]

        for row_idx, row in enumerate(self.cached_rows):
            row_widget = self._build_row_widget(row_idx, row, vis_indices)
            self.table_walker.append(row_widget)

        if self.table_walker:
            self.table_walker.set_focus(self.cursor_row)
        self.table_header = self._build_header_row(max_width)
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
            cells.append((width, text))
        return FlowColumns(cells, dividechars=1)

    def _cell_selected(self, row_idx: int, col_idx: int) -> bool:
        if not self.selection_active:
            return row_idx == self.cursor_row and col_idx == self.cursor_col
        row_start, row_end, col_start, col_end = get_selection_dimensions(
            self, as_bounds=True
        )
        return row_start <= row_idx <= row_end and col_start <= col_idx <= col_end

    def _cell_markup(
        self,
        cell_str: str,
        width: int,
        filter_info: Optional[tuple[str, bool]],
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
            self.close_overlay()

        dialog = ConfirmDialog("Quit csvpeek?", _yes, _no)
        self.show_overlay(dialog, width=("relative", 35))

    def move_cursor(self, key: str) -> None:
        extend = key.startswith("shift")
        if extend and not self.selection_active:
            self.selection_active = True
            self.selection_start_row = self.cursor_row
            self.selection_start_col = self.cursor_col

        cols = len(self.column_names)
        rows = len(self.cached_rows)

        if key.endswith("left"):
            self.cursor_col = max(0, self.cursor_col - 1)
        if key.endswith("right"):
            self.cursor_col = min(cols - 1, self.cursor_col + 1)
        if key.endswith("up"):
            self.cursor_row = max(0, self.cursor_row - 1)
        if key.endswith("down"):
            self.cursor_row = min(rows - 1, self.cursor_row + 1)

        if not extend:
            self.selection_active = False
        else:
            self.selection_end_row = self.cursor_row
            self.selection_end_col = self.cursor_col
        widths = [self.column_widths.get(c, 12) for c in self.column_names]
        self._ensure_cursor_visible(self._current_screen_width(), widths)
        self._refresh_rows()

    def next_page(self) -> None:
        page_size = self._available_body_rows()
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)
        if self.current_page < max_page:
            self.current_page += 1
            self.cursor_row = 0
            self.selection_active = False
            self._refresh_rows()

    def prev_page(self) -> None:
        if self.current_page > 0:
            self.current_page -= 1
            self.cursor_row = 0
            self.selection_active = False
            self._refresh_rows()

    # ------------------------------------------------------------------
    # Filtering and sorting
    # ------------------------------------------------------------------
    def open_filter_dialog(self) -> None:
        if not self.column_names or self.loop is None:
            return

        def _on_submit(filters: dict[str, str]) -> None:
            self.close_overlay()
            self.apply_filters(filters)

        def _on_cancel() -> None:
            self.close_overlay()

        dialog = FilterDialog(
            list(self.column_names), self.current_filters.copy(), _on_submit, _on_cancel
        )
        self.show_overlay(dialog, height=("relative", 80))

    def open_help_dialog(self) -> None:
        if self.loop is None:
            return

        def _on_close() -> None:
            self.close_overlay()

        dialog = HelpDialog(_on_close)
        # Use relative height to avoid urwid sizing warnings on box widgets
        self.show_overlay(dialog, height=("relative", 80))

    def apply_filters(self, filters: Optional[dict[str, str]] = None) -> None:
        if not self.con:
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
        count_query = f"SELECT count(*) FROM {self.table_name}{where}"
        self.total_filtered_rows = self.con.execute(count_query, params).fetchone()[0]  # type: ignore
        self.current_page = 0
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
        self.cursor_row = 0
        self.total_filtered_rows = self.total_rows
        self._refresh_rows()
        self.notify("Filters cleared")

    def sort_current_column(self) -> None:
        if not self.column_names or not self.con:
            return
        if not self.column_names:
            return
        col_name = self.column_names[self.cursor_col]
        if self.sorted_column == col_name:
            self.sorted_descending = not self.sorted_descending
        else:
            self.sorted_column = col_name
            self.sorted_descending = False
        self.current_page = 0
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
        if not self.selection_active:
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
            self.close_overlay()
            self._save_to_file(filename)

        def _on_cancel() -> None:
            self.close_overlay()

        dialog = FilenameDialog("Save as", _on_submit, _on_cancel)
        self.show_overlay(dialog)

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
    def show_overlay(
        self,
        widget: urwid.Widget,
        height: urwid.RelativeSizing | str | tuple = "pack",
        width: urwid.RelativeSizing | str | tuple = ("relative", 80),
    ) -> None:
        if self.loop is None:
            return
        overlay = urwid.Overlay(
            widget,
            self.loop.widget,
            align="center",
            width=width,
            valign="middle",
            height=height,
        )
        self.loop.widget = overlay
        self.overlaying = True

    def close_overlay(self) -> None:
        if self.loop is None:
            return
        if isinstance(self.loop.widget, urwid.Overlay):
            self.loop.widget = self.loop.widget.bottom_w
        self.overlaying = False
        self._refresh_rows()

    # ------------------------------------------------------------------
    # Status handling
    # ------------------------------------------------------------------
    def notify(self, message: str, duration: float = 2.0) -> None:
        self.status_widget.set_text(message)
        if self.loop:
            self.loop.set_alarm_in(duration, lambda *_: self._update_status())

    def _update_status(self, *_args) -> None:  # noqa: ANN002, D401
        if not self.con:
            return
        page_size = self._available_body_rows()
        start = self.current_page * page_size + 1
        end = min((self.current_page + 1) * page_size, self.total_filtered_rows)
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)
        selection_text = ""
        if self.selection_active:
            rows, cols = get_selection_dimensions(self)
            selection_text = f"SELECT {rows}x{cols} | "
        status = (
            f"{selection_text}Page {self.current_page + 1}/{max_page + 1} "
            f"({start:,}-{end:,} of {self.total_filtered_rows:,}) | "
            f"Columns: {len(self.column_names) if self.column_names else '…'}"
            " Press ? for help"
        )
        self.status_widget.set_text(status)

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.load_csv()
        root = self.build_ui()
        screen = urwid.raw_display.Screen()
        self.loop = urwid.MainLoop(
            root,
            palette=[
                ("header", "black", "light gray"),
                ("status", "light gray", "dark gray"),
                ("cell_selected", "black", "yellow"),
                ("filter", "light red", "default"),
                ("focus", "white", "dark blue"),
            ],
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
    import sys

    if len(sys.argv) < 2:
        print("Usage: csvpeek <path_to_csv> | --demo")
        raise SystemExit(1)

    arg = sys.argv[1]
    demo_mode = arg in {"--demo", "demo", ":demo:"}

    if demo_mode:
        csv_path = "__demo__"
    else:
        csv_path = arg
        if not Path(csv_path).exists():
            print(f"Error: File '{csv_path}' not found.")
            raise SystemExit(1)

    app = CSVViewerApp(csv_path)
    app.run()


if __name__ == "__main__":
    main()
