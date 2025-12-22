#!/usr/bin/env python3
"""
csvpeek - A snappy, memory-efficient CSV viewer using Polars and Urwid.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Optional

import polars as pl
import pyperclip
import urwid

from csvpeek.filters import apply_filters_to_lazyframe
from csvpeek.selection_utils import (
    clear_selection_and_update,
    create_selected_dataframe,
    get_selection_dimensions,
    get_single_cell_value,
)


def _truncate(text: str, width: int) -> str:
    """Truncate and pad text to a fixed width."""
    if len(text) > width:
        return text[: width - 1] + "…"
    return text.ljust(width)


class FlowColumns(urwid.Columns):
    """Columns that behave as a 1-line flow widget for ListBox rows."""

    sizing = frozenset(["flow"])

    def rows(self, size, focus=False):  # noqa: ANN001, D401
        return 1


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
        for col in self.columns:
            edit = urwid.Edit(f"{col}: ", current_filters.get(col, ""))
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


class CSVViewerApp:
    """Urwid-based CSV viewer with filtering, sorting, and selection."""

    PAGE_SIZE = 50

    def __init__(self, csv_path: str) -> None:
        self.csv_path = Path(csv_path)
        self.df: Optional[pl.DataFrame] = None
        self.lazy_df: Optional[pl.LazyFrame] = None
        self.filtered_lazy: Optional[pl.LazyFrame] = None
        self.cached_page_df: Optional[pl.DataFrame] = None
        self.column_names: list[str] = []

        self.current_page = 0
        self.total_rows = 0
        self.total_filtered_rows = 0
        self.page_cache: dict[int, pl.DataFrame] = {}

        self.current_filters: dict[str, str] = {}
        self.filter_patterns: dict[str, tuple[str, bool]] = {}
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
        self.listbox = urwid.ListBox(self.table_walker)
        self.status_widget = urwid.Text("")
        self.overlaying = False

    # ------------------------------------------------------------------
    # Data loading and preparation
    # ------------------------------------------------------------------
    def load_csv(self) -> None:
        try:
            self.lazy_df = pl.scan_csv(
                self.csv_path, schema_overrides={}, infer_schema_length=0
            )
            # Cache column names without triggering repeated schema resolution
            self.column_names = self.lazy_df.collect_schema().names()
            self.df = self.lazy_df.head(1).collect()
            self.filtered_lazy = self.lazy_df
            self.total_rows = self.lazy_df.select(pl.len()).collect().item()
            self.total_filtered_rows = self.total_rows
            self._calculate_column_widths()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Error loading CSV: {exc}") from exc

    def _calculate_column_widths(self) -> None:
        if self.lazy_df is None or self.df is None:
            return
        sample_size = min(1000, self.total_filtered_rows)
        sample_df = self.lazy_df.head(sample_size).collect()

        self.column_widths = {}
        for col in self.df.columns:
            header_len = len(col) + 2
            if col in sample_df.columns:
                max_len = sample_df[col].str.len_chars().max() or 0
            else:
                max_len = 0
            width = max(header_len, max_len)
            width = max(8, min(int(width), 40))
            self.column_widths[col] = width

    def _get_adaptive_page_size(self) -> int:
        if self.df is None:
            return self.PAGE_SIZE
        num_cols = len(self.df.columns)
        if num_cols > 20:
            return max(20, self.PAGE_SIZE // 2)
        if num_cols > 10:
            return max(30, int(self.PAGE_SIZE * 0.8))
        return self.PAGE_SIZE

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
        if self.df is None:
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
        if self.df is None:
            return []
        names = list(self.df.columns)
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
    def _invalidate_cache(self) -> None:
        self.page_cache.clear()

    def _get_page_df(self) -> Optional[pl.DataFrame]:
        if self.filtered_lazy is None:
            return None
        page_size = self._get_adaptive_page_size()
        max_page = max(0, (self.total_filtered_rows - 1) // page_size)
        self.current_page = min(self.current_page, max_page)
        if self.current_page in self.page_cache:
            return self.page_cache[self.current_page]
        offset = self.current_page * page_size
        page_df = self.filtered_lazy.slice(offset, page_size).collect().fill_null("")
        self.page_cache[self.current_page] = page_df
        return page_df

    def _refresh_rows(self) -> None:
        page_df = self._get_page_df()
        if page_df is None or self.df is None:
            return
        max_width = self._current_screen_width()
        self.cached_page_df = page_df
        self.table_walker.clear()
        # Clamp cursor within available data
        self.cursor_row = min(self.cursor_row, max(0, page_df.height - 1))
        self.cursor_col = min(self.cursor_col, max(0, len(self.df.columns) - 1))

        visible_cols = self._visible_column_names(max_width)
        vis_indices = [self.df.columns.index(c) for c in visible_cols]

        for row_idx, row in enumerate(page_df.iter_rows()):
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
        if self.df is None:
            return urwid.Text("")
        cells = []
        for col_idx in vis_indices:
            col_name = self.df.columns[col_idx]
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
            raise urwid.ExitMainLoop()
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

    def move_cursor(self, key: str) -> None:
        extend = key.startswith("shift")
        if extend and not self.selection_active:
            self.selection_active = True
            self.selection_start_row = self.cursor_row
            self.selection_start_col = self.cursor_col

        cols = len(self.df.columns) if self.df is not None else 0
        rows = self.cached_page_df.height if self.cached_page_df is not None else 0

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
        widths = (
            [self.column_widths.get(c, 12) for c in self.df.columns]
            if self.df is not None
            else []
        )
        self._ensure_cursor_visible(self._current_screen_width(), widths)
        self._refresh_rows()

    def next_page(self) -> None:
        page_size = self._get_adaptive_page_size()
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
        if self.df is None or self.loop is None:
            return

        def _on_submit(filters: dict[str, str]) -> None:
            self.close_overlay()
            self.apply_filters(filters)

        def _on_cancel() -> None:
            self.close_overlay()

        dialog = FilterDialog(
            list(self.df.columns), self.current_filters.copy(), _on_submit, _on_cancel
        )
        self.show_overlay(dialog)

    def apply_filters(self, filters: Optional[dict[str, str]] = None) -> None:
        if self.lazy_df is None or self.df is None:
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

        self._invalidate_cache()
        self.filtered_lazy = apply_filters_to_lazyframe(
            self.lazy_df, self.df, self.current_filters
        )
        if self.sorted_column:
            self.filtered_lazy = self.filtered_lazy.sort(
                self.sorted_column, descending=self.sorted_descending, nulls_last=True
            )

        self.current_page = 0
        self.cursor_row = 0
        self.total_filtered_rows = self.filtered_lazy.select(pl.len()).collect().item()
        self._refresh_rows()

    def reset_filters(self) -> None:
        self.current_filters = {}
        self.filter_patterns = {}
        self.sorted_column = None
        self.sorted_descending = False
        self.filtered_lazy = self.lazy_df
        self._invalidate_cache()
        self.current_page = 0
        self.cursor_row = 0
        self.total_filtered_rows = self.total_rows
        self._refresh_rows()
        self.notify("Filters cleared")

    def sort_current_column(self) -> None:
        if self.df is None or self.filtered_lazy is None:
            return
        if not self.df.columns:
            return
        col_name = self.df.columns[self.cursor_col]
        if self.sorted_column == col_name:
            self.sorted_descending = not self.sorted_descending
        else:
            self.sorted_column = col_name
            self.sorted_descending = False

        self.filtered_lazy = self.filtered_lazy.sort(
            col_name, descending=self.sorted_descending, nulls_last=True
        )
        self._invalidate_cache()
        self.current_page = 0
        self.cursor_row = 0
        self._refresh_rows()
        direction = "descending" if self.sorted_descending else "ascending"
        self.notify(f"Sorted by {col_name} ({direction})")

    # ------------------------------------------------------------------
    # Selection, copy, save
    # ------------------------------------------------------------------
    def copy_selection(self) -> None:
        if self.cached_page_df is None:
            return
        if not self.selection_active:
            cell_str = get_single_cell_value(self)
            pyperclip.copy(cell_str)
            self.notify("Cell copied")
            return
        selected_df = create_selected_dataframe(self)
        num_rows, num_cols = get_selection_dimensions(self)
        from io import StringIO

        buffer = StringIO()
        selected_df.write_csv(buffer, include_header=True)
        pyperclip.copy(buffer.getvalue())
        clear_selection_and_update(self)
        self.notify(f"Copied {num_rows}x{num_cols}")

    def save_selection_dialog(self) -> None:
        if self.cached_page_df is None or self.loop is None:
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
        if self.cached_page_df is None:
            self.notify("No data to save")
            return
        target = Path(file_path)
        if target.exists():
            self.notify(f"File {target} exists")
            return
        try:
            if self.selection_active:
                df_to_save = create_selected_dataframe(self)
                num_rows, num_cols = get_selection_dimensions(self)
            else:
                df_to_save = self.cached_page_df
                num_rows = df_to_save.height
                num_cols = len(df_to_save.columns)
            df_to_save.write_csv(target, include_header=True)
            clear_selection_and_update(self)
            self.notify(f"Saved {num_rows}x{num_cols} to {target.name}")
        except Exception as exc:  # noqa: BLE001
            self.notify(f"Error saving file: {exc}")

    # ------------------------------------------------------------------
    # Overlay helpers
    # ------------------------------------------------------------------
    def show_overlay(self, widget: urwid.Widget) -> None:
        if self.loop is None:
            return
        overlay = urwid.Overlay(
            widget,
            self.loop.widget,
            align="center",
            width=("relative", 80),
            valign="middle",
            height=("relative", 80),
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
        if self.lazy_df is None:
            return
        page_size = self._get_adaptive_page_size()
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
        )
        self.status_widget.set_text(status)

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.load_csv()
        root = self.build_ui()
        self.loop = urwid.MainLoop(
            root,
            palette=[
                ("header", "black", "light gray"),
                ("status", "light gray", "dark gray"),
                ("cell_selected", "black", "yellow"),
                ("filter", "light red", "default"),
                ("focus", "black", "light cyan"),
            ],
            unhandled_input=self.handle_input,
        )
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
        print("Usage: csvpeek <path_to_csv>")
        raise SystemExit(1)

    csv_path = sys.argv[1]
    if not Path(csv_path).exists():
        print(f"Error: File '{csv_path}' not found.")
        raise SystemExit(1)

    app = CSVViewerApp(csv_path)
    app.run()


if __name__ == "__main__":
    main()
