from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import urwid

if TYPE_CHECKING:  # pragma: no cover
    from csvpeek.csvpeek import CSVViewerApp


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


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------


def current_screen_width(app: "CSVViewerApp") -> int:
    if app.loop and app.loop.screen:
        cols, _rows = app.loop.screen.get_cols_rows()
        return max(cols, 40)
    return 80


def visible_column_names(app: "CSVViewerApp", max_width: int) -> list[str]:
    if not app.column_names:
        return []
    names = list(app.column_names)
    widths = [app.column_widths.get(c, 12) for c in names]
    divide = 1
    start = min(app.col_offset, len(names) - 1 if names else 0)

    ensure_cursor_visible(app, max_width, widths)
    start = app.col_offset

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


def build_header_row(
    app: "CSVViewerApp", max_width: int | None = None
) -> urwid.Columns:
    if not app.column_names:
        return urwid.Columns([])
    if max_width is None:
        max_width = current_screen_width(app)
    cols = []
    for col in visible_column_names(app, max_width):
        label = col
        if app.sorted_column == col:
            label = f"{col} {'▼' if app.sorted_descending else '▲'}"
        width = app.column_widths.get(col, 12)
        header_text = urwid.Text(_truncate(label, width), wrap="clip")
        attr = app._column_attr(app.column_names.index(col))
        if attr:
            header_text = urwid.AttrMap(header_text, attr)
        cols.append((width, header_text))
    return urwid.Columns(cols, dividechars=1)


def build_ui(app: "CSVViewerApp") -> urwid.Widget:
    header_text = urwid.Text(f"csvpeek - {app.csv_path.name}", align="center")
    header = urwid.AttrMap(header_text, "header")
    app.table_header = build_header_row(app, current_screen_width(app))
    body = urwid.Pile(
        [
            ("pack", app.table_header),
            ("pack", urwid.Divider("─")),
            app.listbox,
        ]
    )
    footer = urwid.AttrMap(app.status_widget, "status")
    return urwid.Frame(body=body, header=header, footer=footer)


# ---------------------------------------------------------------------------
# Cursor and selection helpers
# ---------------------------------------------------------------------------


def ensure_cursor_visible(
    app: "CSVViewerApp", max_width: int, widths: list[int]
) -> None:
    if not widths:
        return
    divide = 1
    col = min(app.cursor_col, len(widths) - 1)
    if col < app.col_offset:
        app.col_offset = col
        return
    while True:
        total = 0
        for idx in range(app.col_offset, col + 1):
            total += widths[idx]
            if idx > app.col_offset:
                total += divide
        if total <= max_width or app.col_offset == col:
            break
        app.col_offset += 1


def move_cursor(app: "CSVViewerApp", key: str) -> None:
    extend = key.startswith("shift")

    if extend and not app.selection.active:
        app.selection.start(app.row_offset + app.cursor_row, app.cursor_col)

    cols = len(app.column_names)
    rows = len(app.cached_rows)

    cursor_row = app.cursor_row
    cursor_col = app.cursor_col
    row_offset = app.row_offset

    if key.endswith("left"):
        cursor_col = max(0, cursor_col - 1)
    if key.endswith("right"):
        cursor_col = min(cols - 1, cursor_col + 1)
    if key.endswith("up"):
        if cursor_row > 0:
            cursor_row -= 1
        elif row_offset > 0:
            row_offset -= 1
    if key.endswith("down"):
        if cursor_row < rows - 1:
            cursor_row += 1
        elif row_offset + cursor_row + 1 < app.total_filtered_rows:
            row_offset += 1

    app.cursor_row = cursor_row
    app.cursor_col = cursor_col
    app.row_offset = row_offset

    abs_row = app.row_offset + app.cursor_row
    if extend:
        app.selection.extend(abs_row, app.cursor_col)
    else:
        app.selection.clear()

    widths = [app.column_widths.get(c, 12) for c in app.column_names]
    ensure_cursor_visible(app, current_screen_width(app), widths)
    app._refresh_rows()


def show_overlay(
    app: "CSVViewerApp",
    widget: urwid.Widget,
    *,
    height: urwid.RelativeSizing | str | tuple = "pack",
    width: urwid.RelativeSizing | str | tuple = ("relative", 80),
) -> None:
    if app.loop is None:
        return
    overlay = urwid.Overlay(
        widget,
        app.loop.widget,
        align="center",
        width=width,
        valign="middle",
        height=height,
    )
    app.loop.widget = overlay
    app.overlaying = True


def close_overlay(app: "CSVViewerApp") -> None:
    if app.loop is None:
        return
    if isinstance(app.loop.widget, urwid.Overlay):
        app.loop.widget = app.loop.widget.bottom_w
    app.overlaying = False
    app._refresh_rows()


def update_status(app: "CSVViewerApp", *_args) -> None:  # noqa: ANN002
    if not app.db:
        return
    page_size = available_body_rows(app)
    max_page = max(0, (app.total_filtered_rows - 1) // page_size)
    row_number = app.row_offset + app.cursor_row
    page_idx = row_number // page_size
    selection_info = ""

    if app.selection.active:
        from csvpeek.selection_utils import get_selection_dimensions

        rows, cols = get_selection_dimensions(app)
        selection_info = f"SELECT {rows}x{cols} | "

    page_info = f"Page {page_idx + 1}/{max_page + 1}"
    row_info = f"Row: {row_number + 1}/{app.total_filtered_rows}"
    col_info = f"Col: {app.cursor_col + 1}/{app.total_columns}"

    status = f"{page_info} {row_info}, {col_info} {selection_info} Press ? for help"
    app.status_widget.set_text(status)


def available_body_rows(app: "CSVViewerApp") -> int:
    """Estimate usable body rows based on terminal height."""
    if not app.loop or not app.loop.screen:
        return app.PAGE_SIZE
    _cols, rows = app.loop.screen.get_cols_rows()
    reserved = 4  # header, divider, footer
    return max(5, rows - reserved)


def buffer_size(app: "CSVViewerApp") -> int:
    body = available_body_rows(app)
    return max(body * 4, body + 5)
