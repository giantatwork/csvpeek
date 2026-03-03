from __future__ import annotations

import glob
from pathlib import Path
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
        focus_col: int | None = None,
        focus_val: str | None = None,
    ) -> None:
        self.columns = columns
        self.current_filters = current_filters
        self.on_submit = on_submit
        self.on_cancel = on_cancel

        self.edits: list[urwid.Edit] = []
        edit_rows = []
        pad_width = max((len(c) for c in self.columns), default=0) + 1
        for col_num, col in enumerate(self.columns):
            label = f"{col.ljust(pad_width)}: "
            current_filter = current_filters.get(col)
            if not current_filter:
                if focus_val is not None and col_num == focus_col:
                    current_filter = focus_val
            edit = urwid.Edit(
                label,
                current_filter if current_filter else "",
            )
            self.edits.append(edit)
            edit_rows.append(urwid.AttrMap(edit, None, focus_map="focus"))
        self.walker = urwid.SimpleFocusListWalker(edit_rows)
        if self.walker and focus_col is not None:
            self.walker.focus = max(0, min(focus_col, len(self.walker) - 1))
        listbox = urwid.ListBox(self.walker)
        instructions = urwid.Padding(
            urwid.Text("↑/↓ to move, Enter to apply, Esc to cancel"), left=1, right=1
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
    """Modal dialog for choosing a filename with Tab path-completion."""

    _COMPLETION_HEIGHT = 8  # max rows shown in the completion list

    def __init__(
        self,
        prompt: str,
        on_submit: Callable[[str], None],
        on_cancel: Callable[[], None],
        title: str = "Save Selection",
    ) -> None:
        self.edit = urwid.Edit(f"{prompt}: ")
        self.on_submit = on_submit
        self.on_cancel = on_cancel
        self._completions: list[str] = []
        self._completion_walker = urwid.SimpleFocusListWalker([])
        self._completion_listbox = urwid.ListBox(self._completion_walker)
        self._edit_attrmap = urwid.AttrMap(self.edit, None, focus_map="focus")
        self.pile = urwid.Pile(
            [
                urwid.Text("Tab to complete, Enter to confirm, Esc to cancel"),
                urwid.Divider(),
                self._edit_attrmap,
            ]
        )
        boxed = urwid.LineBox(self.pile, title=title)
        super().__init__(urwid.Filler(boxed, valign="top"))

    # ------------------------------------------------------------------
    # Completion helpers
    # ------------------------------------------------------------------
    def _completion_adapter(self) -> "urwid.BoxAdapter | None":
        for w, _ in self.pile.contents:
            if isinstance(w, urwid.BoxAdapter):
                return w
        return None

    def _show_completions(self, matches: list[str]) -> None:
        self._completions = matches
        self._completion_walker[:] = [
            urwid.AttrMap(urwid.SelectableIcon(m, 0), None, focus_map="focus")
            for m in matches
        ]
        if self._completion_adapter() is None:
            height = min(len(matches), self._COMPLETION_HEIGHT)
            self.pile.contents.append(
                (
                    urwid.BoxAdapter(self._completion_listbox, height),
                    self.pile.options("pack"),
                )
            )
        # Move focus to the completion list
        self.pile.focus_position = len(self.pile.contents) - 1

    def _hide_completions(self) -> None:
        self._completions = []
        self.pile.contents = [
            (w, opts)
            for w, opts in self.pile.contents
            if not isinstance(w, urwid.BoxAdapter)
        ]
        # Return focus to the edit field (index 2)
        self.pile.focus_position = 2

    def _completions_focused(self) -> bool:
        return (
            bool(self._completions)
            and self._completion_adapter() is not None
            and self.pile.focus_position == len(self.pile.contents) - 1
        )

    # ------------------------------------------------------------------
    # Input handling
    # ------------------------------------------------------------------
    def keypress(self, size, key):  # noqa: ANN001
        if self._completions_focused():
            if key == "enter":
                focus = self._completion_walker.focus or 0
                selected = self._completions[focus]
                self.edit.set_edit_text(selected)
                self.edit.set_edit_pos(len(selected))
                self._hide_completions()
                return None
            if key in ("esc", "ctrl g", "tab"):
                self._hide_completions()
                return None
            # up/down/etc. handled by the ListBox inside
            return super().keypress(size, key)

        if key == "tab":
            partial = self.edit.edit_text
            matches = sorted(glob.glob(glob.escape(partial) + "*"))
            matches = [m + "/" if Path(m).is_dir() else m for m in matches]
            if len(matches) == 1:
                self.edit.set_edit_text(matches[0])
                self.edit.set_edit_pos(len(matches[0]))
            elif len(matches) > 1:
                self._show_completions(matches)
            return None
        if key == "enter":
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
            ("r", "Reset all filters"),
            ("f", "Open filter dialog"),
            ("F", "Open filter dialog and preset cursor value"),
            ("s", "Sort by current column (toggle asc/desc)"),
            ("c", "Copy cell or selection"),
            ("w", "Save selection to CSV"),
            ("L", "Open another CSV file"),
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

    # Column visibility adjustment now lives on the app
    app.ensure_cursor_visible(max_width, widths)
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
