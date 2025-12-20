"""Filter modal dialog for csvpeek."""

import re
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Input, Label, Static


class FilterModal(ModalScreen):
    """Modal screen for entering filters."""

    def __init__(
        self,
        columns: list[tuple[str, str]],
        current_filters: dict[str, str],
        selected_column_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.columns = columns  # list of (original, sanitized)
        self.current_filters = current_filters  # keyed by sanitized name
        self.selected_column_key = selected_column_key
        self.filter_inputs: dict[str, Input] = {}  # keyed by sanitized name
        self._used_ids: set[str] = set()

    def _sanitize_id(self, name: str) -> str:
        """Return a Textual-safe, unique id for a column name."""
        base = re.sub(r"[^0-9A-Za-z_-]", "_", name)
        if not base:
            base = "col"
        if base[0].isdigit():
            base = f"col_{base}"

        candidate = base
        counter = 1
        while candidate in self._used_ids:
            candidate = f"{base}_{counter}"
            counter += 1

        self._used_ids.add(candidate)
        return candidate

    def compose(self) -> ComposeResult:
        """Create filter inputs for each column."""
        with Container(id="filter-dialog"):
            yield Static(
                "Enter Filters (Start with / for regex, Tab to navigate, Enter to apply, Esc to cancel)",
                id="filter-title",
            )
            with VerticalScroll(id="filter-inputs"):
                for original_name, sanitized in self.columns:
                    with Horizontal(classes="filter-row"):
                        yield Label(original_name + ":", classes="filter-label")
                        col_id = self._sanitize_id(sanitized)
                        filter_input = Input(
                            value=self.current_filters.get(sanitized, ""),
                            placeholder="text or /regex...",
                            id=f"filter-{col_id}",
                        )
                        self.filter_inputs[sanitized] = filter_input
                        yield filter_input

    def on_mount(self) -> None:
        """Focus first input when modal opens."""
        if self.filter_inputs:
            # Focus the selected column's input if provided, otherwise first input
            if (
                self.selected_column_key
                and self.selected_column_key in self.filter_inputs
            ):
                self.filter_inputs[self.selected_column_key].focus()
            else:
                first_input = list(self.filter_inputs.values())[0]
                first_input.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Apply filters when Enter is pressed."""
        filters = {col: inp.value for col, inp in self.filter_inputs.items()}
        self.dismiss(filters)

    def on_key(self, event) -> None:
        """Handle Escape to cancel."""
        if event.key == "escape":
            self.dismiss(None)
