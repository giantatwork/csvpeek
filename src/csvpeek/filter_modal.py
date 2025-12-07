"""Filter modal dialog for csvpeek."""

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Input, Label, Static


class FilterModal(ModalScreen):
    """Modal screen for entering filters."""

    def __init__(self, columns: list[str], current_filters: dict[str, str]) -> None:
        super().__init__()
        self.columns = columns
        self.current_filters = current_filters
        self.filter_inputs: dict[str, Input] = {}

    def compose(self) -> ComposeResult:
        """Create filter inputs for each column."""
        with Container(id="filter-dialog"):
            yield Static(
                "Enter Filters (Tab to navigate, Enter to apply, Esc to cancel)",
                id="filter-title",
            )
            with VerticalScroll(id="filter-inputs"):
                for col in self.columns:
                    with Horizontal(classes="filter-row"):
                        yield Label(col + ":", classes="filter-label")
                        col_id = col.replace(" ", "_").replace("-", "_")
                        filter_input = Input(
                            value=self.current_filters.get(col, ""),
                            placeholder=f"Filter {col}...",
                            id=f"filter-{col_id}",
                        )
                        self.filter_inputs[col] = filter_input
                        yield filter_input

    def on_mount(self) -> None:
        """Focus first input when modal opens."""
        if self.filter_inputs:
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
