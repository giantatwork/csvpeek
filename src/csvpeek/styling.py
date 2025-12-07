"""Styling utilities for csvpeek cells."""

from rich.text import Text


def style_cell(
    cell_str: str,
    is_selected: bool,
    filter_value: str | None = None,
) -> Text:
    """
    Apply styling to a cell.

    Args:
        cell_str: The cell content as a string
        is_selected: Whether the cell is selected
        filter_value: Lowercased filter value to highlight, or None

    Returns:
        Styled Text object
    """
    text = Text(cell_str)

    # Apply selection background if selected
    if is_selected:
        text.stylize("on rgb(60,80,120)")

    # Apply filter highlighting if filter is active
    if filter_value:
        lower_cell = cell_str.lower()
        if filter_value in lower_cell:
            start = 0
            filter_len = len(filter_value)
            while True:
                pos = lower_cell.find(filter_value, start)
                if pos == -1:
                    break
                text.stylize("#ff6b6b", pos, pos + filter_len)
                start = pos + 1

    return text
