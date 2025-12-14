"""Styling utilities for csvpeek cells."""

import re
from typing import Pattern

from rich.text import Text

# Cache for compiled regex patterns
_regex_cache: dict[str, Pattern] = {}


def style_cell(
    cell_str: str,
    is_selected: bool,
    filter_value: str | None = None,
    is_regex: bool = False,
) -> Text:
    """
    Apply styling to a cell.

    Args:
        cell_str: The cell content as a string
        is_selected: Whether the cell is selected
        filter_value: Filter value to highlight (original form), or None
        is_regex: Whether filter_value is a regex pattern

    Returns:
        Styled Text object
    """
    text = Text(cell_str)

    # Apply selection background if selected
    if is_selected:
        text.stylize("on rgb(60,80,120)")

    # Apply filter highlighting if filter is active
    if filter_value:
        if is_regex:
            # Regex mode: use cached compiled pattern
            try:
                # Get or compile pattern
                if filter_value not in _regex_cache:
                    _regex_cache[filter_value] = re.compile(filter_value, re.IGNORECASE)

                pattern = _regex_cache[filter_value]
                for match in pattern.finditer(cell_str):
                    text.stylize("#ff6b6b", match.start(), match.end())
            except re.error:
                # Invalid regex, skip highlighting
                pass
        else:
            # Literal mode: case-insensitive substring search
            lower_cell = cell_str.lower()
            lower_filter = filter_value.lower()
            if lower_filter in lower_cell:
                start = 0
                filter_len = len(lower_filter)
                while True:
                    pos = lower_cell.find(lower_filter, start)
                    if pos == -1:
                        break
                    text.stylize("#ff6b6b", pos, pos + filter_len)
                    start = pos + 1

    return text
