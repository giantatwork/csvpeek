from __future__ import annotations

from typing import Callable


class ScreenBuffer:
    """Manages paged row buffering without reaching into UI or app state."""

    def __init__(self, fetch_rows: Callable[[int, int], list[tuple]]) -> None:
        self.fetch_rows = fetch_rows
        self.start = 0
        self.rows: list[tuple] = []
        self.num_rows = 0

    def reset(self) -> None:
        self.start = 0
        self.rows = []
        self.num_rows = 0

    def get_page_rows(
        self,
        *,
        desired_start: int,
        page_size: int,
        total_rows: int,
        fetch_size: int,
    ) -> tuple[list[tuple], int]:
        """Return rows for the requested page and the clamped start offset."""

        clamped_start = max(0, min(desired_start, max(0, total_rows - page_size)))
        need_end = min(clamped_start + page_size, total_rows)
        have_start = self.start
        have_end = self.start + self.num_rows

        if self.rows and clamped_start >= have_start and need_end <= have_end:
            start = clamped_start - have_start
            end = start + page_size
            return self.rows[start:end], clamped_start

        self._fill(clamped_start, fetch_size)
        start = 0
        end = min(page_size, self.num_rows)
        return self.rows[start:end], clamped_start

    def _fill(self, start_row: int, fetch_size: int) -> None:
        self.rows = self.fetch_rows(start_row, fetch_size)
        self.num_rows = len(self.rows)
        self.start = start_row
