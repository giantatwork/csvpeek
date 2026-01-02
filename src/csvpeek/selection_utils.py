"""Selection utilities for csvpeek (DuckDB backend)."""

from __future__ import annotations


class Selection:
    """Tracks an anchored selection in absolute row/column coordinates."""

    def __init__(self) -> None:
        self.active = False
        self.anchor_row: int | None = None
        self.anchor_col: int | None = None
        self.focus_row: int | None = None
        self.focus_col: int | None = None

    def clear(self) -> None:
        self.active = False
        self.anchor_row = None
        self.anchor_col = None
        self.focus_row = None
        self.focus_col = None

    def start(self, row: int, col: int) -> None:
        self.active = True
        self.anchor_row = row
        self.anchor_col = col
        self.focus_row = row
        self.focus_col = col

    def extend(self, row: int, col: int) -> None:
        if not self.active or self.anchor_row is None or self.anchor_col is None:
            self.start(row, col)
            return
        self.focus_row = row
        self.focus_col = col

    def bounds(self, fallback_row: int, fallback_col: int) -> tuple[int, int, int, int]:
        """Return (row_start, row_end, col_start, col_end).

        If inactive, falls back to the provided cursor position.
        """

        if not self.active or None in (
            self.anchor_row,
            self.anchor_col,
            self.focus_row,
            self.focus_col,
        ):
            return fallback_row, fallback_row, fallback_col, fallback_col

        row_start = min(self.anchor_row, self.focus_row)
        row_end = max(self.anchor_row, self.focus_row)
        col_start = min(self.anchor_col, self.focus_col)
        col_end = max(self.anchor_col, self.focus_col)

        return row_start, row_end, col_start, col_end

    def dimensions(self, fallback_row: int, fallback_col: int) -> tuple[int, int]:
        row_start, row_end, col_start, col_end = self.bounds(fallback_row, fallback_col)

        return row_end - row_start + 1, col_end - col_start + 1

    def contains(
        self, row: int, col: int, *, fallback_row: int, fallback_col: int
    ) -> bool:
        row_start, row_end, col_start, col_end = self.bounds(fallback_row, fallback_col)

        return row_start <= row <= row_end and col_start <= col <= col_end

    def remove(self, new_selection: "Selection"):
        if not self.active:
            return

        row_start, row_end, col_start, col_end = self.bounds(0, 0)
        row_start_new, row_end_new, col_start_new, col_end_new = new_selection.bounds(
            0, 0
        )

        for idx_row in range(row_start, row_end + 1):
            for idx_col in range(col_start, col_end + 1):
                if (
                    (idx_row < row_start_new)
                    or (idx_row > row_end_new)
                    or (idx_col < col_start_new)
                    or (idx_col > col_end_new)
                ):
                    yield idx_row, idx_col

    def add(self, new_selection: "Selection"):
        if not new_selection.active:
            return

        row_start_new, row_end_new, col_start_new, col_end_new = new_selection.bounds(
            0, 0
        )

        self_active = self.active and None not in (
            self.anchor_row,
            self.anchor_col,
            self.focus_row,
            self.focus_col,
        )
        if self_active:
            row_start, row_end, col_start, col_end = self.bounds(0, 0)

        for idx_row in range(row_start_new, row_end_new + 1):
            for idx_col in range(col_start_new, col_end_new + 1):
                if not self_active:
                    yield idx_row, idx_col
                    continue
                if (
                    idx_row < row_start
                    or idx_row > row_end
                    or idx_col < col_start
                    or idx_col > col_end
                ):
                    yield idx_row, idx_col

    def __repr__(self):
        return f"({self.anchor_row}, {self.anchor_col}) -> ({self.focus_row}, {self.focus_col})"
