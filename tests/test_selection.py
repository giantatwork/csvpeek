from pathlib import Path

from csvpeek.duck import DuckBackend
from csvpeek.selection_utils import (
    Selection,
    create_selected_dataframe,
    get_selection_dimensions,
)


class DummyApp:
    def __init__(self, db: DuckBackend) -> None:
        self.db = db
        self.filter_where = ""
        self.filter_params: list = []
        self.sorted_column = None
        self.sorted_descending = False
        self.row_offset = 0
        self.cursor_row = 0
        self.cursor_col = 0
        self.selection = Selection()


def make_app(csv_path: str) -> DummyApp:
    db = DuckBackend(Path(csv_path))
    db.load()
    return DummyApp(db)


def test_selection_bounds_and_dimensions() -> None:
    sel = Selection()
    sel.start(5, 2)
    sel.extend(7, 4)

    rows, cols = sel.dimensions(fallback_row=0, fallback_col=0)
    assert rows == 3
    assert cols == 3
    assert sel.contains(6, 3, fallback_row=0, fallback_col=0)
    assert not sel.contains(4, 3, fallback_row=0, fallback_col=0)


def test_selection_fallback_when_inactive() -> None:
    sel = Selection()
    rows, cols = sel.dimensions(fallback_row=2, fallback_col=1)
    assert rows == 1
    assert cols == 1
    assert sel.bounds(2, 1) == (2, 2, 1, 1)


def test_create_selected_dataframe_uses_absolute_rows(sample_csv_path: str) -> None:
    app = make_app(sample_csv_path)
    # Select rows 2-4 (0-indexed) and columns 0-1
    app.selection.start(2, 0)
    app.selection.extend(4, 1)

    selected = create_selected_dataframe(app)

    assert selected == [
        ("Bob Johnson", "45"),
        ("Alice Williams", "29"),
        ("Charlie Brown", "52"),
    ]


def test_create_selected_dataframe_fallbacks_to_cursor(sample_csv_path: str) -> None:
    app = make_app(sample_csv_path)
    app.cursor_row = 1
    app.cursor_col = 0
    app.row_offset = 0

    selected = create_selected_dataframe(app)

    assert selected == [("Jane Smith",)]
    assert get_selection_dimensions(app) == (1, 1)
