from csvpeek.csvpeek import CSVViewerApp
from csvpeek.selection_utils import Selection


def make_app(csv_path: str) -> CSVViewerApp:
    app = CSVViewerApp(csv_path)
    app.load_csv()
    return app


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

    selected = app.create_selected_dataframe()

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

    selected = app.create_selected_dataframe()

    assert selected == [("Jane Smith",)]
    assert app.get_selection_dimensions() == (1, 1)


def test_remove() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(2, 2)

    sel2 = Selection()
    sel2.start(1, 1)
    sel2.extend(3, 3)

    removed = list(sel.remove(sel2))
    assert removed == [
        (0, 0),
        (0, 1),
        (0, 2),
        (1, 0),
        (2, 0),
    ]


def test_add() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(2, 2)

    sel2 = Selection()

    sel2.start(1, 1)
    sel2.extend(3, 3)

    added = list(sel.add(sel2))
    assert set(added) == {
        (3, 1),
        (3, 2),
        (3, 3),
        (1, 3),
        (2, 3),
    }


def test_add_when_self_inactive() -> None:
    sel = Selection()  # inactive

    sel2 = Selection()
    sel2.start(1, 1)
    sel2.extend(2, 2)

    added = list(sel.add(sel2))
    assert added == [(1, 1), (1, 2), (2, 1), (2, 2)]


def test_remove_disjoint_selection_removes_all_prev() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(1, 1)

    sel2 = Selection()
    sel2.start(3, 3)
    sel2.extend(4, 4)

    removed = set(sel.remove(sel2))
    assert removed == {(0, 0), (0, 1), (1, 0), (1, 1)}


def test_add_disjoint_selection_adds_all_new() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(1, 1)

    sel2 = Selection()
    sel2.start(3, 3)
    sel2.extend(4, 4)

    added = set(sel.add(sel2))
    assert added == {(3, 3), (3, 4), (4, 3), (4, 4)}


def test_remove_when_new_superset_returns_none() -> None:
    sel = Selection()
    sel.start(1, 1)
    sel.extend(2, 2)

    sel2 = Selection()
    sel2.start(0, 0)
    sel2.extend(3, 3)

    assert list(sel.remove(sel2)) == []


def test_add_when_old_superset_returns_empty() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(3, 3)

    sel2 = Selection()
    sel2.start(1, 1)
    sel2.extend(2, 2)

    assert list(sel.add(sel2)) == []


def test_add_when_new_superset_returns_ring() -> None:
    sel = Selection()
    sel.start(1, 1)
    sel.extend(2, 2)

    sel2 = Selection()
    sel2.start(0, 0)
    sel2.extend(3, 3)

    added = set(sel.add(sel2))
    expected = {
        (r, c)
        for r in range(0, 4)
        for c in range(0, 4)
        if not (1 <= r <= 2 and 1 <= c <= 2)
    }
    assert added == expected


def test_remove_when_new_subset_returns_ring() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(3, 3)

    sel2 = Selection()
    sel2.start(1, 1)
    sel2.extend(2, 2)

    removed = set(sel.remove(sel2))
    expected = {
        (r, c)
        for r in range(0, 4)
        for c in range(0, 4)
        if not (1 <= r <= 2 and 1 <= c <= 2)
    }
    assert removed == expected


def test_add_sharing_edge_row_only_adds_below() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(2, 2)

    sel2 = Selection()
    sel2.start(2, 0)
    sel2.extend(4, 2)

    added = set(sel.add(sel2))
    expected = {(r, c) for r in (3, 4) for c in range(0, 3)}
    assert added == expected


def test_remove_sharing_edge_column_only_removes_left() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(2, 2)

    sel2 = Selection()
    sel2.start(0, 2)
    sel2.extend(2, 4)

    removed = set(sel.remove(sel2))
    expected = {(r, c) for r in range(0, 3) for c in (0, 1)}
    assert removed == expected


def test_scroll() -> None:
    sel = Selection()
    sel.start(0, 0)
    sel.extend(1, 1)

    sel2 = Selection()
    sel2.start(0, 0)
    sel2.extend(2, 1)

    assert set(sel.remove(sel2)) == set()

    assert set(sel.add(sel2)) == {(2, 0), (2, 1)}
