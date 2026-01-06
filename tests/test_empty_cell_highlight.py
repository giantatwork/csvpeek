"""Test that empty cells are properly highlighted when selected."""

from csvpeek.csvpeek import CSVViewerApp


def test_empty_cell_markup_selected():
    """Test that empty cells get proper markup when selected."""
    app = CSVViewerApp.__new__(CSVViewerApp)
    app.filter_patterns = {}

    # Test empty cell selected without filter - should use space for visibility
    markup = app._cell_markup("", width=10, filter_info=None, is_selected=True)
    assert markup == [("cell_selected", " ")]

    # Test empty cell not selected
    markup = app._cell_markup("", width=10, filter_info=None, is_selected=False)
    assert markup == ""


def test_empty_cell_markup_with_filter():
    """Test empty cells with active filter."""
    app = CSVViewerApp.__new__(CSVViewerApp)

    # Empty cell, filter active but no match - should use space when selected
    markup = app._cell_markup(
        "", width=10, filter_info=("test", False), is_selected=True
    )
    assert markup == [("cell_selected", " ")]

    # Empty cell, filter active but no match - should be empty when not selected
    markup = app._cell_markup(
        "", width=10, filter_info=("test", False), is_selected=False
    )
    assert markup == ""


def test_non_empty_cell_markup():
    """Test non-empty cells to ensure normal behavior is preserved."""
    app = CSVViewerApp.__new__(CSVViewerApp)

    # Non-empty cell selected
    markup = app._cell_markup("test", width=10, filter_info=None, is_selected=True)
    assert markup == [("cell_selected", "test")]

    # Non-empty cell not selected
    markup = app._cell_markup("test", width=10, filter_info=None, is_selected=False)
    assert markup == "test"


def test_cell_with_filter_match():
    """Test that filter highlighting works correctly."""
    app = CSVViewerApp.__new__(CSVViewerApp)

    # Cell with filter match, selected
    markup = app._cell_markup(
        "test data", width=15, filter_info=("test", False), is_selected=True
    )
    # Should have filter highlight for "test" and regular selection for " data"
    assert isinstance(markup, list)
    assert any("cell_selected_filter" in str(item) for item in markup)

    # Cell with filter match, not selected
    markup = app._cell_markup(
        "test data", width=15, filter_info=("test", False), is_selected=False
    )
    assert isinstance(markup, list)
    assert any("filter" in str(item) for item in markup)


def test_empty_cell_copy_preserves_emptiness():
    """Test that copying empty cells doesn't include the display space."""
    import csv
    import tempfile

    # Create a CSV with empty cells
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["col1", "col2", "col3"])
        writer.writerow(["data1", "", "data3"])
        writer.writerow(["", "data5", ""])
        csv_path = f.name

    try:
        app = CSVViewerApp(csv_path)
        app.load_csv()
        app._refresh_rows()

        # Position cursor on empty cell (row 0, col 1)
        app.cursor_row = 0
        app.cursor_col = 1

        # Get the single cell value (what would be copied)
        cell_value = app.get_single_cell_value()

        # The actual data should be empty, not a space
        assert cell_value == "", f"Expected empty string, got {cell_value!r}"

        # Verify the markup shows a space for display
        row = app.cached_rows[0]
        markup = app._cell_markup(
            str(row[1] or ""), width=10, filter_info=None, is_selected=True
        )
        assert markup == [("cell_selected", " ")], (
            "Display should show space for empty cell"
        )

        # Test another empty cell
        app.cursor_row = 1
        app.cursor_col = 0
        cell_value = app.get_single_cell_value()
        assert cell_value == "", f"Expected empty string, got {cell_value!r}"

    finally:
        import os

        os.unlink(csv_path)
