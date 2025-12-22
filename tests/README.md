# Tests

This directory contains comprehensive tests for csvpeek's filtering functionality.

## Running Tests

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=csvpeek --cov-report=html
```

## Test Structure

### `conftest.py`
Contains pytest fixtures that provide test data:
- `sample_csv_path`: Standard CSV with various data types
- `numeric_csv_path`: CSV for testing numeric range filters
- `special_chars_csv_path`: CSV for testing special character handling

### `test_filters.py`
Comprehensive tests for the filter functionality:

#### TestStringFiltering
- Case-insensitive substring matching
- Empty filter handling
- Whitespace trimming

#### TestMultiColumnFiltering
- Multiple filters with AND logic
- Non-existent columns ignored

#### TestRegexFiltering
- Regex alternation and anchors via DuckDB `regexp_matches`
- Invalid regex patterns are skipped

#### TestSpecialCharacters
- Literal punctuation in substring filters
- Email characters (+ and @)

## Coverage

The tests cover:
- ✅ String filtering (case-insensitive, substring)
- ✅ Regex filtering (DuckDB vs Python consistency)
- ✅ Special character handling
- ✅ Multi-column AND logic
- ✅ Edge cases and error handling

All tests exercise `build_where_clause` and DuckDB query execution, keeping them UI-independent.
