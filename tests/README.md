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
- `sample_dataframe` and `sample_lazy_frame`: Polars DataFrame fixtures

### `test_filters.py`
Comprehensive tests for the filter functionality:

#### TestStringFiltering
- Case-insensitive substring matching
- Partial string matches
- Empty filter handling
- No-match scenarios

#### TestNumericFiltering
- Exact numeric value matching
- Numeric range filters (e.g., "10-20")
- Float range filtering
- Invalid numeric filter handling

#### TestMultiColumnFiltering
- Multiple filters with AND logic
- Three-column combined filters
- Mixed type (string + numeric) filters

#### TestSpecialCharacters
- Literal dot matching (not regex)
- Special regex characters (parentheses, brackets, asterisk)
- Email special characters (+ and @)

#### TestFilterEdgeCases
- Non-existent column handling
- Whitespace trimming
- Negative number handling in ranges

## Coverage

The tests cover:
- ✅ String filtering (case-insensitive, substring)
- ✅ Numeric filtering (exact match, ranges)
- ✅ Special character escaping
- ✅ Multi-column AND logic
- ✅ Edge cases and error handling
- ✅ Empty and whitespace filters

All tests use the standalone `apply_filters_to_lazyframe` function from `csvpeek.filters`, allowing pure logic testing without UI dependencies.
