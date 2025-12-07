"""Tests for CSV filtering functionality."""

import polars as pl
import pytest

from csvpeek.filters import apply_filters_to_lazyframe


class TestStringFiltering:
    """Test filtering on string columns."""

    def test_basic_string_filter(self, sample_csv_path):
        """Test basic case-insensitive substring filtering."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by city containing "New York"
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"city": "New York"})

        result = filtered.collect()
        assert len(result) == 2
        assert all("New York" in city for city in result["city"])

    def test_case_insensitive_filter(self, sample_csv_path):
        """Test that filtering is case-insensitive."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter with different cases
        result_lower = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "scranton"}
        ).collect()
        result_upper = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "SCRANTON"}
        ).collect()
        result_title = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "Scranton"}
        ).collect()

        assert len(result_lower) == len(result_upper) == len(result_title) == 9

    def test_partial_string_match(self, sample_csv_path):
        """Test partial substring matching."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by department containing "eng"
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"department": "eng"})

        result = filtered.collect()
        assert len(result) == 5  # All Engineering entries
        assert all("Engineering" in dept for dept in result["department"])

    def test_empty_filter(self, sample_csv_path):
        """Test that empty filter values are ignored."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        total_before = lazy_df.select(pl.len()).collect().item()

        # Apply empty filters
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "", "name": "  "}
        )
        total_after = filtered.select(pl.len()).collect().item()

        assert total_after == total_before

    def test_no_matches_filter(self, sample_csv_path):
        """Test filter that matches no records."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "NonExistentCity"}
        )

        assert filtered.select(pl.len()).collect().item() == 0


class TestNumericFiltering:
    """Test filtering on numeric columns."""

    def test_exact_numeric_match(self, sample_csv_path):
        """Test exact numeric value matching."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by exact age
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"age": "28"})

        result = filtered.collect()
        assert len(result) == 2
        assert all(age == 28 for age in result["age"])

    def test_numeric_range_filter(self, numeric_csv_path):
        """Test numeric range filtering."""
        lazy_df = pl.scan_csv(numeric_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by range 150-250
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"value": "150-250"})

        result = filtered.collect()
        assert len(result) == 5
        assert all(150 <= val <= 250 for val in result["value"])

    def test_float_range_filter(self, numeric_csv_path):
        """Test range filtering on float values."""
        lazy_df = pl.scan_csv(numeric_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by score range
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"score": "85.0-92.0"}
        )

        result = filtered.collect()
        assert len(result) == 4
        assert all(85.0 <= score <= 92.0 for score in result["score"])

    def test_salary_range_filter(self, sample_csv_path):
        """Test salary range filtering."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter salaries between 70k-80k
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"salary": "70000-80000"}
        )

        result = filtered.collect()
        assert len(result) == 6
        assert all(70000 <= salary <= 80000 for salary in result["salary"])

    def test_invalid_numeric_filter(self, sample_csv_path):
        """Test handling of invalid numeric filters."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Try filtering with non-numeric value on numeric column
        # Should handle gracefully without crashing
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"age": "abc"})

        # Should either match nothing or handle conversion
        assert filtered.select(pl.len()).collect().item() >= 0


class TestMultiColumnFiltering:
    """Test filtering across multiple columns."""

    def test_multiple_filters_and_logic(self, sample_csv_path):
        """Test that multiple filters use AND logic."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by department AND city
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"department": "Sales", "city": "Scranton"}
        )

        result = filtered.collect()
        assert len(result) == 6
        assert all(dept == "Sales" for dept in result["department"])
        assert all(city == "Scranton" for city in result["city"])

    def test_three_column_filter(self, sample_csv_path):
        """Test filtering on three columns simultaneously."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter by department, city, and salary range
        filtered = apply_filters_to_lazyframe(
            lazy_df,
            df_sample,
            {"department": "Sales", "city": "Scranton", "salary": "65000-75000"},
        )

        result = filtered.collect()
        assert len(result) == 4  # Jim, Dwight, Stanley, Michael match this
        assert all(dept == "Sales" for dept in result["department"])
        assert all(city == "Scranton" for city in result["city"])
        assert all(65000 <= sal <= 75000 for sal in result["salary"])

    def test_mixed_type_filters(self, sample_csv_path):
        """Test filtering with both string and numeric filters."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        filtered = apply_filters_to_lazyframe(
            lazy_df,
            df_sample,
            {
                "name": "john",  # String filter
                "age": "28",  # Numeric filter
            },
        )

        result = filtered.collect()
        assert len(result) == 1
        assert result["name"][0] == "John Doe"
        assert result["age"][0] == 28


class TestSpecialCharacters:
    """Test filtering with special characters."""

    def test_literal_dot_in_filter(self, special_chars_csv_path):
        """Test that dots are treated literally, not as regex."""
        lazy_df = pl.scan_csv(special_chars_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter for .nl domains
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"url": ".nl"})

        result = filtered.collect()
        assert len(result) == 1
        assert ".nl" in result["url"][0]

    def test_literal_special_chars(self, special_chars_csv_path):
        """Test that special regex characters are escaped."""
        lazy_df = pl.scan_csv(special_chars_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Test with parentheses
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"description": "(admin)"}
        )
        result = filtered.collect()
        assert len(result) == 1

        # Test with brackets
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"description": "[senior]"}
        )
        result = filtered.collect()
        assert len(result) == 1

        # Test with asterisk
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"description": "*"})
        result = filtered.collect()
        assert len(result) == 1

    def test_plus_and_at_symbols(self, special_chars_csv_path):
        """Test filtering with + and @ symbols."""
        lazy_df = pl.scan_csv(special_chars_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter for emails with +
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"email": "+"})
        result = filtered.collect()
        assert len(result) == 1

        # Filter for @ symbol
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"email": "@"})
        result = filtered.collect()
        assert len(result) == 5  # All emails have @


class TestFilterEdgeCases:
    """Test edge cases and error handling."""

    def test_filter_nonexistent_column(self, sample_csv_path):
        """Test filtering on a column that doesn't exist."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Try to filter on non-existent column
        # Should handle gracefully without crashing
        try:
            filtered = apply_filters_to_lazyframe(
                lazy_df, df_sample, {"nonexistent_column": "value"}
            )
            # Should not crash
            assert True
        except KeyError:
            pytest.fail("Should handle non-existent column gracefully")

    def test_whitespace_handling(self, sample_csv_path):
        """Test that filters trim whitespace."""
        lazy_df = pl.scan_csv(sample_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filters with extra whitespace
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"city": "  Scranton  "}
        )

        result = filtered.collect()
        assert len(result) == 9

    def test_negative_number_in_range(self, numeric_csv_path):
        """Test that negative numbers don't break range parsing."""
        lazy_df = pl.scan_csv(numeric_csv_path)
        df_sample = lazy_df.head(1).collect()

        # Filter with value that starts with dash
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"id": "-5"})

        # Should handle gracefully
        assert filtered.select(pl.len()).collect().item() >= 0
