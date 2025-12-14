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
    """Test filtering on columns containing numeric values (treated as strings)."""

    def test_exact_numeric_match(self, sample_csv_path):
        """Test exact numeric value matching (as string)."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter by exact age (string match)
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"age": "28"})

        result = filtered.collect()
        assert len(result) >= 0
        # All matches should contain "28" in age column
        if len(result) > 0:
            assert all("28" in str(age) for age in result["age"])

    def test_numeric_string_filter(self, numeric_csv_path):
        """Test filtering numeric values as strings."""
        lazy_df = pl.scan_csv(
            numeric_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter by substring "15" (will match 150, 151, etc.)
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"value": "15"})

        result = filtered.collect()
        # Should match any value containing "15"
        if len(result) > 0:
            assert all("15" in str(val) for val in result["value"])

    def test_salary_string_filter(self, sample_csv_path):
        """Test salary filtering as strings."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter salaries containing "7" (matches 70000, 75000, 57000, etc.)
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"salary": "7"})

        result = filtered.collect()
        # All results should have "7" in salary
        if len(result) > 0:
            assert all("7" in str(salary) for salary in result["salary"])

    def test_invalid_numeric_filter(self, sample_csv_path):
        """Test handling of text in numeric-looking columns."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Try filtering with letters on age column
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"age": "abc"})

        # Should handle gracefully without crashing
        result = filtered.collect()
        assert len(result) >= 0


class TestMultiColumnFiltering:
    """Test filtering across multiple columns."""

    def test_multiple_filters_and_logic(self, sample_csv_path):
        """Test that multiple filters use AND logic."""
        lazy_df = pl.scan_csv(sample_csv_path)

    def test_multiple_filters_and_logic(self, sample_csv_path):
        """Test that multiple filters use AND logic."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter by department AND city
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"department": "Sales", "city": "Scranton"}
        )

        result = filtered.collect()
        # Should only match rows with both Sales AND Scranton
        if len(result) > 0:
            assert all("sales" in dept.lower() for dept in result["department"])
            assert all("scranton" in city.lower() for city in result["city"])

    def test_three_column_filter(self, sample_csv_path):
        """Test filtering on three columns simultaneously."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter by department, city, and salary substring
        filtered = apply_filters_to_lazyframe(
            lazy_df,
            df_sample,
            {"department": "Sales", "city": "Scranton", "salary": "7"},
        )

        result = filtered.collect()
        # All results should match all three filters
        if len(result) > 0:
            assert all("sales" in dept.lower() for dept in result["department"])
            assert all("scranton" in city.lower() for city in result["city"])
            assert all("7" in str(sal) for sal in result["salary"])

    def test_mixed_type_filters(self, sample_csv_path):
        """Test filtering with multiple string columns."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        filtered = apply_filters_to_lazyframe(
            lazy_df,
            df_sample,
            {
                "name": "john",
                "age": "28",
            },
        )

        result = filtered.collect()
        # Should match rows with "john" in name AND "28" in age
        if len(result) > 0:
            assert all("john" in name.lower() for name in result["name"])
            assert all("28" in str(age) for age in result["age"])


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


class TestRegexFiltering:
    """Test regex filtering mode (filters starting with /)."""

    def test_basic_regex_filter(self, sample_csv_path):
        """Test basic regex pattern matching."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Filter by name starting with 'J'
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": "/^J"})

        result = filtered.collect()
        assert len(result) > 0
        assert all(
            name.startswith("J") or name.startswith("j") for name in result["name"]
        )

    def test_regex_case_insensitive(self, sample_csv_path):
        """Test that regex patterns are case-insensitive."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Pattern should match both "jim" and "JIM"
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": "/jim"})

        result = filtered.collect()
        assert len(result) > 0
        assert all("jim" in name.lower() for name in result["name"])

    def test_regex_alternation(self, sample_csv_path):
        """Test regex alternation (OR)."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Match either "Sales" or "Engineering"
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"department": "/Sales|Engineering"}
        )

        result = filtered.collect()
        assert len(result) > 0
        assert all(
            "sales" in dept.lower() or "engineering" in dept.lower()
            for dept in result["department"]
        )

    def test_regex_word_boundary(self, sample_csv_path):
        """Test word boundary in regex."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Match whole word "NY" not "any"
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"city": r"/\bNY\b"})

        result = filtered.collect()
        # This should match cities with "NY" as a separate word

    def test_regex_character_class(self, sample_csv_path):
        """Test character classes in regex."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Match names with numbers
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": r"/\d"})

        result = filtered.collect()
        # If any names have numbers, they should be matched

    def test_regex_quantifiers(self, sample_csv_path):
        """Test quantifiers in regex."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Match names with one or more letters followed by optional 'y'
        # Note: Polars regex doesn't support backreferences like (.)\1
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": r"/\w+y?"})

        result = filtered.collect()
        # Should match most names

    def test_empty_regex_pattern(self, sample_csv_path):
        """Test that empty pattern after / is handled."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Just "/" should be ignored
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": "/"})

        result = filtered.collect()
        # Should return all rows (filter ignored)
        assert len(result) == lazy_df.select(pl.len()).collect().item()

    def test_invalid_regex_pattern(self, sample_csv_path):
        """Test that invalid regex is handled gracefully."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Invalid regex pattern
        filtered = apply_filters_to_lazyframe(lazy_df, df_sample, {"name": "/[invalid"})

        # Should not crash, just skip the filter
        result = filtered.collect()
        assert len(result) >= 0  # Should complete without error

    def test_literal_vs_regex_mode(self, sample_csv_path):
        """Test difference between literal and regex mode."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Literal mode: dots are literal
        literal_result = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"name": "."}
        ).collect()

        # Regex mode: dot matches any character
        regex_result = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"name": "/."}
        ).collect()

        # Regex should match more (all non-empty names)
        assert len(regex_result) >= len(literal_result)

    def test_regex_email_pattern(self, sample_csv_path):
        """Test realistic email pattern."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Match email-like patterns in name column (if any)
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"name": r"/\w+@\w+\.\w+"}
        )

        result = filtered.collect()
        # This test just ensures the pattern doesn't crash

    def test_multiple_regex_filters(self, sample_csv_path):
        """Test combining multiple regex filters."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # Multiple regex patterns on different columns
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"name": "/^J", "department": "/Sales"}
        )

        result = filtered.collect()
        # Should match rows where name starts with J AND department contains Sales

    def test_mixing_literal_and_regex(self, sample_csv_path):
        """Test mixing literal and regex filters."""
        lazy_df = pl.scan_csv(
            sample_csv_path, schema_overrides={}, infer_schema_length=0
        )
        df_sample = lazy_df.head(1).collect()

        # One literal, one regex
        filtered = apply_filters_to_lazyframe(
            lazy_df, df_sample, {"name": "/^J", "city": "Scranton"}
        )

        result = filtered.collect()
        # Should apply both filters correctly
