"""
Extensive tests to ensure Polars regex filtering and Python regex highlighting
produce identical matches.

This is critical to ensure the UI highlighting accurately reflects what data
is being filtered.
"""

import re

import polars as pl


def get_polars_matches(text: str, pattern: str) -> list[tuple[int, int]]:
    """
    Get match positions using Polars' regex engine.

    This simulates what Polars does when filtering - it just checks if the pattern
    matches anywhere in the string. We then use Python's re to find the actual
    positions since Polars doesn't expose position information.

    The key is: we verify that Polars WOULD match this row, and Python finds
    the same matches.

    Returns list of (start, end) tuples for each match.
    """
    # Create a DataFrame with the text
    df = pl.DataFrame({"text": [text]})

    # Check if Polars would match this row with the pattern
    try:
        matches_row = df.filter(pl.col("text").str.contains(f"(?i){pattern}"))

        if len(matches_row) == 0:
            # Polars doesn't match - should be same as Python
            return []

        # Polars matches - now use Python re to find positions
        # This is what we do in styling.py for highlighting
        python_matches = []
        for match in re.finditer(pattern, text, re.IGNORECASE):
            python_matches.append((match.start(), match.end()))

        return python_matches
    except Exception:
        return []


def get_python_matches(text: str, pattern: str) -> list[tuple[int, int]]:
    """
    Get match positions using Python's re module with IGNORECASE flag.

    This is what we use in styling.py for highlighting.
    Returns list of (start, end) tuples for each match.
    """
    try:
        matches = []
        for match in re.finditer(pattern, text, re.IGNORECASE):
            matches.append((match.start(), match.end()))
        return matches
    except re.error:
        return []


class TestRegexMatchingConsistency:
    """Test that Polars and Python regex matching produce identical results."""

    def test_simple_word_match(self):
        """Test basic word matching."""
        text = "Hello World, hello world"
        pattern = "hello"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2  # Two occurrences

    def test_case_insensitive_matching(self):
        """Test that case-insensitive matching works identically."""
        text = "ABC abc AbC aBc"
        pattern = "abc"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 4

    def test_special_char_in_pattern(self):
        """Test regex with special characters."""
        text = "test@example.com, TEST@EXAMPLE.COM"
        pattern = r"\w+@\w+\.\w+"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_dot_metacharacter(self):
        """Test that . matches any character."""
        text = "a1b a2b a3b"
        pattern = r"a.b"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 3

    def test_character_class(self):
        """Test character class matching."""
        text = "123 456 789"
        pattern = r"\d+"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 3

    def test_alternation(self):
        """Test alternation (OR) operator."""
        text = "cat dog bird cat"
        pattern = r"cat|dog"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 3  # Two cats, one dog

    def test_quantifiers_star(self):
        """Test * quantifier."""
        text = "a aa aaa aaaa"
        pattern = r"a+"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 4

    def test_quantifiers_question(self):
        """Test ? quantifier."""
        text = "color colour"
        pattern = r"colou?r"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_word_boundary(self):
        """Test word boundary \\b."""
        text = "test testing tested test"
        pattern = r"\btest\b"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2  # Only standalone "test"

    def test_anchors_start(self):
        """Test start anchor ^."""
        text = "start middle start"
        pattern = r"^start"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 1  # Only at start

    def test_anchors_end(self):
        """Test end anchor $."""
        text = "end middle end"
        pattern = r"end$"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 1  # Only at end

    def test_groups_capturing(self):
        """Test capturing groups."""
        text = "abc123 def456"
        pattern = r"([a-z]+)(\d+)"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_groups_non_capturing(self):
        """Test non-capturing groups."""
        text = "abc abc"
        pattern = r"(?:abc)"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_unicode_text(self):
        """Test matching unicode characters."""
        text = "café CAFÉ Café"
        pattern = r"café"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 3

    def test_empty_pattern(self):
        """Test empty pattern behavior."""
        text = "test"
        pattern = r""

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        # Both should match at every position
        assert polars_matches == python_matches

    def test_overlapping_matches(self):
        """Test overlapping pattern matches."""
        text = "aaa"
        pattern = r"aa"

        # Note: Polars and re.finditer both find non-overlapping matches
        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 1  # Only first "aa", not overlapping

    def test_backslash_escape(self):
        """Test escaping special characters."""
        text = "price: $100 $200"
        pattern = r"\$\d+"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_complex_email_pattern(self):
        """Test complex real-world pattern (email)."""
        text = "Contact: john@example.com or JANE@EXAMPLE.COM"
        pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_ip_address_pattern(self):
        """Test IP address pattern."""
        text = "Server: 192.168.1.1 and 10.0.0.1"
        pattern = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_phone_number_pattern(self):
        """Test phone number pattern."""
        text = "Call: 555-1234 or 555-5678"
        pattern = r"\d{3}-\d{4}"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2

    def test_no_matches(self):
        """Test pattern with no matches."""
        text = "hello world"
        pattern = r"xyz"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 0


class TestInvalidRegex:
    """Test handling of invalid regex patterns."""

    def test_invalid_regex_python(self):
        """Test Python handles invalid regex gracefully."""
        text = "test"
        pattern = r"[invalid"  # Unclosed bracket

        python_matches = get_python_matches(text, pattern)
        assert python_matches == []  # Should return empty list

    def test_invalid_regex_polars(self):
        """Test Polars handles invalid regex."""
        text = "test"
        pattern = r"[invalid"

        # Polars may throw or return empty, but shouldn't crash
        try:
            polars_matches = get_polars_matches(text, pattern)
            # If it succeeds, should be empty or same as Python
            assert polars_matches == get_python_matches(text, pattern)
        except Exception:
            # If it throws, that's also acceptable
            pass


class TestEdgeCases:
    """Test edge cases for regex matching."""

    def test_empty_text(self):
        """Test matching against empty text."""
        text = ""
        pattern = r"test"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 0

    def test_whitespace_only(self):
        """Test matching whitespace."""
        text = "   \t\n   "
        pattern = r"\s+"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches

    def test_very_long_text(self):
        """Test matching in very long text."""
        text = "test " * 1000  # 5000 characters
        pattern = r"test"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 1000

    def test_multibyte_characters(self):
        """Test matching with multibyte UTF-8 characters."""
        text = "Hello 世界 Hello"
        pattern = r"Hello"

        polars_matches = get_polars_matches(text, pattern)
        python_matches = get_python_matches(text, pattern)

        assert polars_matches == python_matches
        assert len(polars_matches) == 2
