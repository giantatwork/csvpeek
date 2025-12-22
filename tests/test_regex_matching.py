"""Ensure DuckDB regex matching aligns with Python's regex highlighting."""

from __future__ import annotations

import re

import duckdb


def duckdb_matches(text: str, pattern: str) -> bool:
    try:
        with duckdb.connect() as con:
            return bool(
                con.execute(
                    "SELECT regexp_matches(?, ?, 'i')", [text, pattern]
                ).fetchone()[0]
            )
    except Exception:
        return False


def python_matches(text: str, pattern: str) -> bool:
    try:
        return re.search(pattern, text, re.IGNORECASE) is not None
    except re.error:
        return False


class TestRegexMatchingConsistency:
    """DuckDB regexp_matches should agree with Python re search semantics for highlighting."""

    def _assert_same(self, text: str, pattern: str):
        assert duckdb_matches(text, pattern) == python_matches(text, pattern)

    def test_simple_word_match(self):
        self._assert_same("Hello World, hello world", "hello")

    def test_case_insensitive_matching(self):
        self._assert_same("ABC abc AbC aBc", "abc")

    def test_special_char_in_pattern(self):
        self._assert_same("test@example.com, TEST@EXAMPLE.COM", r"\w+@\w+\.\w+")

    def test_dot_metacharacter(self):
        self._assert_same("a1b a2b a3b", r"a.b")

    def test_character_class(self):
        self._assert_same("123 456 789", r"\d+")

    def test_alternation(self):
        self._assert_same("cat dog bird cat", r"cat|dog")

    def test_quantifiers_star(self):
        self._assert_same("a aa aaa aaaa", r"a+")

    def test_quantifiers_question(self):
        self._assert_same("color colour", r"colou?r")

    def test_word_boundary(self):
        self._assert_same("test testing tested test", r"\btest\b")

    def test_anchors_start(self):
        self._assert_same("start middle start", r"^start")

    def test_anchors_end(self):
        self._assert_same("end middle end", r"end$")

    def test_groups_capturing(self):
        self._assert_same("abc123 def456", r"([a-z]+)(\d+)")

    def test_groups_non_capturing(self):
        self._assert_same("abc abc", r"(?:abc)")

    def test_unicode_text(self):
        self._assert_same("café CAFÉ Café", r"café")

    def test_empty_pattern(self):
        self._assert_same("test", r"")

    def test_overlapping_matches(self):
        self._assert_same("aaa", r"aa")

    def test_backslash_escape(self):
        self._assert_same("price: $100 $200", r"\$\d+")

    def test_complex_email_pattern(self):
        self._assert_same(
            "Contact: john@example.com or JANE@EXAMPLE.COM",
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        )

    def test_ip_address_pattern(self):
        self._assert_same(
            "Server: 192.168.1.1 and 10.0.0.1", r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
        )

    def test_phone_number_pattern(self):
        self._assert_same("Call: 555-1234 or 555-5678", r"\d{3}-\d{4}")

    def test_no_matches(self):
        self._assert_same("hello world", r"xyz")


class TestInvalidRegex:
    """Invalid patterns should be handled gracefully."""

    def test_invalid_regex(self):
        assert duckdb_matches("test", r"[invalid") is False
        assert python_matches("test", r"[invalid") is False


class TestEdgeCases:
    """Edge inputs behave consistently."""

    def _assert_same(self, text: str, pattern: str):
        assert duckdb_matches(text, pattern) == python_matches(text, pattern)

    def test_empty_text(self):
        self._assert_same("", r"test")

    def test_whitespace_only(self):
        self._assert_same("   \t\n   ", r"\s+")

    def test_very_long_text(self):
        self._assert_same("test " * 1000, r"test")

    def test_multibyte_characters(self):
        self._assert_same("Hello 世界 Hello", r"Hello")
