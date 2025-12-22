"""Tests for DuckDB-backed filtering helpers."""

from __future__ import annotations

import duckdb

from csvpeek.filters import build_where_clause


def _load_table(con: duckdb.DuckDBPyConnection, csv_path: str) -> tuple[list[str], int]:
    con.execute("DROP TABLE IF EXISTS data")
    con.execute(
        "CREATE TABLE data AS SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE)",
        [csv_path],
    )
    columns = [row[1] for row in con.execute("PRAGMA table_info('data')").fetchall()]
    total_rows = con.execute("SELECT count(*) FROM data").fetchone()[0]
    return columns, total_rows


def _filter_rows(
    con: duckdb.DuckDBPyConnection,
    csv_path: str,
    filters: dict[str, str],
    select: str = "*",
):
    columns, total_rows = _load_table(con, csv_path)
    where_clause, params = build_where_clause(filters, columns)
    rows = con.execute(f"SELECT {select} FROM data{where_clause}", params).fetchall()
    return rows, total_rows


class TestStringFiltering:
    """Case-insensitive substring filtering."""

    def test_basic_string_filter(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(
                con, sample_csv_path, {"city": "New York"}, select="city"
            )
        assert len(rows) == 2
        assert all("new york" in city[0].lower() for city in rows)

    def test_case_insensitive_filter(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(con, sample_csv_path, {"city": "scranton"})
        assert len(rows) == 9

    def test_whitespace_handling(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(con, sample_csv_path, {"city": "  Scranton  "})
        assert len(rows) == 9


class TestMultiColumnFiltering:
    """Filters combine with AND semantics."""

    def test_multiple_filters_and_logic(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(
                con,
                sample_csv_path,
                {"department": "Sales", "city": "Scranton"},
                select="department, city",
            )
        assert len(rows) == 6
        assert all(
            row[0].lower() == "sales" and row[1].lower() == "scranton" for row in rows
        )

    def test_empty_filter_is_ignored(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, total = _filter_rows(con, sample_csv_path, {"city": "  "})
        assert len(rows) == total

    def test_nonexistent_column_is_ignored(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, total = _filter_rows(
                con, sample_csv_path, {"does_not_exist": "value"}
            )
        assert len(rows) == total


class TestRegexFiltering:
    """Regex filters use DuckDB regexp_matches with 'i' flag."""

    def test_basic_regex_filter(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(con, sample_csv_path, {"name": "/^j"}, select="name")
        assert len(rows) == 3
        assert all(name[0].lower().startswith("j") for name in rows)

    def test_regex_alternation(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(
                con,
                sample_csv_path,
                {"department": "/Sales|Engineering"},
                select="department",
            )
        assert len(rows) == 14
        assert all(
            "sales" in dept[0].lower() or "engineering" in dept[0].lower()
            for dept in rows
        )

    def test_invalid_regex_is_skipped(self, sample_csv_path):
        with duckdb.connect() as con:
            rows, total = _filter_rows(con, sample_csv_path, {"name": "/["})
        assert len(rows) == total


class TestSpecialCharacters:
    """Literal substring matching handles punctuation characters."""

    def test_literal_dot_in_filter(self, special_chars_csv_path):
        with duckdb.connect() as con:
            rows, _ = _filter_rows(
                con, special_chars_csv_path, {"url": ".nl"}, select="url"
            )
        assert len(rows) == 1
        assert ".nl" in rows[0][0]

    def test_plus_and_at_symbols(self, special_chars_csv_path):
        with duckdb.connect() as con:
            plus_rows, _ = _filter_rows(
                con, special_chars_csv_path, {"email": "+"}, select="email"
            )
            at_rows, _ = _filter_rows(
                con, special_chars_csv_path, {"email": "@"}, select="email"
            )
        assert len(plus_rows) == 1
        assert len(at_rows) == 5
