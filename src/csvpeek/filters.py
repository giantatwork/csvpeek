"""Filter utilities for CSV data (DuckDB backend)."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

import duckdb


def _quote_ident(name: str) -> str:
    repl = name.replace('"', '""')
    return f'"{repl}"'


@lru_cache(maxsize=256)
def _is_valid_duckdb_regex(pattern: str) -> bool:
    """Validate a pattern against DuckDB's regex engine (RE2).

    Filters run via ``regexp_matches`` in DuckDB, so patterns must be validated
    against that engine rather than Python's ``re`` to avoid query-time errors
    on syntax DuckDB accepts/rejects differently.
    """
    try:
        duckdb.execute("SELECT regexp_matches('', ?, 'i')", [pattern])
        return True
    except Exception:  # noqa: BLE001
        return False


def build_where_clause(
    filters: dict[str, str], valid_columns: Iterable[str]
) -> tuple[str, list]:
    """Build a DuckDB WHERE clause and parameters from filter definitions.

    Literal filters use a case-insensitive substring match; filters prefixed with
    '/' are treated as case-insensitive regex via regexp_matches.
    """

    clauses = []
    params: list = []
    valid = set(valid_columns)

    for col, raw in filters.items():
        if col not in valid:
            continue
        val = raw.strip()
        if not val:
            continue

        ident = _quote_ident(col)

        if val.startswith("/"):
            pattern = val[1:]
            if not pattern:
                continue
            if not _is_valid_duckdb_regex(pattern):
                continue
            clauses.append(f"regexp_matches({ident}, ?, 'i')")
            params.append(pattern)
        else:
            clauses.append(f"lower({ident}) LIKE ?")
            params.append(f"%{val.lower()}%")

    if not clauses:
        return "", []

    return " WHERE " + " AND ".join(clauses), params
