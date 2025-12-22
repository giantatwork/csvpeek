"""Filter utilities for CSV data (DuckDB backend)."""

from __future__ import annotations

import re
from typing import Iterable


def _quote_ident(name: str) -> str:
    return f'"{name.replace('"', '""')}"'


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
            try:
                re.compile(pattern)
            except re.error:
                continue
            clauses.append(f"regexp_matches({ident}, ?, 'i')")
            params.append(pattern)
        else:
            clauses.append(f"lower({ident}) LIKE ?")
            params.append(f"%{val.lower()}%")

    if not clauses:
        return "", []

    return " WHERE " + " AND ".join(clauses), params
