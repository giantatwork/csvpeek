from __future__ import annotations

from pathlib import Path

import duckdb


class DuckBackend:
    """DuckDB-backed data source for csvpeek."""

    def __init__(self, csv_path: Path, table_name: str = "data") -> None:
        self.csv_path = Path(csv_path)
        self.table_name = table_name
        self.con: duckdb.DuckDBPyConnection | None = None
        self.column_names: list[str] = []
        self.total_rows: int = 0

    def load(self) -> None:
        """Load the CSV into an in-memory DuckDB table and read schema/row count."""
        self.con = duckdb.connect(database=":memory:")
        self.con.execute(
            f"""
            CREATE TABLE {self.table_name} AS
            SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE)
            """,
            [str(self.csv_path)],
        )
        info = self.con.execute(f"PRAGMA table_info('{self.table_name}')").fetchall()
        self.column_names = [row[1] for row in info]
        self.total_rows = self.con.execute(
            f"SELECT count(*) FROM {self.table_name}"
        ).fetchone()[0]  # type: ignore

    def quote_ident(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def column_widths(self) -> dict[str, int]:
        if not self.con or not self.column_names:
            return {}
        selects = [
            f"max(length({self.quote_ident(col)})) AS len_{idx}"
            for idx, col in enumerate(self.column_names)
        ]
        query = f"SELECT {', '.join(selects)} FROM {self.table_name}"
        lengths = self.con.execute(query).fetchone()
        if lengths is None:
            lengths = [0] * len(self.column_names)

        widths: dict[str, int] = {}
        for idx, col in enumerate(self.column_names):
            header_len = len(col) + 2
            data_len = lengths[idx] or 0  # length() returns None if column is empty
            max_len = max(header_len, int(data_len))
            width = max(8, min(max_len, 40))
            widths[col] = width
        return widths

    def _order_clause(self, sorted_column: str | None, sorted_descending: bool) -> str:
        if not sorted_column:
            return ""
        direction = "DESC" if sorted_descending else "ASC"
        return f" ORDER BY {self.quote_ident(sorted_column)} {direction}"

    def count_filtered(self, where: str, params: list) -> int:
        if not self.con:
            return 0
        count_query = f"SELECT count(*) FROM {self.table_name}{where}"
        return self.con.execute(count_query, params).fetchone()[0]  # type: ignore

    def fetch_rows(
        self,
        where: str,
        params: list,
        sorted_column: str | None,
        sorted_descending: bool,
        limit: int,
        offset: int,
    ) -> list[tuple]:
        if not self.con:
            return []
        order_clause = self._order_clause(sorted_column, sorted_descending)
        query = f"SELECT * FROM {self.table_name}{where}{order_clause} LIMIT ? OFFSET ?"
        return self.con.execute(query, params + [limit, offset]).fetchall()
