from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import duckdb


class DuckBackend:
    """DuckDB-backed data source for csvpeek."""

    # Rows sampled when estimating display column widths. Widths are cosmetic
    # and clamped to <=40 chars, so scanning the whole table is wasteful; the
    # first N rows give a good-enough estimate and bound startup cost.
    WIDTH_SAMPLE_ROWS = 5000

    def __init__(self, csv_path: Path, table_name: str = "data") -> None:
        self.csv_path = Path(csv_path)
        self.table_name = table_name
        self.con: duckdb.DuckDBPyConnection | None = None
        self.column_names: list[str] = []
        self.total_rows: int = 0
        self._tmp_dir: str | None = None

    def load(self) -> None:
        """Load the CSV into an on-disk DuckDB table and read schema/row count.

        The table is materialized into a temporary on-disk database rather than
        an in-memory one. DuckDB's buffer pool keeps resident memory bounded by
        spilling to disk, so files larger than available RAM load without
        OOM-killing the process, while paging/sorting/filtering stay fast.
        """
        self._tmp_dir = tempfile.mkdtemp(prefix="csvpeek-")
        db_path = Path(self._tmp_dir) / "data.duckdb"
        self.con = duckdb.connect(database=str(db_path))
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
        query = (
            f"SELECT {', '.join(selects)} "
            f"FROM (SELECT * FROM {self.table_name} LIMIT {self.WIDTH_SAMPLE_ROWS})"
        )
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

    def _select_clause_with_stripped_newlines(self) -> str:
        """Build a SELECT clause that strips control characters from all columns."""
        if not self.column_names:
            return "*"
        selects = [
            f"regexp_replace({self.quote_ident(col)}, '[\\x00-\\x1f\\x7f-\\x9f]', '', 'g') AS {self.quote_ident(col)}"
            for col in self.column_names
        ]
        return ", ".join(selects)

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
        strip_control_chars: bool = True,
    ) -> list[tuple]:
        if not self.con:
            return []
        order_clause = self._order_clause(sorted_column, sorted_descending)
        select_clause = (
            self._select_clause_with_stripped_newlines()
            if strip_control_chars
            else "*"
        )
        query = f"SELECT {select_clause} FROM {self.table_name}{where}{order_clause} LIMIT ? OFFSET ?"
        return self.con.execute(query, params + [limit, offset]).fetchall()

    def close(self) -> None:
        """Close the connection and remove the temporary on-disk database."""
        if self.con is not None:
            try:
                self.con.close()
            except Exception:  # noqa: BLE001
                pass
            self.con = None
        if self._tmp_dir is not None:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None
