from __future__ import annotations

import argparse
import csv
import tempfile
import time
from pathlib import Path

from csvpeek.csvpeek import CSVViewerApp


def build_csv(rows: int, cols: int) -> Path:
    header = [f"col{i}" for i in range(cols)]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    path = Path(tmp.name)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in range(rows):
            writer.writerow([f"{c}-{r}" for c in header])
    return path


def time_full_redraw(csv_path: Path, iterations: int) -> float:
    app = CSVViewerApp(str(csv_path))
    app.load_csv()
    total = 0.0
    for _ in range(iterations):
        app.page_redraw_needed = True
        app.cursor_row = 0
        app.cursor_col = 0
        app.cursor_direction = "R"
        start = time.perf_counter()
        app._refresh_rows()
        total += time.perf_counter() - start
    return total / iterations


def time_fast_path(csv_path: Path, iterations: int) -> float:
    app = CSVViewerApp(str(csv_path))
    app.load_csv()
    app.page_redraw_needed = True
    app._refresh_rows()  # build initial widgets
    total = 0.0
    for i in range(iterations):
        app.page_redraw_needed = False
        app.cursor_direction = "R" if i % 2 == 0 else "L"
        app.cursor_col = 1 if i % 2 == 0 else 0
        app.cursor_row = 0
        start = time.perf_counter()
        app._refresh_rows()
        total += time.perf_counter() - start
    return total / iterations


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark cursor redraw paths")
    parser.add_argument("--rows", type=int, default=1000, help="rows in synthetic CSV")
    parser.add_argument("--cols", type=int, default=40, help="columns in synthetic CSV")
    parser.add_argument("--iters", type=int, default=200, help="iterations per mode")
    args = parser.parse_args()

    csv_path = build_csv(args.rows, args.cols)
    try:
        full = time_full_redraw(csv_path, args.iters)
        fast = time_fast_path(csv_path, args.iters)
        print(f"CSV size: {args.rows} rows x {args.cols} cols")
        print(f"Full redraw avg: {full * 1000:.3f} ms over {args.iters} iters")
        print(f"Fast path   avg: {fast * 1000:.3f} ms over {args.iters} iters")
        speedup = full / fast if fast else float("inf")
        print(f"Speedup: {speedup:.2f}x")
    finally:
        csv_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
