"""Main entry point for csvpeek."""

import argparse
from pathlib import Path


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="csvpeek - A snappy, memory-efficient CSV viewer",
        epilog="Example: csvpeek --color-columns data.csv",
    )
    parser.add_argument("csv_path", nargs="?", help="Path to the CSV file")
    parser.add_argument(
        "--color-columns",
        action="store_true",
        help="Color each column using alternating colors",
    )
    parser.add_argument(
        "--column-colors",
        help="Comma-separated list of urwid colors to cycle through for columns",
    )

    args = parser.parse_args(argv)

    if not args.csv_path:
        parser.error("CSV path required")

    if args.csv_path.startswith("-"):
        parser.error(
            "Place options before the CSV path, e.g. csvpeek [OPTIONS] <file.csv>"
        )
    csv_path = args.csv_path
    if not Path(csv_path).exists():
        parser.error(f"File '{csv_path}' not found.")

    colors = None
    if args.column_colors:
        colors = [c.strip() for c in args.column_colors.split(",") if c.strip()]

    return args, csv_path, colors


def main(argv: list[str] | None = None):
    """Main entry point."""
    from csvpeek.csvpeek import CSVViewerApp

    args, csv_path, colors = parse_args(argv)

    app = CSVViewerApp(
        csv_path,
        color_columns=args.color_columns or bool(colors),
        column_colors=colors,
    )
    app.run()


if __name__ == "__main__":
    main()
