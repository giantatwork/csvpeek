"""Main entry point for csvpeek."""

import sys
from pathlib import Path


def main():
    """Main entry point."""
    from csvpeek.csvpeek import CSVViewerApp

    if len(sys.argv) < 2:
        print("Usage: csvpeek <path_to_csv> | --demo")
        sys.exit(1)

    arg = sys.argv[1]
    demo_mode = arg in {"--demo", "demo", ":demo:"}

    if demo_mode:
        csv_path = "__demo__"
    else:
        csv_path = arg
        if not Path(csv_path).exists():
            print(f"Error: File '{csv_path}' not found.")
            sys.exit(1)

    app = CSVViewerApp(csv_path)
    app.run()


if __name__ == "__main__":
    main()
