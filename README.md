# csvpeek

> A fast CSV viewer in your terminal - peek at your data instantly ⚡

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10+-blue.svg)

**Csvpeek** is a snappy, memory-efficient CSV viewer built for speed. Powered by [DuckDB](https://duckdb.org/) for fast SQL-backed querying and [Urwid](https://urwid.org/) for a lean terminal UI.

## Features

- **Fast** - DuckDB streaming with LIMIT/OFFSET keeps startup instant, even with huge files
- **Large File Support** - Pagination handles millions of rows without breaking a sweat
- **Cell Selection** - Select and copy ranges with keyboard shortcuts
- **Column Sorting** - Sort by any column instantly
- **Keyboard-First** - Every action is a keystroke away

## Quick Start

### Installation

```bash
uv tool install csvpeek
```

Or install from source:

```bash
git clone https://github.com/giantatwork/csvpeek.git
cd csvpeek
pip install -e .
```

### Usage

```bash
csvpeek your_data.csv
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `/` | Open filter dialog |
| `r` | Reset all filters |
| `Ctrl+D` | Next page |
| `Ctrl+U` | Previous page |
| `s` | Sort current column |
| `c` | Copy selection to clipboard |
| `w` | Save selection to file |
| `Shift+Arrow` | Select cells |
| `Arrow Keys` | Navigate (clears selection) |
| `q` | Quit |

## Usage Examples

### Basic Viewing
Open any CSV file and start navigating immediately:
```bash
csvpeek data.csv
```

### Filtering
1. Press `/` to open the filter dialog
2. Enter filter values for any columns
3. Press `Enter` to apply
4. Filter matches are highlighted in red

**Filter modes:**
- **Literal mode**: Case-insensitive substring search (e.g., `scranton` matches "Scranton")
- **Regex mode**: Start with `/` for regex patterns (e.g., `/^J` matches names starting with J)
  - `/\d+` - Contains digits
  - `/sales|eng` - Contains "sales" OR "eng"
  - `/^test$` - Exactly "test"
  - All regex patterns are case-insensitive

### Sorting
1. Navigate to any column
2. Press `s` to sort by that column
3. Press `s` again to toggle ascending/descending

### Selection & Copy
1. Position cursor on starting cell
2. Hold `Shift` and use arrow keys to select a range
3. Press `c` to copy selection as tab-separated values
4. Paste anywhere with `Ctrl+V`



## Requirements
- Python 3.12+
- DuckDB >= 1.1.0
- Urwid >= 2.1.0
- Pyperclip >= 1.9.0

## License

MIT License - see LICENSE file for details

## Acknowledgments

Built with amazing open-source tools:
- [DuckDB](https://duckdb.org/) - Embedded analytics database
- [Urwid](https://urwid.org/) - Lightweight terminal UI toolkit

## Contact

Found a bug? Have a feature request? [Open an issue](https://github.com/giantatwork/csvpeek/issues)!
