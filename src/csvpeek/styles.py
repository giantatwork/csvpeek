"""CSS styling for the csvpeek application."""

APP_CSS = """
Screen {
    background: $surface;
}

FilterModal {
    align: center middle;
    background: rgba(0, 0, 0, 0.5);
}

#filter-dialog {
    width: 80;
    height: auto;
    max-height: 70%;
    background: $panel;
    border: thick $primary;
    padding: 0 1;
}

#filter-title {
    text-style: bold;
    color: $accent;
    margin-bottom: 0;
    padding: 1 0 0 0;
}

#filter-inputs {
    height: auto;
    max-height: 25;
    padding: 0;
}

.filter-row {
    height: 3;
    align: left middle;
    margin: 0;
    padding: 0;
}

.filter-label {
    width: 25;
    height: 3;
    content-align: right middle;
    padding-right: 1;
    color: $text;
}

Input {
    width: 1fr;
    height: 3;
    padding: 0 1;
    margin: 0;
    background: $boost;
    border: tall $primary;
}

Input:focus {
    border: tall $accent;
}

DataTable {
    height: 1fr;
    margin: 0;
}

#status {
    height: 1;
    background: $boost;
    color: $text;
    padding: 0 1;
}

Footer {
    background: $panel;
    dock: bottom;
}
"""
