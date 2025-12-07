"""Pytest fixtures for csvpeek tests."""

import polars as pl
import pytest


@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file with sample data for testing."""
    csv_content = """name,age,city,salary,department
John Doe,28,New York,75000,Engineering
Jane Smith,34,San Francisco,95000,Engineering
Bob Johnson,45,Chicago,65000,Sales
Alice Williams,29,Boston,80000,Marketing
Charlie Brown,52,Seattle,110000,Engineering
Diana Prince,31,Los Angeles,72000,Sales
Eve Davis,27,Austin,68000,Marketing
Frank Miller,41,Denver,88000,Engineering
Grace Lee,36,Portland,91000,Sales
Henry Wilson,33,Miami,77000,Marketing
Sarah Connor,30,New York,82000,Engineering
Michael Scott,42,Scranton,71000,Sales
Pam Beesly,28,Scranton,55000,Sales
Jim Halpert,31,Scranton,67000,Sales
Dwight Schrute,38,Scranton,75000,Sales
Angela Martin,35,Scranton,62000,Accounting
Kevin Malone,40,Scranton,58000,Accounting
Oscar Martinez,37,Scranton,68000,Accounting
Stanley Hudson,55,Scranton,69000,Sales
Phyllis Vance,50,Scranton,60000,Sales"""

    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def sample_dataframe():
    """Create a sample Polars DataFrame for testing."""
    data = {
        "name": [
            "John Doe",
            "Jane Smith",
            "Bob Johnson",
            "Alice Williams",
            "Charlie Brown",
        ],
        "age": [28, 34, 45, 29, 52],
        "city": ["New York", "San Francisco", "Chicago", "Boston", "Seattle"],
        "salary": [75000, 95000, 65000, 80000, 110000],
        "department": [
            "Engineering",
            "Engineering",
            "Sales",
            "Marketing",
            "Engineering",
        ],
    }
    return pl.DataFrame(data)


@pytest.fixture
def sample_lazy_frame(sample_dataframe):
    """Create a sample LazyFrame from the DataFrame."""
    return sample_dataframe.lazy()


@pytest.fixture
def numeric_csv_path(tmp_path):
    """Create a CSV with numeric data for range filtering tests."""
    csv_content = """id,value,score
1,100,85.5
2,150,92.0
3,200,78.3
4,250,88.7
5,300,95.2
6,120,81.4
7,180,89.9
8,220,76.8
9,280,93.5
10,320,97.1"""

    csv_file = tmp_path / "numeric_data.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def special_chars_csv_path(tmp_path):
    """Create a CSV with special characters for testing literal matching."""
    csv_content = """email,url,description
john@example.com,https://example.com,User (admin)
jane.smith@test.org,http://test.org/path,Developer [senior]
bob+filter@mail.nl,https://site.nl/page?id=1,Manager - Sales
alice_w@domain.co.uk,http://domain.co.uk,Analyst * Data
charlie.brown@site.de,https://site.de/test,Engineer | Backend"""

    csv_file = tmp_path / "special_chars.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)
