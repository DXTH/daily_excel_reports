"""Configuration and fixtures for pytest."""

import os
import tempfile
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from src.utils.config import Config


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=5),
            "value": [10, 20, 30, 40, 50],
            "category": ["A", "B", "C", "D", "E"],
        }
    )


@pytest.fixture
def sample_config() -> Config:
    """Create a sample Config object for testing."""
    config = Config()
    config.set("validation_rules.excel.required_columns", ["date", "value"])
    config.set("validation_rules.csv.required_columns", ["date", "value"])
    config.set("csv_options.delimiter", ",")
    config.set("csv_options.encoding", "utf-8")
    config.set("concurrency.loaders", 2)
    config.set("concurrency.transformers", 1)
    return config


@pytest.fixture
def temp_excel_file(sample_df) -> Generator[str, None, None]:
    """Create a temporary Excel file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        sample_df.to_excel(temp.name, index=False)
        temp_path = temp.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def temp_csv_file(sample_df) -> Generator[str, None, None]:
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp:
        sample_df.to_csv(temp.name, index=False)
        temp_path = temp.name

    yield temp_path

    # Cleanup
    if Path(temp_path).exists():
        Path(temp_path).unlink()


@pytest.fixture
def temp_output_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
