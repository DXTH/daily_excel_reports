"""Tests for data loaders."""

import os
import tempfile

import pandas as pd
import pytest

from src.loaders import DataLoader, ExcelLoader, CSVLoader, load_files_concurrently
from src.utils.exceptions import FileLoadError


def test_data_loader_validate_required_columns():
    """Test DataLoader validation of required columns."""
    # Create a simple DataFrame
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create DataLoader with validation rules
    loader = DataLoader(
        validation_rules={"required_columns": ["col1", "col2", "missing_col"]}
    )

    # Validate DataFrame
    valid_df, messages = loader.validate(df)

    # Check that validation messages contain missing column error
    assert any("missing_col" in msg for msg in messages)

    # Check that DataFrame is returned unchanged
    pd.testing.assert_frame_equal(df, valid_df)


def test_csv_loader():
    """Test CSVLoader loading a CSV file."""
    # Create a temporary CSV file
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp:
        data.to_csv(temp.name, index=False)
        temp_path = temp.name

    try:
        # Create CSVLoader
        loader = CSVLoader()

        # Load and validate data
        result = loader.load(temp_path)

        # Check DataFrame
        assert len(result) == 3
        assert list(result.columns) == ["col1", "col2"]
    finally:
        os.unlink(temp_path)


def test_csv_loader_with_validation():
    """Test CSVLoader with validation rules."""
    # Create a temporary CSV file with one invalid row
    data = pd.DataFrame(
        {
            "col1": [1, 2, 3],
            "col2": ["a", "b", None],  # None is invalid for non-null validation
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp:
        data.to_csv(temp.name, index=False)
        temp_path = temp.name

    try:
        # Create validator function
        def validate_non_null(series):
            return series.notna()

        # Create CSVLoader with validation rule
        loader = CSVLoader(
            validation_rules={
                "required_columns": ["col1", "col2"],
                "col2": [validate_non_null],
            }
        )

        # Load and validate data
        result = loader.load(temp_path)

        # Check that invalid row was filtered out
        assert len(result) == 2
        assert all(result["col2"].notna())
    finally:
        os.unlink(temp_path)


def test_excel_loader():
    """Test ExcelLoader loading an Excel file."""
    # Create a temporary Excel file
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        data.to_excel(temp.name, index=False)
        temp_path = temp.name

    try:
        # Create ExcelLoader
        loader = ExcelLoader()

        # Load data
        result = loader.load(temp_path)

        # Check DataFrame
        assert len(result) == 3
        assert "col1" in result.columns
        assert "col2" in result.columns
    finally:
        os.unlink(temp_path)


def test_excel_loader_multiple_sheets():
    """Test ExcelLoader loading multiple sheets."""
    # Create data for two sheets
    data1 = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    data2 = pd.DataFrame({"col3": [4, 5, 6], "col4": ["d", "e", "f"]})

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        with pd.ExcelWriter(temp.name) as writer:
            data1.to_excel(writer, sheet_name="Sheet1", index=False)
            data2.to_excel(writer, sheet_name="Sheet2", index=False)
        temp_path = temp.name

    try:
        # Create ExcelLoader with multiple sheet loading
        loader = ExcelLoader(sheet_name=None)  # Load all sheets

        # Load data
        result = loader.load(temp_path)

        # Check combined DataFrame
        assert "sheet" in result.columns  # Sheet name column added
        assert len(result) == 6  # 3 rows from each sheet

        # Check sheet1 data
        sheet1_data = result[result["sheet"] == "Sheet1"]
        assert len(sheet1_data) == 3
        assert "col1" in sheet1_data.columns

        # Check sheet2 data
        sheet2_data = result[result["sheet"] == "Sheet2"]
        assert len(sheet2_data) == 3
        assert "col3" in sheet2_data.columns

    finally:
        os.unlink(temp_path)


def test_load_files_concurrently():
    """Test loading multiple files concurrently."""
    # Create two temporary files
    file_paths = []
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp:
        data.to_csv(temp.name, index=False)
        file_paths.append(temp.name)

    # Create Excel file
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        data.to_excel(temp.name, index=False)
        file_paths.append(temp.name)

    try:
        # Create loader factory function
        def loader_factory(path):
            if path.endswith(".csv"):
                return CSVLoader()
            elif path.endswith((".xlsx", ".xls")):
                return ExcelLoader()
            return None

        # Load files concurrently
        results = load_files_concurrently(file_paths, loader_factory)

        # Check results
        assert len(results) == 2
        for path, result in results.items():
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert "col1" in result.columns
            assert "col2" in result.columns

    finally:
        for path in file_paths:
            if os.path.exists(path):
                os.unlink(path)


def test_loader_file_not_found():
    """Test loading a non-existent file raises FileLoadError."""
    # Create loaders
    csv_loader = CSVLoader()
    excel_loader = ExcelLoader()

    # Test with non-existent files
    with pytest.raises(FileLoadError):
        csv_loader.load("non_existent_file.csv")

    with pytest.raises(FileLoadError):
        excel_loader.load("non_existent_file.xlsx")
