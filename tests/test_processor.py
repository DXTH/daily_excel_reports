"""Tests for the DataProcessor class."""

import os
import tempfile
from unittest import mock

import pandas as pd
import pytest

from src.loaders import CSVLoader, ExcelLoader
from src.processor import DataProcessor
from src.utils.config import Config


def test_processor_initialization():
    """Test DataProcessor initialization."""
    # Test with default config
    processor = DataProcessor()
    assert processor.config is not None
    assert processor.error_summary == []

    # Test with custom config
    config = Config()
    config.set("test_key", "test_value")
    processor = DataProcessor(config)
    assert processor.config.get("test_key") == "test_value"


def test_get_file_loader():
    """Test getting file loaders based on file extension."""
    processor = DataProcessor()

    # Test Excel loader
    excel_loader = processor._get_file_loader("file.xlsx")
    assert isinstance(excel_loader, ExcelLoader)

    # Test CSV loader
    csv_loader = processor._get_file_loader("file.csv")
    assert isinstance(csv_loader, CSVLoader)

    # Test unsupported file type
    unsupported_loader = processor._get_file_loader("file.txt")
    assert unsupported_loader is None


def test_create_transformation_pipeline():
    """Test creating a transformation pipeline."""
    # Create a config with transformations
    config = Config()

    # Create a processor
    processor = DataProcessor(config)

    # Get pipeline
    pipeline = processor._create_transformation_pipeline()

    # Check that pipeline is created (even if empty)
    assert pipeline is not None


def test_process_files_no_files():
    """Test processing with no files found."""
    processor = DataProcessor()

    # Call process_files with non-existent patterns
    output_files = processor.process_files(["non_existent_pattern_*.xlsx"])

    # Check that no output files were returned
    assert output_files == []
    assert len(processor.error_summary) == 0


@mock.patch("src.processor.load_files_concurrently")
def test_process_files_load_error(mock_load_files):
    """Test processing with file loading errors."""
    # Mock load_files_concurrently to return an error
    mock_load_files.return_value = {"file.xlsx": Exception("Mock loading error")}

    # Create processor and call process_files
    processor = DataProcessor()

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
        # Mock glob to return the temp file path
        with mock.patch("glob.glob", return_value=[temp.name]):
            output_files = processor.process_files([temp.name])

            # Check that no output files were returned
            assert output_files == []

            # Check error summary has one error
            assert len(processor.error_summary) == 1
            assert "Mock loading error" in processor.error_summary[0]


@mock.patch("src.processor.load_files_concurrently")
@mock.patch("src.processor.transform_dataframes_concurrently")
def test_process_files_transform_error(mock_transform, mock_load):
    """Test processing with transformation errors."""
    # Create test data
    df = pd.DataFrame({"col1": [1, 2, 3]})

    # Mock loading to succeed
    mock_load.return_value = {"file.xlsx": df}

    # Mock transform to return an error
    mock_transform.return_value = {"file.xlsx": Exception("Mock transform error")}

    # Create processor and call process_files
    processor = DataProcessor()

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
        output_files = processor.process_files([temp.name])

        # Check that no output files were returned
        assert output_files == []

        # Check error summary
        assert len(processor.error_summary) == 1
        assert "Mock transform error" in processor.error_summary[0]


@mock.patch("src.processor.load_files_concurrently")
@mock.patch("src.processor.transform_dataframes_concurrently")
@mock.patch("src.processor.ExcelExporter")
def test_process_files_success(mock_exporter_class, mock_transform, mock_load):
    """Test successful processing of files."""
    # Create test data
    df = pd.DataFrame({"col1": [1, 2, 3]})

    # Mock loading and transformation to succeed
    mock_load.return_value = {"file.xlsx": df}
    mock_transform.return_value = {"file.xlsx": df}

    # Mock exporter
    mock_exporter = mock.MagicMock()
    mock_exporter.export.return_value = "output/file.xlsx"
    mock_exporter_class.return_value = mock_exporter

    # Create processor with specific output directory
    processor = DataProcessor()

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp:
        with tempfile.TemporaryDirectory() as output_dir:
            # Process files
            output_files = processor.process_files([temp.name], output_dir=output_dir)

            # Check that output files were returned
            assert len(output_files) == 1
            assert output_files[0] == "output/file.xlsx"

            # Check error summary is empty
            assert len(processor.error_summary) == 0


@mock.patch("src.processor.load_files_concurrently")
@mock.patch("src.processor.transform_dataframes_concurrently")
@mock.patch("src.processor.MultiSheetExcelExporter")
def test_process_files_combined_output(
    mock_multi_exporter_class, mock_transform, mock_load
):
    """Test processing files with combined output."""
    # Create test data
    df1 = pd.DataFrame({"col1": [1, 2, 3]})
    df2 = pd.DataFrame({"col2": [4, 5, 6]})

    # Mock loading and transformation to succeed
    mock_load.return_value = {"file1.xlsx": df1, "file2.xlsx": df2}
    mock_transform.return_value = {"file1.xlsx": df1, "file2.xlsx": df2}

    # Mock exporter
    mock_exporter = mock.MagicMock()
    mock_exporter.export_multiple.return_value = "output/combined.xlsx"
    mock_multi_exporter_class.return_value = mock_exporter

    # Create processor with specific output directory
    processor = DataProcessor()

    # Mock glob to return the file paths
    with mock.patch("glob.glob", side_effect=lambda x: [x]):
        # Process files with combine_output=True
        output_files = processor.process_files(
            ["file1.xlsx", "file2.xlsx"], output_dir="output", combine_output=True
        )

        # Check that output file was returned
        assert len(output_files) == 1
        assert output_files[0] == "output/combined.xlsx"

        # Check error summary is empty
        assert len(processor.error_summary) == 0

        # Check that export_multiple was called with correct params
        mock_exporter.export_multiple.assert_called_once()
        args = mock_exporter.export_multiple.call_args
        assert len(args[0][0]) == 2  # First arg should be the dictionary with 2 entries


def test_get_error_summary():
    """Test getting error summary."""
    processor = DataProcessor()

    # Add errors to summary
    test_errors = ["Error 1", "Error 2", "Error 3"]
    processor.error_summary = test_errors

    # Get summary
    summary = processor.get_error_summary()

    # Check summary
    assert summary == test_errors
    assert len(summary) == 3
