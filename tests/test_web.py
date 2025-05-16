"""Tests for web interface functions."""

from unittest import mock

import pandas as pd
import pytest

from src.web import create_sample_config


def test_create_sample_config():
    """Test creating sample configuration."""
    config = create_sample_config()

    # Check structure
    assert "validation_rules" in config
    assert "excel" in config["validation_rules"]
    assert "csv" in config["validation_rules"]
    assert "csv_options" in config
    assert "concurrency" in config

    # Check specific values
    assert "Date" in config["validation_rules"]["excel"]["required_columns"]
    assert "Value" in config["validation_rules"]["excel"]["required_columns"]
    assert config["csv_options"]["delimiter"] == ","
    assert isinstance(config["concurrency"]["loaders"], int)


@mock.patch("src.web.st")
def test_display_dataframe_preview(mock_st):
    """Test the display_dataframe_preview function."""
    from src.web import display_dataframe_preview

    # Create test DataFrame
    df = pd.DataFrame(
        {
            "col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "col2": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"],
        }
    )

    # Call function
    display_dataframe_preview(df)

    # Check st.dataframe was called with head(10)
    mock_st.dataframe.assert_called_once()
    args, _ = mock_st.dataframe.call_args
    assert len(args[0]) == 10

    # Check caption format
    mock_st.caption.assert_called_once()
    args, _ = mock_st.caption.call_args
    caption = args[0]
    assert "12" in caption  # Total rows
    assert "2" in caption  # Number of columns
