"""Web interface for the Daily Excel Reports application using Streamlit."""

import json
import os
import sys
import tempfile

import pandas as pd
import streamlit as st

# Add parent directory to path to allow absolute imports
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)  # noqa: PTH100, PTH118, PTH120

from src.processor import DataProcessor
from src.utils.config import Config
from src.utils.exceptions import BaseError
from src.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)

# Configure logging
configure_logging()


def create_sample_config() -> None:
    """Create a sample configuration dictionary.

    Returns:
        Sample configuration dictionary
    """
    return {
        "validation_rules": {
            "excel": {"required_columns": ["Date", "Value"]},
            "csv": {"required_columns": ["Date", "Value"]},
        },
        "csv_options": {"delimiter": ",", "encoding": "utf-8"},
        "concurrency": {"loaders": 4, "transformers": 2},
    }


def display_dataframe_preview(df: pd.DataFrame) -> None:
    """Display a preview of a DataFrame.

    Args:
        df: DataFrame to display
    """
    st.dataframe(df.head(10), use_container_width=True)
    st.caption(f"Showing 10 of {len(df)} rows, {len(df.columns)} columns")


def main() -> None:  # noqa: C901
    """Main entry point for the Streamlit application."""
    st.set_page_config(page_title="Daily Excel Reports", page_icon="📊", layout="wide")

    st.title("Daily Excel Reports")
    st.write("Process Excel and CSV files with configurable transformations")

    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")

        # Configuration options
        config_option = st.radio(
            "Configuration Source",
            ["Default", "Upload Config File", "Edit Config"],
        )

        config = Config()

        if config_option == "Upload Config File":
            config_file = st.file_uploader(
                "Upload YAML or JSON configuration",
                type=["yaml", "yml", "json"],
            )
            if config_file:
                try:
                    # Save uploaded file to a temporary file
                    with tempfile.NamedTemporaryFile(
                        delete=False,
                        suffix=os.path.splitext(config_file.name)[1],  # noqa: PTH122
                    ) as tmp:
                        tmp.write(config_file.getvalue())
                        tmp_path = tmp.name

                    # Load configuration
                    config.load_config(tmp_path)
                    st.success("Configuration loaded successfully")

                    # Clean up temporary file
                    os.unlink(tmp_path)  # noqa: PTH108
                except Exception as e:  # noqa: BLE001
                    st.error(f"Failed to load configuration: {e!s}")

        elif config_option == "Edit Config":
            sample_config = create_sample_config()
            config_text = st.text_area(
                "Edit Configuration (JSON)",
                json.dumps(sample_config, indent=2),
                height=400,
            )

            try:
                config_data = json.loads(config_text)
                # Update configuration
                for key, value in config_data.items():
                    config.set(key, value)
                st.success("Configuration parsed successfully")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e!s}")

        st.header("Output Options")
        output_dir = st.text_input("Output Directory", "output")
        combine_output = st.checkbox(
            "Combine all files into single output with multiple sheets",
        )

    # Main area for file upload and processing
    st.header("Upload Files")
    uploaded_files = st.file_uploader(
        "Upload Excel or CSV files",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        st.write(f"Uploaded {len(uploaded_files)} files")

        # Display previews
        if st.checkbox("Show file previews"):
            for file in uploaded_files:
                st.subheader(file.name)

                try:
                    # Determine file type
                    file_ext = os.path.splitext(file.name)[1].lower()

                    if file_ext in [".xlsx", ".xls"]:
                        df = pd.read_excel(file)
                    elif file_ext == ".csv":
                        df = pd.read_csv(file)
                    else:
                        st.warning(f"Unsupported file type: {file_ext}")
                        continue

                    display_dataframe_preview(df)
                except Exception as e:
                    st.error(f"Error previewing {file.name}: {e!s}")

        # Process button
        if st.button("Process Files"):
            with st.spinner("Processing files..."):
                try:
                    # Save uploaded files to temporary directory
                    temp_dir = tempfile.mkdtemp()
                    file_paths = []

                    for file in uploaded_files:
                        file_path = os.path.join(temp_dir, file.name)
                        with open(file_path, "wb") as f:
                            f.write(file.getvalue())
                        file_paths.append(file_path)

                    # Create output directory
                    os.makedirs(output_dir, exist_ok=True)

                    # Process files
                    processor = DataProcessor(config)
                    output_files = processor.process_files(
                        file_paths,
                        output_dir=output_dir,
                        combine_output=combine_output,
                    )

                    # Display results
                    if output_files:
                        st.success(
                            f"Successfully processed {len(output_files)} output files",
                        )

                        for output_file in output_files:
                            st.write(f"- {os.path.basename(output_file)}")

                            # Allow download of output files
                            with open(output_file, "rb") as file:
                                st.download_button(
                                    label=f"Download {os.path.basename(output_file)}",
                                    data=file,
                                    file_name=os.path.basename(output_file),
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                )
                    else:
                        st.warning("No output files were generated")

                    # Display error summary
                    error_summary = processor.get_error_summary()
                    if error_summary:
                        st.error(f"Completed with {len(error_summary)} errors")
                        for error in error_summary:
                            st.write(f"- {error}")

                except BaseError as e:
                    st.error(f"Application error: {e!s}")
                except Exception as e:
                    st.error(f"Unhandled exception: {e!s}")

                finally:
                    # Clean up temporary files
                    for file_path in file_paths:
                        if os.path.exists(file_path):
                            os.unlink(file_path)
                    if os.path.exists(temp_dir):
                        os.rmdir(temp_dir)

    # Documentation
    with st.expander("Documentation"):
        st.markdown(
            """
        ## Daily Excel Reports
        This application processes Excel and CSV files with configurable transformations.
        ### Features:
        - **Load data** from Excel and CSV files
        - **Apply transformations** to clean, filter, and enhance the data
        - **Export processed data** to formatted Excel files
        - **Error handling** to catch and report issues without crashing
        - **Concurrent processing** of multiple files
        ### Usage:
        1. Configure the application using the sidebar
        2. Upload one or more Excel or CSV files
        3. Optionally preview the uploaded files
        4. Click "Process Files" to start processing
        5. Download the processed output files
        """,
        )


if __name__ == "__main__":
    main()
