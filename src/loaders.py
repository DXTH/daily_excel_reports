"""File loading module for Excel and CSV data sources."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from src.utils.exceptions import FileLoadError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class DataLoader:
    """Base class for loading data from various sources."""

    def __init__(self, validation_rules: dict | None = None):
        """Initialize the data loader with optional validation rules.

        Args:
            validation_rules: Dictionary of column names and validation functions
        """
        self.validation_rules = validation_rules or {}

    def load(self, file_path: str) -> pd.DataFrame:
        """Load data from a file and validate it.

        Args:
            file_path: Path to the file to load

        Returns:
            DataFrame containing the loaded data

        Raises:
            FileLoadError: If the file cannot be loaded
        """
        raise NotImplementedError("Subclasses must implement load method")

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """Validate the loaded data.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (valid_data, validation_messages)
        """
        validation_messages = []

        # Check required columns
        if "required_columns" in self.validation_rules:
            missing = set(self.validation_rules["required_columns"]) - set(df.columns)
            if missing:
                msg = f"Missing required columns: {', '.join(missing)}"
                validation_messages.append(msg)
                logger.warning(msg)

        # Apply custom validation functions
        valid_mask = pd.Series(True, index=df.index)

        for column, validators in self.validation_rules.items():
            if column == "required_columns":
                continue

            if column not in df.columns:
                continue

            for validator in validators:
                try:
                    result = validator(df[column])
                    if isinstance(result, pd.Series):
                        invalid_rows = ~result
                        if invalid_rows.any():
                            count = invalid_rows.sum()
                            msg = f"{count} rows failed validation for column {column}"
                            validation_messages.append(msg)
                            logger.warning(msg)
                            valid_mask = valid_mask & ~invalid_rows
                except Exception as e:
                    msg = f"Validation error in column {column}: {e!s}"
                    validation_messages.append(msg)
                    logger.error(msg)

        valid_df = df[valid_mask].copy() if not valid_mask.all() else df

        if len(valid_df) < len(df):
            msg = f"Filtered out {len(df) - len(valid_df)} invalid rows"
            validation_messages.append(msg)
            logger.warning(msg)

        return valid_df, validation_messages


class ExcelLoader(DataLoader):
    """Loader for Excel files."""

    def __init__(self, sheet_name: str | int | list | None = 0, **kwargs):
        """Initialize Excel loader.

        Args:
            sheet_name: Sheet name(s) to load
            **kwargs: Additional arguments for DataLoader
        """
        super().__init__(**kwargs)
        self.sheet_name = sheet_name

    def load(self, file_path: str) -> pd.DataFrame:
        """Load data from Excel file.

        Args:
            file_path: Path to Excel file

        Returns:
            DataFrame with the loaded data

        Raises:
            FileLoadError: If the file cannot be loaded
        """
        try:
            logger.info(f"Loading Excel file: {file_path}")
            df = pd.read_excel(file_path, sheet_name=self.sheet_name)

            # If multiple sheets were loaded, combine them
            if isinstance(df, dict):
                combined = pd.concat(
                    [sheet.assign(sheet=name) for name, sheet in df.items()],
                    ignore_index=True,
                )
                logger.info(
                    f"Combined {len(df)} sheets with {len(combined)} total rows",
                )
                valid_df, messages = self.validate(combined)
                return valid_df

            logger.info(f"Loaded {len(df)} rows from {file_path}")
            valid_df, messages = self.validate(df)
            return valid_df

        except Exception as e:
            error_msg = f"Failed to load Excel file {file_path}: {e!s}"
            logger.error(error_msg)
            raise FileLoadError(error_msg) from e


class CSVLoader(DataLoader):
    """Loader for CSV files."""

    def __init__(self, delimiter: str = ",", encoding: str = "utf-8", **kwargs):
        """Initialize CSV loader.

        Args:
            delimiter: Field delimiter
            encoding: File encoding
            **kwargs: Additional arguments for DataLoader
        """
        super().__init__(**kwargs)
        self.delimiter = delimiter
        self.encoding = encoding

    def load(self, file_path: str) -> pd.DataFrame:
        """Load data from CSV file.

        Args:
            file_path: Path to CSV file

        Returns:
            DataFrame with the loaded data

        Raises:
            FileLoadError: If the file cannot be loaded
        """
        try:
            logger.info(f"Loading CSV file: {file_path}")
            df = pd.read_csv(
                file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
            )
            logger.info(f"Loaded {len(df)} rows from {file_path}")
            valid_df, messages = self.validate(df)
            return valid_df

        except Exception as e:
            error_msg = f"Failed to load CSV file {file_path}: {e!s}"
            logger.error(error_msg)
            raise FileLoadError(error_msg) from e


def load_files_concurrently(
    file_paths: list[str],
    loader_factory,
    max_workers: int = None,
) -> dict[str, pd.DataFrame | Exception]:
    """Load multiple files concurrently using ThreadPoolExecutor.

    Args:
        file_paths: List of file paths to load
        loader_factory: Function that returns a loader instance for a file path
        max_workers: Maximum number of worker threads

    Returns:
        Dictionary mapping file paths to DataFrames or exceptions
    """
    results = {}

    def load_file(path):
        try:
            loader = loader_factory(path)
            return path, loader.load(path)
        except Exception as e:
            logger.error(f"Error loading {path}: {e!s}")
            return path, e

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(load_file, path): path for path in file_paths}

        for future in as_completed(futures):
            path, result = future.result()
            results[path] = result

    return results
