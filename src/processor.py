"""Main processor module for the Daily Excel Reports application."""

from pathlib import Path

import pandas as pd

from .exporters import (
    ExcelExporter,
    MultiSheetExcelExporter,
    apply_excel_formatting,
    generate_output_filename,
)
from .loaders import CSVLoader, ExcelLoader, load_files_concurrently
from .transformations import (
    TransformationPipeline,
    transform_dataframes_concurrently,
)
from .utils.config import Config
from .utils.logging import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """Main class for processing data files."""

    def __init__(self, config: Config | None = None) -> None:
        """Initialize the data processor.

        Args:
            config: Optional configuration object
        """
        self.config = config or Config()
        self.error_summary: list[str] = []

    def _get_file_loader(self, file_path: str) -> any:
        """Get appropriate loader for file type.

        Args:
            file_path: Path to the file

        Returns:
            DataLoader instance
        """
        file_ext = Path(file_path).suffix.lower()

        if file_ext in [".xlsx", ".xlsm", ".xls"]:
            validation_rules = self.config.get("validation_rules.excel", {})
            return ExcelLoader(validation_rules=validation_rules)

        if file_ext in [".csv"]:
            validation_rules = self.config.get("validation_rules.csv", {})
            delimiter = self.config.get("csv_options.delimiter", ",")
            encoding = self.config.get("csv_options.encoding", "utf-8")
            return CSVLoader(
                delimiter=delimiter,
                encoding=encoding,
                validation_rules=validation_rules,
            )

        msg = f"Unsupported file type: {file_ext}"
        logger.warning(msg)
        return None

    def _create_transformation_pipeline(self) -> TransformationPipeline:
        """Create a transformation pipeline based on configuration.

        Returns:
            TransformationPipeline instance
        """
        from .transformations import ColumnTransformation

        pipeline = TransformationPipeline()

        # Add transformations from configuration if available
        transformations = self.config.get("transformations", [])

        for transform_config in transformations:
            transform_type = transform_config.get("type")
            name = transform_config.get("name", transform_type)

            if transform_type == "column":
                # Dynamic import of transformation function
                module_name = transform_config.get(
                    "function_module",
                    "daily_excel_reports.transformations",
                )
                function_name = transform_config.get("function")

                if function_name:
                    try:
                        module = __import__(module_name, fromlist=[function_name])
                        func = getattr(module, function_name)

                        columns = transform_config.get("columns", [])
                        transformation = ColumnTransformation(name, columns, func)
                        pipeline.add_transformation(transformation)

                    except (ImportError, AttributeError) as e:
                        msg = f"Failed to import transformation function: {e}"
                        logger.exception(msg)

        return pipeline

    def _find_input_files(self, file_patterns: list[str]) -> list[str]:
        """Find all files matching the given patterns.

        Args:
            file_patterns: List of file patterns to process

        Returns:
            List of file paths
        """
        input_files = []
        for pattern in file_patterns:
            # Convert string pattern to Path and resolve glob
            if "*" in pattern:
                # Handle glob pattern
                pattern_path = Path(pattern)
                parent_dir = (
                    pattern_path.parent if pattern_path.parent != Path() else Path.cwd()
                )
                pattern_name = pattern_path.name
                matched_files = [str(p) for p in parent_dir.glob(pattern_name)]
                input_files.extend(matched_files)
            else:
                # Handle direct file path
                input_files.append(pattern)

        if not input_files:
            msg = f"No files found matching patterns: {file_patterns}"
            logger.warning(msg)
        else:
            msg = f"Found {len(input_files)} files to process"
            logger.info(msg)

        return input_files

    def _load_and_transform_data(
        self,
        input_files: list[str],
    ) -> dict[str, pd.DataFrame | Exception]:
        """Load and transform data from input files.

        Args:
            input_files: List of files to process

        Returns:
            Dictionary mapping file paths to transformed DataFrames or exceptions
        """
        # Reset error summary
        self.error_summary = []

        # Step 1: Load all files concurrently
        loaded_data = load_files_concurrently(
            input_files,
            self._get_file_loader,
            max_workers=self.config.get("concurrency.loaders"),
        )

        # Check for loading errors
        for path, result in loaded_data.items():
            if isinstance(result, Exception):
                error_msg = f"Failed to load {path}: {result}"
                self.error_summary.append(error_msg)
                logger.error(error_msg)

        # Step 2: Transform all loaded DataFrames concurrently
        transformed_data = transform_dataframes_concurrently(
            loaded_data,
            self._create_transformation_pipeline,
            max_workers=self.config.get("concurrency.transformers"),
        )

        # Check for transformation errors
        for path, result in transformed_data.items():
            if isinstance(result, Exception):
                error_msg = f"Failed to transform {path}: {result}"
                self.error_summary.append(error_msg)
                logger.error(error_msg)

        return transformed_data

    def _export_combined_output(
        self,
        transformed_data: dict[str, pd.DataFrame | Exception],
        output_dir: str,
    ) -> list[str]:
        """Export all data as a combined Excel file with multiple sheets.

        Args:
            transformed_data: Dictionary of file paths to DataFrame or Exception objects
            output_dir: Directory to save output files

        Returns:
            List of output file paths
        """
        output_files = []

        # Combine all successfully transformed DataFrames into one file
        valid_data = {
            Path(path).stem: data_frame
            for path, data_frame in transformed_data.items()
            if isinstance(data_frame, pd.DataFrame)
        }

        if valid_data:
            try:
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                output_path = Path(output_dir) / f"combined_report_{timestamp}.xlsx"
                exporter = MultiSheetExcelExporter(
                    formatting_func=apply_excel_formatting,
                )
                output_file = exporter.export_multiple(valid_data, str(output_path))
                output_files.append(output_file)
                msg = f"Combined {len(valid_data)} sheets into {output_file}"
                logger.info(msg)
            except Exception as e:  # noqa: BLE001
                error_msg = f"Failed to export combined output: {e}"
                self.error_summary.append(error_msg)
                logger.error(error_msg)  # noqa: TRY400

        return output_files

    def _export_individual_files(
        self,
        transformed_data: dict[str, pd.DataFrame | Exception],
        output_dir: str,
    ) -> list[str]:
        """Export each DataFrame to a separate file.

        Args:
            transformed_data: Dictionary of file paths to DataFrame or Exception objects
            output_dir: Directory to save output files

        Returns:
            List of output file paths
        """
        output_files = []

        # Export each DataFrame to a separate file
        for path, data_frame in transformed_data.items():
            if not isinstance(data_frame, pd.DataFrame):
                continue

            try:
                output_path = generate_output_filename(path, output_dir)
                exporter = ExcelExporter(formatting_func=apply_excel_formatting)
                output_file = exporter.export(data_frame, output_path)
                output_files.append(output_file)
                msg = f"Exported {path} to {output_file}"
                logger.info(msg)

            except Exception as e:  # noqa: BLE001
                error_msg = f"Failed to export {path}: {e}"
                self.error_summary.append(error_msg)
                logger.error(error_msg)  # noqa: TRY400

        return output_files

    def process_files(
        self,
        file_patterns: list[str],
        output_dir: str = "output",
        *,
        combine_output: bool = False,
    ) -> list[str]:
        """Process data files.

        Args:
            file_patterns: List of file patterns to process
            output_dir: Directory for output files
            combine_output: Whether to combine all files into a single output

        Returns:
            List of output file paths
        """
        # Step 1: Find files matching the patterns
        input_files = self._find_input_files(file_patterns)
        if not input_files:
            return []

        # Step 2: Load and transform the data
        transformed_data = self._load_and_transform_data(input_files)

        # Step 3: Export the transformed data
        if combine_output and any(
            isinstance(data_frame, pd.DataFrame)
            for data_frame in transformed_data.values()
        ):
            output_files = self._export_combined_output(transformed_data, output_dir)
        else:
            output_files = self._export_individual_files(transformed_data, output_dir)

        # Log error summary
        if self.error_summary:
            msg = f"Processing completed with {len(self.error_summary)} errors"
            logger.warning(msg)
            for error in self.error_summary:
                f"  - {error}"
                logger.warning(error)
        else:
            logger.info("Processing completed successfully")

        return output_files

    def get_error_summary(self) -> list[str]:
        """Get summary of errors encountered during processing.

        Returns:
            List of error messages
        """
        return self.error_summary.copy()
