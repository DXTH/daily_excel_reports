"""Export module for saving processed data to files."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from src.utils.exceptions import ExportError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class Exporter:
    """Base class for data exporters."""

    def __init__(self) -> None:
        """Initialize the exporter."""

    def export(self, data_frame: pd.DataFrame, output_path: str) -> str:
        """Export data to a file.

        Args:
            data_frame: DataFrame to export
            output_path: Path to save the exported data

        Returns:
            Path to the exported file

        Raises:
            ExportError: If the data cannot be exported
        """
        msg = "Subclasses must implement export method"
        raise NotImplementedError(msg)


class ExcelExporter(Exporter):
    """Exporter for Excel files."""

    def __init__(
        self,
        sheet_name: str = "Data",
        formatting_func: Callable[[pd.DataFrame, str], None] | None = None,
    ) -> None:
        """Initialize Excel exporter.

        Args:
            sheet_name: Name of the sheet to create
            formatting_func: Optional function to apply Excel formatting
        """
        super().__init__()
        self.sheet_name = sheet_name
        self.formatting_func = formatting_func

    def export(self, data_frame: pd.DataFrame, output_path: str) -> str:
        """Export DataFrame to Excel file.

        Args:
            data_frame: DataFrame to export
            output_path: Path to save the Excel file

        Returns:
            Path to the exported file

        Raises:
            ExportError: If the data cannot be exported
        """
        try:
            # Ensure the directory exists
            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            msg = f"Exporting {len(data_frame)} rows to {output_path}"
            logger.info(msg)

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                data_frame.to_excel(writer, sheet_name=self.sheet_name, index=False)

                if self.formatting_func:
                    msg = f"Applying formatting to sheet {self.sheet_name}"
                    logger.debug(msg)
                    self.formatting_func(writer, self.sheet_name)

            logger.info(f"Successfully exported data to {output_path}")
            return output_path

        except Exception as e:
            error_msg = f"Failed to export data to {output_path}: {e}"
            logger.exception(error_msg)
            raise ExportError(error_msg) from e


class CSVExporter(Exporter):
    """Exporter for CSV files."""

    def __init__(self, delimiter: str = ",", encoding: str = "utf-8") -> None:
        """Initialize CSV exporter.

        Args:
            delimiter: Field delimiter
            encoding: File encoding
        """
        super().__init__()
        self.delimiter = delimiter
        self.encoding = encoding

    def export(self, data_frame: pd.DataFrame, output_path: str) -> str:
        """Export DataFrame to CSV file.

        Args:
            data_frame: DataFrame to export
            output_path: Path to save the CSV file

        Returns:
            Path to the exported file

        Raises:
            ExportError: If the data cannot be exported
        """
        try:
            # Ensure the directory exists
            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Exporting {len(data_frame)} rows to {output_path}")
            data_frame.to_csv(
                output_path,
                index=False,
                delimiter=self.delimiter,
                encoding=self.encoding,
            )
            logger.info(f"Successfully exported data to {output_path}")
            return output_path

        except Exception as e:
            error_msg = f"Failed to export data to {output_path}: {e}"
            logger.exception(error_msg)
            raise ExportError(error_msg) from e


class MultiSheetExcelExporter(Exporter):
    """Exporter for multi-sheet Excel files."""

    def __init__(
        self,
        formatting_func: Callable[[pd.DataFrame, str], None] | None = None,
    ) -> None:
        """Initialize multi-sheet Excel exporter.

        Args:
            formatting_func: Optional function to apply Excel formatting
        """
        super().__init__()
        self.formatting_func = formatting_func

    def export_multiple(
        self,
        dataframes: dict[str, pd.DataFrame],
        output_path: str,
    ) -> str:
        """Export multiple DataFrames to separate sheets.

        Args:
            dataframes: Dictionary mapping sheet names to DataFrames
            output_path: Path to save the Excel file

        Returns:
            Path to the exported file

        Raises:
            ExportError: If the data cannot be exported
        """
        try:
            # Ensure the directory exists
            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            logger.info(
                f"Exporting {len(dataframes)} sheets to {output_path}",
            )

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                for sheet_name, sheet_data in dataframes.items():
                    if not isinstance(sheet_data, pd.DataFrame):
                        logger.warning(
                            f"Skipping sheet {sheet_name}: not a DataFrame",
                        )
                        continue

                    logger.debug(
                        f"Adding sheet {sheet_name} with {len(sheet_data)} rows",
                    )
                    sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)

                    if self.formatting_func:
                        logger.debug(
                            f"Applying formatting to sheet {sheet_name}",
                        )
                        self.formatting_func(writer, sheet_name)

            logger.info(
                f"Successfully exported {len(dataframes)} sheets to {output_path}",
            )
            return output_path

        except Exception as e:
            error_msg = f"Failed to export multiple sheets to {output_path}: {e}"
            logger.exception(error_msg)
            raise ExportError(error_msg) from e

    def export(self, data_frame: pd.DataFrame, output_path: str) -> str:
        """Export a single DataFrame to an Excel file.

        Args:
            data_frame: DataFrame to export
            output_path: Path to save the Excel file

        Returns:
            Path to the exported file

        Raises:
            ExportError: If the data cannot be exported
        """
        return self.export_multiple({"Data": data_frame}, output_path)


def apply_excel_formatting(writer: any, sheet_name: str) -> None:  # noqa: C901
    """Apply standard Excel formatting to a worksheet.

    Args:
        writer: ExcelWriter object
        sheet_name: Name of the sheet to format
    """
    try:
        # Get the openpyxl workbook and worksheet
        worksheet = writer.sheets[sheet_name]

        # Define styles
        header_font = Font(bold=True)
        header_fill = PatternFill(
            start_color="D9E1F2",
            end_color="D9E1F2",
            fill_type="solid",
        )
        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )

        # Format headers - make them bold with light blue background
        for cell in worksheet[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal="center", vertical="center")

        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter

            for cell in column:
                if cell.value:
                    try:
                        cell_length = len(str(cell.value))
                        max_length = max(max_length, cell_length)
                    except TypeError:
                        ...

            adjusted_width = max_length + 2
            worksheet.column_dimensions[column_letter].width = adjusted_width

    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to apply Excel formatting: {e}")


def generate_output_filename(
    input_path: str,
    output_dir: str,
    suffix: str = "_processed",
) -> str:
    """Generate an output filename based on the input path.

    Args:
        input_path: Path to the input file
        output_dir: Directory for output files
        suffix: Suffix to add to the filename

    Returns:
        Generated output path
    """
    # Create output directory if it doesn't exist
    output_path_obj = Path(output_dir)
    output_path_obj.mkdir(parents=True, exist_ok=True)

    # Get the base filename without extension
    input_path_obj = Path(input_path)
    base_name = input_path_obj.stem

    # Add timestamp
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")

    # Create output path
    output_filename = f"{base_name}{suffix}_{timestamp}.xlsx"
    return str(output_path_obj / output_filename)
