"""Script to generate Excel sample data from CSV."""

from pathlib import Path

import pandas as pd


def create_excel_from_csv(csv_path: str, excel_path: str) -> bool:
    """Create an Excel file from a CSV file.

    Args:
        csv_path: Path to the CSV file
        excel_path: Path to create the Excel file
    """
    try:
        print(f"Reading CSV from {Path(csv_path).absolute()}")
        df = pd.read_csv(csv_path)
        print(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")

        print(f"Writing Excel to {Path(excel_path).absolute()}")
        # Use a simple export with openpyxl engine but no formatting
        df.to_excel(excel_path, index=False, engine="openpyxl")
        print("Excel file created successfully")

        return True
    except Exception as e:
        print(f"Error: {e!s}")
        return False


if __name__ == "__main__":
    # Get the directory of this script
    current_dir = Path(__file__).parent.absolute()
    if not current_dir:  # If the script is run from current directory
        current_dir = Path.cwd()

    print(f"Current directory: {current_dir}")

    # Ensure data directory exists
    data_dir = current_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # Construct paths
    csv_path = current_dir / "sample_data.csv"
    excel_path = data_dir / "sample_data.xlsx"

    # Make sure the CSV file exists
    if not csv_path.exists():
        print(f"Error: CSV file not found at {csv_path}")
    else:
        create_excel_from_csv(str(csv_path), str(excel_path))
