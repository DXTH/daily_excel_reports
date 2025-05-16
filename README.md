# Daily Excel Reports

A robust Python system for processing Excel and CSV files with configurable transformations, error handling, and concurrent processing.

## Features

- **Modular Architecture**: Separate modules for loading, transforming, and exporting data
- **Robust Error Handling**: Comprehensive error handling with detailed logs and error summaries
- **Concurrent Processing**: Process multiple files in parallel for better performance
- **Pluggable Transformations**: Easily extend with custom transformation logic
- **Multiple Interfaces**: Command-line and web interfaces

## Installation

```bash
# Using Poetry (recommended)
poetry install

# Using pip
pip install .
```

## Usage

### Command-line Interface

```bash
# Basic usage
excel-reports cli "data/*.xlsx" "reports/*.csv" --output-dir output

# Combine all files into a single output with multiple sheets
excel-reports cli "data/*.xlsx" --combine --output-dir output

# Use a configuration file
excel-reports cli "data/*.xlsx" --config config.yml

# Show help
excel-reports cli --help
```

### Web Interface

```bash
# Start the web interface
excel-reports web
```

Then open your browser to http://localhost:8501

## Configuration

You can configure the application using YAML or JSON files. Example:

```yaml
validation_rules:
  excel:
    required_columns:
      - Date
      - Value
  csv:
    required_columns:
      - Date
      - Value

csv_options:
  delimiter: ","
  encoding: "utf-8"

concurrency:
  loaders: 4
  transformers: 2

transformations:
  - type: column
    name: Clean Strings
    function: clean_string
    columns:
      - Category
      - Description
  
  - type: column
    name: Format Dates
    function: format_date
    columns:
      - Date
      - TransactionDate
```

## Creating Custom Transformations

You can create custom transformations by extending the transformation classes or using the example transformations:

```python
from daily_excel_reports.src.transformations import TransformationPipeline, ColumnTransformation

def uppercase_text(series):
    return series.str.upper() if hasattr(series, 'str') else series

# Create a pipeline
pipeline = TransformationPipeline()

# Add a transformation
pipeline.add_transformation(
    ColumnTransformation("Make Uppercase", ["Name", "Category"], uppercase_text)
)
```

## Development

### Project Structure

```
daily_excel_reports/
├── src/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── web.py
│   ├── loaders.py
│   ├── transformations.py
│   ├── exporters.py
│   ├── processor.py
│   ├── transformations_examples.py
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       ├── exceptions.py
│       └── logging.py
├── tests/
│   └── ...
└── __init__.py
```

### Running Tests

```bash
poetry run pytest
```

## License

MIT
