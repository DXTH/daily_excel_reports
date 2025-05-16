"""Command-line interface for the Daily Excel Reports application."""

import os
import sys
import argparse
from typing import List

from .processor import DataProcessor
from .utils.logging import configure_logging, get_logger
from .utils.config import Config
from .utils.exceptions import BaseError

logger = get_logger(__name__)


def parse_arguments(args: List[str] = None):
    """Parse command-line arguments.

    Args:
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Parsed arguments object
    """
    parser = argparse.ArgumentParser(
        description="Process Excel/CSV files with configurable transformations"
    )

    parser.add_argument(
        "files",
        nargs="+",
        help='File patterns to process (e.g., "data/*.xlsx" "reports/*.csv")',
    )

    parser.add_argument(
        "-c", "--config", help="Path to configuration file (YAML or JSON)", default=None
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        help="Output directory for processed files",
        default="output",
    )

    parser.add_argument(
        "--combine",
        action="store_true",
        help="Combine all input files into a single output file with multiple sheets",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )

    parser.add_argument("--log-dir", help="Directory for log files", default="logs")

    return parser.parse_args(args)


def main(args: List[str] = None):
    """Main entry point for the application.

    Args:
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code
    """
    try:
        # Parse arguments
        parsed_args = parse_arguments(args)

        # Configure logging
        import logging

        log_level = getattr(logging, parsed_args.log_level)
        configure_logging(parsed_args.log_dir, console_level=log_level)

        logger.info("Starting Daily Excel Reports")

        # Load configuration
        config = Config()
        if parsed_args.config:
            config.load_config(parsed_args.config)

        # Create processor
        processor = DataProcessor(config)

        # Process files
        output_files = processor.process_files(
            parsed_args.files,
            output_dir=parsed_args.output_dir,
            combine_output=parsed_args.combine,
        )

        # Print summary
        if output_files:
            logger.info(f"Successfully processed {len(output_files)} output files:")
            for file_path in output_files:
                logger.info(f"  - {file_path}")
        else:
            logger.warning("No output files were generated")

        # Check for errors
        error_summary = processor.get_error_summary()
        if error_summary:
            logger.warning(f"Completed with {len(error_summary)} errors")
            return 1
        else:
            logger.info("Processing completed successfully")
            return 0

    except BaseError as e:
        logger.error(f"Application error: {str(e)}")
        return 1
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
