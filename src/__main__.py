"""Main entry point for the Daily Excel Reports application."""

import argparse
import sys

from src.cli import main as cli_main
from src.web import main as web_main


def main() -> None:
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Daily Excel Reports - Process Excel and CSV files",
    )

    # Define the main subcommand argument
    parser.add_argument(
        "mode",
        choices=["cli", "web"],
        default="cli",
        nargs="?",
        help="Run mode: command line (cli) or web interface (web)",
    )

    # Parse only the first argument to determine mode
    args, remaining_args = parser.parse_known_args()

    if args.mode == "web":
        # Run web interface
        web_main()
        return 0
    # Run CLI interface
    return cli_main(remaining_args)


if __name__ == "__main__":
    sys.exit(main())
