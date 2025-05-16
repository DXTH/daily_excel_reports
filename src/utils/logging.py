"""Logging configuration for the Daily Excel Reports application."""

import os
import logging
from datetime import datetime
from typing import Optional


# Default log levels
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# Default log format
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Global logger instance
_logger = None


def configure_logging(
    log_dir: str = "logs",
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    log_format: str = DEFAULT_FORMAT,
) -> None:
    """Configure global logging for the application.

    Args:
        log_dir: Directory to store log files
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_format: Format string for log messages
    """
    global _logger

    if _logger is not None:
        return

    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"daily_excel_reports_{timestamp}.log")

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Create formatters
    formatter = logging.Formatter(log_format)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Create application logger
    _logger = logging.getLogger("daily_excel_reports")
    _logger.info(f"Logging configured. Log file: {log_file}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (defaults to application root logger)

    Returns:
        Logger instance
    """
    # Ensure logging is configured
    if _logger is None:
        configure_logging()

    # Return requested logger
    if name is None:
        return _logger
    else:
        return logging.getLogger(name)
