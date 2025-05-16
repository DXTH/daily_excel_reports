"""Configuration management for the Daily Excel Reports application."""

import json
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ConfigError
from .logging import get_logger

logger = get_logger(__name__)


class Config:
    """Configuration manager class."""

    def __init__(self, config_path: str | None = None) -> None:
        """Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file (optional)
        """
        self.config_data: dict[str, Any] = {}
        if config_path:
            self.load_config(config_path)

    def load_config(self, config_path: str) -> None:  # noqa: C901
        """Load configuration from a file.

        Args:
            config_path: Path to the configuration file

        Raises:
            ConfigError: If the configuration file cannot be loaded
        """
        try:
            config_file_path = Path(config_path)
            logger.info(f"Loading configuration from {config_file_path}")

            if not config_file_path.exists():
                msg = f"Configuration file not found: {config_file_path}"
                raise ConfigError(  # noqa: TRY301
                    msg,
                )

            file_ext = config_file_path.suffix.lower()

            if file_ext in (".yaml", ".yml"):
                with config_file_path.open("r", encoding="utf-8") as file:
                    self.config_data = yaml.safe_load(file)

            elif file_ext == ".json":
                with config_file_path.open("r", encoding="utf-8") as file:
                    self.config_data = json.load(file)

            else:
                msg = f"Unsupported configuration file format: {file_ext}"
                raise ConfigError(  # noqa: TRY301
                    msg,
                )

            logger.info("Configuration loaded successfully")

        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            error_msg = f"Failed to load configuration: {e}"
            logger.exception(error_msg)
            raise ConfigError(error_msg) from e

    def get(self, key: str, default: None = None) -> any:
        """Get a configuration value.

        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if the key is not found

        Returns:
            Configuration value or default
        """
        # Support dot notation for nested keys
        keys = key.split(".")
        value = self.config_data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set(self, key: str, value: any) -> None:
        """Set a configuration value.

        Args:
            key: Configuration key (supports dot notation for nested keys)
            value: Value to set
        """
        # Support dot notation for nested keys
        keys = key.split(".")
        config = self.config_data

        # Navigate to the last level
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        # Set the value
        config[keys[-1]] = value

    def save(self, config_path: str, _format: str = "yaml") -> None:
        """Save configuration to a file.

        Args:
            config_path: Path to the configuration file
            format: File format ('yaml' or 'json')

        Raises:
            ConfigError: If the configuration cannot be saved
        """
        try:
            config_file_path = Path(config_path)
            logger.info(f"Saving configuration to {config_file_path}")

            # Create directory if it doesn't exist
            config_file_path.parent.mkdir(parents=True, exist_ok=True)

            if _format.lower() == "yaml":
                with config_file_path.open("w", encoding="utf-8") as file:
                    yaml.dump(self.config_data, file, default_flow_style=False)

            elif _format.lower() == "json":
                with config_file_path.open("w", encoding="utf-8") as file:
                    json.dump(self.config_data, file, indent=2)

            else:
                msg = f"Unsupported configuration format: {_format}"
                raise ConfigError(msg)  # noqa: TRY301

            logger.info("Configuration saved successfully")

        except Exception as e:
            error_msg = f"Failed to save configuration: {e}"
            logger.exception(error_msg)
            raise ConfigError(error_msg) from e

    def get_all(self) -> dict[str, Any]:
        """Get all configuration data.

        Returns:
            Dictionary with all configuration data
        """
        return self.config_data.copy()
