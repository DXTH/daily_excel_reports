"""Tests for the Config class."""

import json
import os
import tempfile

import pytest
import yaml

from src.utils.config import Config
from src.utils.exceptions import ConfigError


def test_config_initialization():
    """Test Config initialization without a config file."""
    config = Config()
    assert config.config_data == {}


def test_config_get_default():
    """Test getting a value with a default."""
    config = Config()
    value = config.get("non_existent_key", "default_value")
    assert value == "default_value"


def test_config_set_get():
    """Test setting and getting values."""
    config = Config()

    # Test simple values
    config.set("key1", "value1")
    assert config.get("key1") == "value1"

    # Test nested values with dot notation
    config.set("nested.key", "nested_value")
    assert config.get("nested.key") == "nested_value"

    # Test deeper nesting
    config.set("deep.nested.key", 123)
    assert config.get("deep.nested.key") == 123
    assert config.get("deep.nested.nonexistent", "default") == "default"


def test_config_load_json():
    """Test loading configuration from a JSON file."""
    test_config = {"test_key": "test_value", "nested": {"key": 123}}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp:
        json.dump(test_config, temp)
        temp_path = temp.name

    try:
        config = Config()
        config.load_config(temp_path)

        assert config.get("test_key") == "test_value"
        assert config.get("nested.key") == 123
    finally:
        os.unlink(temp_path)


def test_config_load_yaml():
    """Test loading configuration from a YAML file."""
    test_config = {"test_key": "test_value", "nested": {"key": 123}}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp:
        yaml.dump(test_config, temp)
        temp_path = temp.name

    try:
        config = Config()
        config.load_config(temp_path)

        assert config.get("test_key") == "test_value"
        assert config.get("nested.key") == 123
    finally:
        os.unlink(temp_path)


def test_config_load_nonexistent_file():
    """Test loading from a nonexistent file raises ConfigError."""
    config = Config()
    with pytest.raises(ConfigError):
        config.load_config("non_existent_file.json")


def test_config_load_invalid_format():
    """Test loading a file with invalid format raises ConfigError."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as temp:
        temp.write("This is not a valid config file")
        temp_path = temp.name

    try:
        config = Config()
        with pytest.raises(ConfigError):
            config.load_config(temp_path)
    finally:
        os.unlink(temp_path)


def test_config_save_json():
    """Test saving configuration to a JSON file."""
    config = Config()
    config.set("test_key", "test_value")
    config.set("nested.key", 123)

    with tempfile.TemporaryDirectory() as temp_dir:
        save_path = os.path.join(temp_dir, "config.json")
        config.save(save_path, format="json")

        # Load saved config to verify
        with open(save_path, "r") as f:
            saved_data = json.load(f)

        assert saved_data["test_key"] == "test_value"
        assert saved_data["nested"]["key"] == 123


def test_config_save_yaml():
    """Test saving configuration to a YAML file."""
    config = Config()
    config.set("test_key", "test_value")
    config.set("nested.key", 123)

    with tempfile.TemporaryDirectory() as temp_dir:
        save_path = os.path.join(temp_dir, "config.yaml")
        config.save(save_path, format="yaml")

        # Load saved config to verify
        with open(save_path, "r") as f:
            saved_data = yaml.safe_load(f)

        assert saved_data["test_key"] == "test_value"
        assert saved_data["nested"]["key"] == 123


def test_config_get_all():
    """Test getting all configuration data."""
    config = Config()
    test_data = {"key1": "value1", "nested": {"key2": "value2"}}

    # Set each item
    for key, value in test_data.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                config.set(f"{key}.{subkey}", subvalue)
        else:
            config.set(key, value)

    # Get all data
    all_data = config.get_all()

    assert all_data == test_data

    # Verify it's a copy
    all_data["key1"] = "modified"
    assert config.get("key1") == "value1"  # Original should be unchanged
