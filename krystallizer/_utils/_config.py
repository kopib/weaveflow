from pathlib import Path
from typing import Any


# A private dictionary to hold all package settings.
_settings = {
    "asset_path": None,
}


def set_option(key: str, value: Any) -> None:
    """
    Set a configuration option for the processflow package.

    Args:
        key (str): The name of the option to set (e.g., 'asset_path').
        value (Any): The value to set for the option.
    """
    if key not in _settings:
        raise KeyError(f"Invalid option key: '{key}'. Valid keys are: {list(_settings.keys())}")
    
    # Optional: You can add type validation here if you want
    if key == "asset_path" and not isinstance(value, (str, Path)):
        raise TypeError(f"'{key}' must be a string or a pathlib.Path object.")

    _settings[key] = value


def get_option(key: str) -> Any:
    """
    Get a configuration option for the processflow package.

    Args:
        key (str): The name of the option to get.
    """
    return _settings.get(key)