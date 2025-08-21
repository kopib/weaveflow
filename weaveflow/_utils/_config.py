"""
This module manages global configuration settings for the weaveflow package.

It provides functions to set and retrieve package-level options, such as
default paths or feature flags, allowing for centralized configuration
management.
"""
from pathlib import Path
from typing import Any
from collections.abc import Iterable


# A private dictionary to hold all package settings.
_settings = {
    "asset_path": None,  # TODO: Think about other namespecs
    "exclude_spool": None,
    "include_spool": None,
}


def set_weaveflow_option(options: Iterable[str], values: Iterable[Any]) -> None:
    """
    Set a configuration option for the processflow package.

    Args:
        options (Iterable): The name of the option to set (e.g., 'asset_path').
        value (Any): The value to set for the option.
    """

    if isinstance(options, str):
        options = [options]

    if isinstance(values, (str, Path)):
        values = [values]

    if not isinstance(options, Iterable):
        raise TypeError("Key must be a string or an iterable of strings.")

    if not isinstance(values, Iterable):
        raise TypeError("Value must be a string or an iterable of strings.")

    for option, value in zip(options, values):
        if option not in _settings:
            # TODO: Integrate with logger, warning instead of KeyError
            raise KeyError(
                f"Invalid option key: {option!r}. Valid options are: {list(_settings.keys())}"
            )

        if not isinstance(option, str):
            raise TypeError("Key must be a string.")
        if not isinstance(value, (str, Path)):
            raise TypeError("Value must be a string or a pathlib.Path.")

        _settings[option] = value


def _get_option(key: str) -> Any:
    """
    Get a configuration option for the processflow package.

    Args:
        key (str): The name of the option to get.
    """
    return _settings.get(key)
