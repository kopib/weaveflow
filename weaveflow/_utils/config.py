"""
This module manages global configuration settings for the weaveflow package.

It offers a simple, centralized mechanism for setting and retrieving
package-level options that can affect the behavior of various components. This
is particularly useful for defining environment-wide settings without having to
pass them repeatedly to different functions or classes.

The primary use case is setting the `asset_path` for the `@spool_asset`
decorator, allowing a user to define a single, consistent location for all
their configuration files.

The module exposes `set_weaveflow_option` to modify settings and an internal
`_get_option` to retrieve them, providing a controlled interface to a private,
module-level settings dictionary.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

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

    for option, value in zip(options, values, strict=False):
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
