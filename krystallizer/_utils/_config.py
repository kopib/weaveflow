from pathlib import Path
from typing import Any
from collections.abc import Iterable


# A private dictionary to hold all package settings.
_settings = {
    "asset_path": None, # TODO: Think about other namespecs
    "exclude_spool": None,
    "include_spool": None,
}


def set_krystallizer_option(options: Iterable[str], value: Any) -> None:
    """
    Set a configuration option for the processflow package.

    Args:
        options (Iterable): The name of the option to set (e.g., 'asset_path').
        value (Any): The value to set for the option.
    """

    if isinstance(options, str):
        options = [options]

    if not isinstance(options, Iterable):
        raise TypeError("Key must be a string or an iterable of strings.")

    for option in options:
        if option not in _settings:
            # TODO: Integrate with logger, warning instead of KeyError
            raise KeyError(f"Invalid option key: {option!r}. Valid options are: {list(_settings.keys())}")

        if not isinstance(option, str):
            raise TypeError("Key must be a string.")

        _settings[option] = value


def _get_option(key: str) -> Any:
    """
    Get a configuration option for the processflow package.

    Args:
        key (str): The name of the option to get.
    """
    return _settings.get(key)
