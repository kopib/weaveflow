"""
This module provides utility functions shared across the decorator implementations.
"""

def _dump_str_to_list(s: str | list) -> list[str]:
    """Convert a string to a list of strings."""
    if isinstance(s, str):
        return [s]
    elif isinstance(s, list):
        return s
    else:
        raise TypeError("Argument must be a string or a list of strings.")
