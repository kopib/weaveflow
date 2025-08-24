"""
This module provides utility functions for filesystem operations, primarily
focused on filtering and handling collections of file paths. These helpers are
used internally to support features that involve reading multiple files from a
directory.

The main function, `_handle_files_from_iterable`, is a key component of the
`@spool` decorator's logic. It allows the decorator to flexibly include or
exclude configuration files from a directory based on user-provided patterns.
For example, a user can specify `include=["config"]` to load only files
containing "config" in their name, or `exclude=["test"]` to ignore test-related
files. This enables more organized and complex configuration management.
"""

from collections.abc import Iterable
from pathlib import Path


def _handle_files_from_iterable(
    iterable: Iterable[str | Path],
    contain_matching: Iterable[str] | str | None = None,
    include: bool = True,
) -> list:  # Returning a list is more specific and often better
    """Includes or excludes elements from an iterable based on string matching."""

    if not contain_matching:
        return list(iterable)

    if isinstance(contain_matching, str):
        contain_matching = [contain_matching]

    if not isinstance(contain_matching, Iterable):
        raise TypeError("Argument 'contain_matching' must be a string or an iterable.")

    return [
        item
        for item in iterable
        # - If include=True, it keeps items where a match is found.
        # - If include=False, it keeps items where no match is found.
        if include is any(pattern in Path(item).name for pattern in contain_matching)
    ]
