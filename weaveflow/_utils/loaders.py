"""
This module contains the core logic for loading configuration data from files.
It serves as the engine behind the `@spool` and `@spool_asset` decorators,
orchestrating the entire process of finding, parsing, and merging data from
various sources.

The main entry point is `_load_config_data`, which consolidates all the
functionality. Its key responsibilities include:
- **Path Resolution**: Determining the correct directory to search for files,
  either from a user-specified path or by inferring it from the location of
  the decorated object.
- **File Discovery**: Handling different loading scenarios, such as loading a
  single specific file (`_load_config_data_specific`) or globbing a directory
  for multiple files based on supported extensions (`_load_config_data_multiple`).
- **Filtering**: Applying `include` and `exclude` patterns to filter the list
  of discovered files.
- **Parsing**: Using the `_ConfigReader` (from `parsers.py`) to read and parse
  the content of each file, with support for custom engines for non-standard
  file types (e.g., CSV).
- **Merging**: Aggregating the data from all parsed files into a single
  dictionary that can be injected into a task.
"""

from collections.abc import Callable, Iterable
from inspect import getfile
from pathlib import Path
from typing import Any

from pandas import DataFrame

from .filesystem import _handle_files_from_iterable

# Import from sibling modules using relative imports to avoid
# circular import
from .parsers import _ConfigReader


def _load_default_extensions(custom_engine: dict[str, Any] | None = None) -> list[str]:
    """Load default extensions and add custom ones if provided."""
    default_extensions = ["*.json", "*.yaml", "*.yml", "*.toml"]
    if custom_engine and isinstance(custom_engine, dict):
        for ext in custom_engine:
            ext_strip = ext.lstrip("*").lstrip(".")
            default_extensions.append(f"*.{ext_strip}")

    return default_extensions


def _validate_load_config_data_args(
    exclude: Iterable[str],
    include: Iterable[str],
    specific_file: str,
    custom_engine: dict[str, Any],
):
    # It does not make sense to specify both include and exclude
    if exclude and include:
        raise ValueError("Cannot specify both 'exclude' and 'include'.")

    # It does not make sense to specify exclude/include for a specific files
    if (exclude or include) and specific_file:
        raise ValueError("Cannot specify both 'specific_file' and 'exclude/include'.")

    # Assert custom engine is callable
    if not ((custom_engine is None) or isinstance(custom_engine, dict)):
        raise TypeError(
            "Custom engine must be a dict mapping file extensions to read function."
        )


def _load_config_data_specific(
    parent_dir: Path,
    specific_file: str | Path,
    file_feed: str | Path,
    custom_engine: dict[str, Any],
) -> dict[str, Any]:
    """
    Load config data from a specific file, either by specific_file + parent_dir
    or by file_feed. If custom engine is specified, it extends the default engine.
    """
    # If file_feed is specified, use that assuming full path is
    # provided else use parent_dir and specific_file
    if isinstance(file_feed, (str, Path)):
        config_path = file_feed
    else:
        config_path = parent_dir / specific_file
    # Check if file exists and raise error if not
    if not config_path.exists():
        raise FileNotFoundError(f"Specified config file not found: {config_path}")
    # Read specific file, can be dict or any other type (if custom engine)
    data: dict | DataFrame = _ConfigReader(
        str(config_path), custom_engine=custom_engine
    ).read()
    # If data is a DataFrame, wrap it in a dict to standardize output
    if isinstance(data, DataFrame):
        data = {config_path.stem: data}

    return data


def _load_config_data_multiple(
    parent_dir: Path,
    exclude: Iterable[str],
    include: Iterable[str],
    custom_engine: dict[str, Any],
) -> dict[str, Any]:
    """
    Load config data from multiple files in a directory specified
    by parent_dir according to include/exclude patterns and pre-defined
    Reader engine. If custom engine is specified, it extends the default engine.
    """
    data = {}
    default_extensions = _load_default_extensions(custom_engine)

    config_files = []
    for ext in default_extensions:
        config_files.extend(parent_dir.glob(ext))

    # If no spool's found, raise an error
    if not config_files:
        raise FileNotFoundError(f"No config files found in {parent_dir}.")

    # Remove files according to include/exclude
    config_files = _handle_files_from_iterable(
        config_files,
        include or exclude,  # Pass in include or exclude pattern
        include=include is not None,  # If include is specified, include is True
    )

    for config_file in config_files:
        tmp_data = _ConfigReader(config_file, custom_engine=custom_engine).read()

        if isinstance(tmp_data, DataFrame):
            if tmp_data.empty:
                continue

            kname = Path(config_file).stem

            if kname in data:
                # TODO: Only raise warning and adjust kname by file extension
                raise ValueError(
                    f"Duplicate key {kname!r} found in config files."
                    f"Filenames interfers with key from other config file."
                    f"Consider renaming the file when using a custom engine."
                )

            tmp_data = {kname: tmp_data}

        # If file is empty, skip it
        if not tmp_data:
            continue

        data.update(tmp_data)

    return data


def _load_config_data(
    *,
    obj: Callable | None = None,
    path: str | None = None,
    exclude: Iterable[str] | None = None,
    include: Iterable[str] | None = None,
    specific_file: str | None = None,
    file_feed: str | None = None,
    custom_engine: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Helper to find, read, and merge config files for a given object."""

    # Validate arguments before proceeding
    _validate_load_config_data_args(exclude, include, specific_file, custom_engine)

    # If path specified, use that
    if isinstance(path, (str, Path)):
        parent_dir = Path(path)

        if not parent_dir.exists():
            raise FileNotFoundError(f"Specified path not found: {parent_dir}")

    # Otherwise, use the directory of the object
    elif obj is not None:
        parent_dir = Path(getfile(obj)).parent
    elif file_feed is None:
        raise ValueError("Either 'obj', 'path' or 'file_feed' must be specified.")

    # Load config data from file feed specific file or multiple files
    if file_feed is not None:
        data = _load_config_data_specific(None, None, file_feed, custom_engine)
    elif specific_file:
        data = _load_config_data_specific(parent_dir, specific_file, None, custom_engine)
    else:
        data = _load_config_data_multiple(parent_dir, exclude, include, custom_engine)

    if not data:
        raise ValueError("Config files found, but no data found in config files.")

    return data
