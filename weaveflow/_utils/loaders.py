from collections.abc import Callable
from inspect import getfile
from pathlib import Path
from typing import Any, Iterable
from pandas import DataFrame

# Import from sibling modules using relative imports to avoid
# circular import
from .parsers import _ConfigReader
from .filesystem import _handle_files_from_iterable


def _load_default_extensions(custom_engine: dict[str, Any] = None) -> list[str]:
    """Load default extensions and add custom ones if provided."""
    default_extensions = ["*.json", "*.yaml", "*.yml", "*.toml"]
    if custom_engine and isinstance(custom_engine, dict):
        for ext in custom_engine:
            ext = ext.lstrip("*").lstrip(".")
            default_extensions.append(f"*.{ext}")

    return default_extensions


def _load_config_data(
    *,
    obj: Callable = None,
    path: str = None,
    exclude: Iterable[str] = None,
    include: Iterable[str] = None,
    specific_file: str = None,
    custom_engine: dict[str, Any] = None,
) -> dict[str, Any]:
    """Helper to find, read, and merge config files for a given object."""

    # It does not make sense to specify both include and exclude
    if exclude and include:
        raise ValueError("Cannot specify both 'exclude' and 'include'.")

    # It does not make sense to specify exclude/include for a specific files
    if (exclude or include) and specific_file:
        raise ValueError("Cannot specify both 'specific_file' and 'exclude/include'.")

    # Assert custom engine is callable
    if not ((custom_engine is None) or isinstance(custom_engine, dict)):
        raise TypeError("Custom engine must be a dict mapping file extensions to read function.")

    # If path specified, use that
    if isinstance(path, (str, Path)):
        parent_dir = Path(path)

        if not parent_dir.exists():
            raise FileNotFoundError(f"Specified path not found: {parent_dir}")

    # Otherwise, use the directory of the object
    elif obj is not None:
        parent_dir = Path(getfile(obj)).parent
    else:
        raise ValueError("Either 'obj' or 'path' must be specified.")

    data = {}
    default_extensions = _load_default_extensions(custom_engine)

    if specific_file:
        config_path = parent_dir / specific_file
        if not config_path.exists():
            raise FileNotFoundError(f"Specified config file not found: {config_path}")
        # Read specific file, can be dict (if config file) or any other type (if custom engine)
        data: dict | DataFrame = _ConfigReader(str(config_path), custom_engine=custom_engine).read()
        # If data is a DataFrame, wrap it in a dict to standardize output
        if isinstance(data, DataFrame):
            data = {config_path.stem: data}
    else:
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

    if not data:
        raise ValueError("Config files found, but no data found in config files.")

    return data


def _file_feeder(path: str):
    """Feeds files to the _load_config_data function and return config data."""
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Specified path not found: {path}.")

    if path.is_file():
        file, path = path.name, path.parent
        return _load_config_data(path=path, specific_file=file)

    elif path.is_dir():
        return _load_config_data(path=path)

    else:
        raise ValueError(f"Path is neither a file nor a directory: {path}.")
