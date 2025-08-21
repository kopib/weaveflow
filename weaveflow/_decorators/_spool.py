"""
This module provides the '@spool' and '@spool_asset' decorators, which
are used to automatically populate objects with data from configuration files.
"""
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from pathlib import Path
from inspect import getfile, isclass, signature
import functools
from pandas import DataFrame

from weaveflow._decorators._weave import _get_function_args
from weaveflow._utils._reader import _ConfigReader as _Reader
from weaveflow._utils._config import _get_option as get_option


@dataclass
class SPoolRegistry:
    """
    A dataclass that holds key-value data and provides direct
    attribute access to that data.

    Note: This class is designed to be used with the `@spool` decorator.
    It allows you to access the data as attributes, e.g., `data.price`.

    Attention: Overwrite the `__dict__` attribute to allow direct access to attributes.
    """

    _kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Convert the dictionary keys into instance attributes."""
        if self._kwargs:
            for key, value in self._kwargs.items():
                setattr(self, key, value)

        self.__dict__ = dict(self._kwargs)

    @classmethod
    def from_file(cls, path: str) -> "SPoolRegistry":
        """Create an InputRegistry instance from a config file."""
        data = _file_feeder(path)
        return cls(data)


def _handle_files_from_iterable(
    iterable: Iterable[str | Path],
    contain_matching: Iterable[str] | str = None,
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
    obj: callable = None,
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
        data: dict | DataFrame = _Reader(str(config_path), custom_engine=custom_engine).read()
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
            tmp_data = _Reader(config_file, custom_engine=custom_engine).read()

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


def spool(
    _func: callable = None,
    *,
    custom_engine: callable = None,
    feed_file: str = None,
    file: str = None,
    path: str = None,
    exclude: Iterable[str] = None,
    include: Iterable[str] = None,
) -> callable:
    """
    A decorator that auto-populates an object from config files.

    - If applied to a class, it wraps __init__ to populate the instance.
    - If applied to a function, it returns a populated InputRegistry instance.
    """

    def decorator(func_or_class: callable) -> callable:
        setattr(func_or_class, "_spool", True)

        # --- Handle class decoration ---
        if isclass(func_or_class):
            original_init = func_or_class.__init__

            @functools.wraps(original_init)
            def new_init(self, **kwargs) -> Any:
                # Load data from config files
                loaded_data = _load_config_data(
                    obj=func_or_class,
                    path=path,
                    specific_file=file,
                    exclude=exclude,
                    include=include,
                    custom_engine=custom_engine,
                )
                setattr(func_or_class, "_spool_meta", loaded_data)
                # Combine loaded data with runtime kwargs (runtime kwargs win)
                final_args = {**loaded_data, **kwargs}
                # Filter out any args that aren't in the original __init__
                init_params = set(signature(original_init).parameters)
                final_args = {k: v for k, v in final_args.items() if k in init_params}
                # Call the original __init__ with the populated arguments
                original_init(self, **final_args)

            # Replace the class's __init__ with our new one
            func_or_class.__init__ = new_init

            return func_or_class

        # -- Handle function decoration ---
        else:

            @functools.wraps(func_or_class)
            def wrapper(**kwargs) -> SPoolRegistry:
                # Load data and get required args
                loaded_data = _load_config_data(
                    obj=func_or_class,
                    path=path,
                    specific_file=file,
                    exclude=exclude,
                    include=include,
                    custom_engine=custom_engine,
                )
                required_args, _ = _get_function_args(func_or_class)

                # Check that all required args are present
                rargs_dict = {}
                for arg in required_args:
                    if arg not in loaded_data:
                        raise ValueError(f"Required arg '{arg}' not found in config.")
                    rargs_dict[arg] = loaded_data[arg]

                # Return the generic registry object
                return SPoolRegistry(rargs_dict | kwargs)

            return wrapper

    if _func is None:
        # Return the decorator itself for Python to apply.
        return decorator
    else:
        # The function/class is passed directly as _func. Apply the decorator now.
        return decorator(_func)


def spool_asset(
    _func: callable = None,
    *,
    file: str = None,
    custom_engine: callable = None,
):
    """
    A wrapper for 'spool' that reads from a pre-configured asset path.
    The path can be set globally via processflow.set_option("asset_path", ...).
    """
    # TODO: Create setup.py to set up folder structure
    asset_path = (
        Path(get_option("asset_path")) or Path(__file__).parent.parent.parent / "assets/static"
    )
    return spool(_func, file=file, path=asset_path, custom_engine=custom_engine)
