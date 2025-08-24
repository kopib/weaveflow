"""
This module provides the `@spool` and `@spool_asset` decorators, which are
central to `weaveflow`'s philosophy of separating configuration from code. These
decorators automate the loading of parameters, constants, and small datasets
from external files directly into Python objects.

Core Functionality:
- **Parameter Externalization**: Instead of hardcoding values (like model
  hyperparameters, thresholds, or file paths) in your pipeline logic, you can
  store them in configuration files (e.g., YAML, JSON, TOML).
- **Automatic Population**: The decorators read these files and inject the
  data as attributes into a decorated class or as an accessible object for a
  decorated function.
- **Extensible File Support**: Natively supports JSON, YAML, and TOML. The
  `custom_engine` parameter allows users to provide their own reader functions
  to support other formats, such as CSVs (using `pandas.read_csv`).

Decorators:
- **`@spool`**: The primary decorator. It can be configured with a specific path,
  file, or include/exclude patterns to find configuration files.
  - When applied to a class, it injects the loaded data as keyword arguments
    into the class's `__init__` method.
  - When applied to a function, it returns an `SPoolRegistry` instance, which
    is a simple object providing attribute-style access to the loaded data.
- **`@spool_asset`**: A convenience wrapper around `@spool`. It is designed to
  load from a pre-configured "assets" directory, which can be set globally
  via `weaveflow.options.set_weaveflow_option`. This simplifies the common use
  case of having a dedicated folder for pipeline assets.

`SPoolRegistry`:
A simple dataclass that holds the loaded configuration data and makes it
accessible via attributes (e.g., `config.my_parameter`). This is the return
type for functions decorated with `@spool`.
"""

import functools
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from inspect import isclass, signature
from pathlib import Path
from typing import Any

from weaveflow._utils import (
    _get_function_args,
    _get_option,
    _load_config_data,
)


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


def spool(
    _func: Callable | None = None,
    *,
    custom_engine: Callable | None = None,
    feed_file: str | None = None,
    file: str | None = None,
    path: str | None = None,
    exclude: Iterable[str] | None = None,
    include: Iterable[str] | None = None,
) -> Callable:
    """
    A decorator that auto-populates an object from config files.

    This decorator reads data from configuration files (JSON, YAML, TOML, or custom)
    located in a specified path and injects this data into the decorated object.

    If applied to a class:
        It wraps the `__init__` method of the class. When an instance of the
        class is created, data from the config files is loaded and passed as
        keyword arguments to the original `__init__` method. Any `kwargs`
        provided during instantiation will override values from the config files.

    If applied to a function:
        It wraps the function to return an `SPoolRegistry` instance. This
        `SPoolRegistry` will contain the data loaded from the config files,
        accessible as attributes. The function's required arguments must be
        present in the loaded data.

    Args:
        _func (Callable | None, optional): The function or class to be decorated.
            This argument is automatically populated when `@spool` is used without
            parentheses (e.g., `@spool`). Defaults to None.
        custom_engine (Callable | None, optional): A dictionary mapping file
            extensions (e.g., "csv") to custom reader functions (e.g., `pd.read_csv`).
            This allows `spool` to load data from formats not natively supported.
            Defaults to None.
        feed_file (str | None, optional): A specific file path to load data from.
            If provided, `path`, `exclude`, and `include` are ignored. Defaults to None.
        file (str | None, optional): The name of a specific file (e.g., "my_config.json")
            within the `path` to load data from. Defaults to None.
        path (str | None, optional): The directory path where configuration files
            are located. If not provided, the directory of the decorated object's
            file is used. Defaults to None.
        exclude (Iterable[str] | None, optional): A list of strings. Files whose
            names contain any of these strings will be excluded from loading.
            Cannot be used with `include`. Defaults to None.
        include (Iterable[str] | None, optional): A list of strings. Only files
            whose names contain any of these strings will be included for loading.
            Cannot be used with `exclude`. Defaults to None.

    Returns:
        Callable: The decorated function or class.

    Raises:
        ValueError: If both `exclude` and `include` are specified.
        ValueError: If both `specific_file` and `exclude`/`include` are specified.
        TypeError: If `custom_engine` is not a dictionary or its values are not callables.
        FileNotFoundError: If the specified `path` or `file` does not exist, or
                           if no config files are found in the specified directory.
        ValueError: If config files are found but contain no data.
        ValueError: If a required argument for a decorated function is not found in the config.

    Example:
        ```python
        from dataclasses import dataclass
        import weaveflow as wf
        import pandas as pd


        # Example 1: Spooling a class with default config files
        # Assumes 'assets/data/market_data.yaml' exists
        @wf.spool_asset
        @dataclass
        class MarketData:
            risk_free_rate: float
            equity_risk_premium: float
            industry_betas: dict[str, float]


        # Example 2: Spooling a class with a custom engine for CSV files
        # Assumes 'assets/data/analyst_ratings.csv' exists
        @wf.spool_asset(custom_engine={"csv": pd.read_csv})
        @dataclass
        class AnalystRatings:
            analyst_ratings_registry: dict[str, pd.DataFrame]


        # Example 3: Spooling a function
        @wf.spool(file="my_function_params.json")
        def get_function_params(param1: str, param2: int):
            # This function will return an SPoolRegistry instance
            # containing param1 and param2 from 'my_function_params.json'
            pass


        # To use the spooled function:
        # params = get_function_params()
        # print(params.param1)
        ```
    """

    def decorator(func_or_class: Callable) -> Callable:
        func_or_class._spool = True

        # Handle class decoration
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
                func_or_class._spool_meta = loaded_data
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

        # Handle function decoration
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
    _func: Callable | None = None,
    *,
    file: str | None = None,
    custom_engine: Callable | None = None,
):
    """
    A wrapper for 'spool' that reads from a pre-configured asset path.
    The path can be set globally via processflow.set_option("asset_path", ...).
    """
    # TODO: Create setup.py to set up folder structure
    asset_path = (
        Path(_get_option("asset_path"))
        or Path(__file__).parent.parent.parent / "assets/static"
    )
    return spool(_func, file=file, path=asset_path, custom_engine=custom_engine)
