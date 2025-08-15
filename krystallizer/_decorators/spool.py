from dataclasses import dataclass, field
from typing import Any
from pathlib import Path
from inspect import getfile, isclass, signature
import functools

from krystallizer._decorators.weave import _get_function_args
from krystallizer._utils._reader import _ConfigReader as _Reader
from krystallizer._utils._config import get_option


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


def _load_config_data(obj: callable = None, path: str = None, specific_file: str = None) -> dict[str, Any]:
    """Helper to find, read, and merge config files for a given object."""

    # If path specified, use that
    if isinstance(path, (str, Path)):
        parent_dir = Path(path)
    # Otherwise, use the directory of the object
    elif obj is not None:
        parent_dir = Path(getfile(obj)).parent
    else:
        raise ValueError("Either 'obj' or 'path' must be specified.")

    data = {}

    if specific_file:
        config_path = parent_dir / specific_file
        if not config_path.exists():
            raise FileNotFoundError(f"Specified config file not found: {config_path}")
        data = _Reader(str(config_path)).read()
    else:
        config_files = []
        for ext in ["*.json", "*.yaml", "*.yml", "*.toml"]:
            config_files.extend(parent_dir.glob(ext))
        
        if not config_files:
            raise FileNotFoundError(f"No config files found in {parent_dir}.")

        for config_file in config_files:
            data.update(_Reader(str(config_file)).read())
    
    return data


def _file_feeder(path: str):
    """Feeds files to the _load_config_data function and return config data."""
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Specified path not found: {path}")
    
    if path.is_file():
        file, path = path.name, path.parent
        return _load_config_data(path=path, specific_file=file)
    
    elif path.is_dir():
        return _load_config_data(path=path)    
    
    else:
        raise ValueError(f"Path is neither a file nor a directory: {path}")


def spool(_func: callable = None, *, feed_file: str = None, file: str = None, path: str = None) -> callable:
    """
    A decorator that auto-populates an object from config files.
    
    - If applied to a class, it wraps __init__ to populate the instance.
    - If applied to a function, it returns a populated InputRegistry instance.
    """
    def decorator(func_or_class: callable) -> callable:

        setattr(func_or_class, "__spool__", True)
        
        # --- Handle class decoration ---
        if isclass(func_or_class):

            original_init = func_or_class.__init__
            
            @functools.wraps(original_init)
            def new_init(self, **kwargs):
                # Load data from config files
                loaded_data = _load_config_data(func_or_class, path, file)
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
            def wrapper(**kwargs):
                # Load data and get required args
                loaded_data = _load_config_data(func_or_class, specific_file=file)
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
        
    # This is the new part that handles the two call patterns
    if _func is None:
        # Return the decorator itself for Python to apply.
        return decorator
    else:
        # The function/class is passed directly as _func. Apply the decorator now.
        return decorator(_func)


def spool_asset(_func: callable = None, *, file: str = None):
    """
    A wrapper for 'spool' that reads from a pre-configured asset path.
    The path can be set globally via processflow.set_option("asset_path", ...).
    """
    # TODO: Create setup.py to set up folder structure
    asset_path = get_option("asset_path") or Path(__file__).parent.parent.parent / "assets/static"
    return spool(_func, file=file, path=asset_path)
