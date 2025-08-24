"""
This module implements the '@refine' decorator, which is designed for
tasks that perform sequential, in-place transformations on a DataFrame.
"""

import functools
import inspect
from typing import Any, Callable

from .meta import RefineMeta
from weaveflow._errors import ParamsFromIsNotASpoolError
from weaveflow._utils import _dump_object_to_dict


def _is_refine(f: Callable) -> bool:
    """Check if a function is a refine task."""
    return callable(f) and hasattr(f, "_refine_meta")


def refine(
    _func: Callable = None,
    *,
    description: str = None,
    on_method: str = None,
    params_from: Any = None,
) -> Callable:
    """Decorator to mark a function or class as a 'refine' task for DataFrame transformations.

    Refine tasks are designed for sequential, in-place transformations on a DataFrame,
    such as cleaning, filtering, or grouping. When decorating a class, the `on_method`
    parameter specifies which method within the class should be executed as the
    refinement logic. Parameters can be injected from `@spool` decorated objects.

    Args:
        _func (Callable | None, optional): The function or class to be decorated.
            This argument is automatically populated when `@refine` is used without
            parentheses (e.g., `@refine`). Defaults to None.
        description (str | None, optional): A brief description of the refine task.
            This metadata can be used for documentation or visualization. Defaults to None.
        on_method (str | None, optional): The name of the method within a decorated
            class that should be executed as the refine task. If decorating a function,
            this argument is not allowed. Defaults to "run" if decorating a class
            and not explicitly provided.
        params_from (Any | None, optional): An object decorated with `@spool`
            from which to extract additional parameters for the function or class's
            `__init__` method (for classes) or the function itself (for functions).
            Defaults to None.

    Returns:
        Callable: The decorated function or a wrapper around the decorated class,
            enhanced with refine metadata.

    Raises:
        ValueError: If `on_method` is provided when decorating a function.
        AttributeError: If decorating a class and the specified `on_method` does not exist.
        TypeError: If `params_from` is provided but the object is not
                   decorated with `@spool`.

    Example:
        ```python
        import pandas as pd
        import weaveflow as wf


        @wf.refine(on_method="process", description="Orchestrates the preprocessing steps.")
        class DataPreprocessor:
            def __init__(self, df: pd.DataFrame):
                self.df = df

            def _remove_missing_values(self):
                self.df.dropna(subset=["pe_ratio"], inplace=True)

            def process(self) -> pd.DataFrame:
                self._remove_missing_values()
                return self.df
        ```
    """
    ParamsFromIsNotASpoolError(params_from)
    _on_method_arg = on_method

    def decorator(func_or_class: Callable) -> Callable:
        if _is_refine(func_or_class):
            return func_or_class

        # Get params from object and name of object
        params = _dump_object_to_dict(params_from)
        params_object_name = getattr(params_from, "__name__", None)

        # Determine the on_method at decoration time, not at runtime
        method_to_run_name = None
        if inspect.isclass(func_or_class):
            method_to_run_name = _on_method_arg or "run"

        # Create the complete, immutable metadata object once
        refine_meta = RefineMeta(
            _refine=True,
            _refine_description=description,
            _refine_name=func_or_class.__name__,
            _params=params,
            _params_object=params_object_name,
            _on_method=method_to_run_name,
        )

        # Handle class decoration using on_method for execution plan
        if inspect.isclass(func_or_class):

            @functools.wraps(func_or_class, updated=())
            def class_wrapper(*args, **kwargs):
                instance = func_or_class(*args, **kwargs, **params)
                if not hasattr(instance, method_to_run_name):
                    raise AttributeError(
                        f"Instance of {func_or_class.__name__} has no method {method_to_run_name!r}"
                    )
                method_to_run = getattr(instance, method_to_run_name)

                return method_to_run()

            setattr(class_wrapper, "_refine_meta", refine_meta)
            return class_wrapper
        # Handle function decoration
        else:
            if _on_method_arg is not None:
                raise ValueError("Argument 'on_method' only valid for classes.")

            setattr(func_or_class, "_refine_meta", refine_meta)

            @functools.wraps(func_or_class)
            def func_wrapper(*args, **kwargs):
                return func_or_class(*args, **kwargs, **params)

            return func_wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)
