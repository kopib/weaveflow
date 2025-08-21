"""
This module implements the '@refine' decorator, which is designed for
tasks that perform sequential, in-place transformations on a DataFrame.
"""
from dataclasses import replace
import functools
import inspect
from typing import Any, Callable

from weaveflow._decorators._meta import RefineMeta
from weaveflow._errors import ParamsFromIsNotASpoolError
from weaveflow._decorators._utils import dump_object_to_dict


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
    """A smart decorator for DataFrame transformation tasks."""
    ParamsFromIsNotASpoolError(params_from)
    _on_method_arg = on_method

    def decorator(func_or_class: Callable) -> Callable:
        if _is_refine(func_or_class):
            return func_or_class

        # Get params from object and name of object
        params = dump_object_to_dict(params_from)
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
