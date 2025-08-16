from collections.abc import Iterable
import functools
import inspect
from typing import Callable

from weaveflow._decorators._meta import RefineMeta


def _is_refine(f: Callable) -> bool:
    """Check if a function is a refine task."""
    return callable(f) and hasattr(f, "_refine_meta")


def refine(
    _func: Callable = None, *, description: str = None, on_method: str = None
) -> Callable:
    """
    A smart decorator for DataFrame transformation tasks.

    - If applied to a function, it tags it with metadata.
    - If applied to a class, it automatically calls a specified method
      (default 'run') upon instantiation and returns the result.
    """

    # Capture the on_method from the arguments passed to refine
    _on_method_arg = on_method

    def decorator(func_or_class: Callable) -> Callable:

        if _is_refine(func_or_class):
            return func_or_class

        # Define meta data class for refine decorator
        refine_meta = RefineMeta(True, description, func_or_class.__name__)

        if inspect.isclass(func_or_class):

            # Fallback to 'run' if no method is specified
            method_to_run_name = _on_method_arg or "run"

            # Class decoration
            @functools.wraps(func_or_class, updated=())
            def class_wrapper(*args, **kwargs):
                # Create an instance of the original class
                instance = func_or_class(*args, **kwargs)
                # Get the method to be executed
                if not hasattr(instance, method_to_run_name):
                    raise AttributeError(
                        f"Instance of {func_or_class.__name__} has no method {method_to_run_name!r}"
                    )
                method_to_run = getattr(instance, method_to_run_name)
                # In case of class decoration, store the method name in meta data
                setattr(refine_meta, "_on_method", method_to_run_name)
                # Call the method and return its result
                return method_to_run()

            setattr(class_wrapper, "_refine_meta", refine_meta)
            return class_wrapper

        else:

            if _on_method_arg is not None:
                raise ValueError("Argument 'on_method' only valid for classes.")

            # Function decoration
            f = func_or_class
            setattr(f, "_refine_meta", refine_meta)

            @functools.wraps(f)
            def func_wrapper(*args, **kwargs):
                return f(*args, **kwargs)

            return func_wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)
