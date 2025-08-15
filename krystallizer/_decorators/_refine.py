from collections.abc import Iterable
import functools
import inspect
from typing import Callable

from krystallizer._decorators._meta import RefineMeta


def weave_refine(
    _func: Callable = None, *, description: str = None, on_method: str = None
) -> Callable:
    """
    A smart decorator for DataFrame transformation tasks.

    - If applied to a FUNCTION, it tags it with metadata.
    - If applied to a CLASS, it automatically calls a specified method
      (default 'run') upon instantiation and returns the result.
    """

    def decorator(func_or_class: Callable) -> Callable:

        # Define meta data class for refine decorator
        refine_meta = RefineMeta(True, description or func_or_class.__name__)

        if inspect.isclass(func_or_class):

            # Class decoration
            @functools.wraps(func_or_class, updated=())
            def class_wrapper(*args, **kwargs):
                # Create an instance of the original class
                instance = func_or_class(*args, **kwargs)
                # Get the method to be executed
                if not hasattr(instance, on_method):
                    raise AttributeError(
                        f"Instance of {func_or_class.__name__} has no method {on_method!r}"
                    )
                method_to_run = getattr(instance, on_method or "run")

                # Call the method and return its result
                return method_to_run()

            setattr(class_wrapper, "_refine_meta", refine_meta)
            return class_wrapper

        else:

            if on_method is not None:
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
