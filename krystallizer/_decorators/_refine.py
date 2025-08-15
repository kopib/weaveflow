import functools
import inspect
from typing import Callable


def refine(
    _func: Callable = None, *, description: str = None, on_method: str = "run"
) -> Callable:
    """
    A smart decorator for DataFrame transformation tasks.

    - If applied to a FUNCTION, it tags it with metadata.
    - If applied to a CLASS, it automatically calls a specified method
      (default 'run') upon instantiation and returns the result.
    """

    def decorator(func_or_class: Callable) -> Callable:
        if inspect.isclass(func_or_class):

            # Logic for decorating a CLASS
            @functools.wraps(func_or_class, updated=())
            def class_wrapper(*args, **kwargs):
                # Create an instance of the original class
                instance = func_or_class(*args, **kwargs)
                # Get the method to be executed
                if not hasattr(instance, on_method):
                    raise AttributeError(
                        f"Instance of {func_or_class.__name__} has no method {on_method!r}"
                    )
                method_to_run = getattr(instance, on_method)

                # Call the method and return its result
                return method_to_run()

            # Also tag the wrapper so the ProcessFlow knows what it is
            setattr(class_wrapper, "__refined__", True)
            setattr(
                class_wrapper,
                "_refine_description",
                description or func_or_class.__name__,
            )
            return class_wrapper

        else:
            # Logic for decorating a FUNCTION
            f = func_or_class
            setattr(f, "__refined__", True)
            setattr(f, "_refine_description", description or f.__name__)

            @functools.wraps(f)
            def func_wrapper(*args, **kwargs):
                return f(*args, **kwargs)

            return func_wrapper

    if _func is None:
        # Called with parentheses: @refine() or @refine(on_method=...)
        return decorator
    else:
        # Called without parentheses: @refine
        return decorator(_func)
