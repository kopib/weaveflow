"""
Main decorator for weave tasks on data frames.
"""

import functools
import inspect

from krystallizer._decorators._meta import WeaveMeta


def _get_function_args(f: callable, nrargs: int = None) -> tuple[list[str], list[str]]:
    """
    Identifies the required and optional arguments of a function.
    Returns two lists: (required_args, optional_args).
    """

    # Check if nargs is provided and is an integer
    if nrargs is not None and not isinstance(nrargs, int):
        raise ValueError("Argument 'nrargs' must be an integer.")

    # Check if nargs is negative
    if isinstance(nrargs, int) and nrargs < 0:
        raise ValueError("Argument 'nrargs' must be a non-negative integer.")

    required = []
    optional = []

    # Get the signature of the function
    sig = inspect.signature(f)

    # Iterate through each parameter in the signature
    for param in sig.parameters.values():
        if param.default == inspect.Parameter.empty:
            required.append(param.name)
        else:
            optional.append(param.name)

    if isinstance(nrargs, int) and nrargs > 0:

        if optional:
            raise ValueError(
                "Function has optional arguments, but 'nrargs' is specified. "
                "Please remove 'nrargs' or the optional arguments."
            )

        if len(required) < nrargs:
            raise ValueError(
                f"Function {f.__name__} requires at least {nrargs} inputs, but only {len(required)} were found."
            )
        required = required[:nrargs]
        optional = optional[nrargs:]

    return required, optional


def _is_weave(f: callable) -> bool:
    """Check if a function is a weave task."""
    return callable(f) and hasattr(f, "_weave_meta")


def weave(
    outputs: str | list[str], nrargs: int = None, params_from: object = None
) -> callable:

    # TODO: Infer number of inputs if not provided

    if params_from and nrargs is not None:
        raise ValueError(
            "Cannot use 'nrargs' and 'params_from' at the same time. "
            "Please specify either data inputs or parameter inputs, but not both ways."
        )

    if params_from is not None:
        if not hasattr(params_from, "_spool"):
            raise TypeError(
                "Argument 'params_from' must be a callable object, "
                "typically a function or class decorated with @spool."
            )

    # Convert string to list
    if isinstance(outputs, str):
        outputs = [outputs]

    # Check if outputs is a list of strings
    if not isinstance(outputs, list):
        raise ValueError("Argument 'outputs' must be a string or a list of strings.")

    def decorator(f: callable):

        # If function is already a weave task, return function
        if _is_weave(f):
            return f

        setattr(f, "_weave", True)

        # Set function attributes
        required_args, optional_args = _get_function_args(f, nrargs)

        if params_from is not None:
            params_object = params_from()
            params = getattr(params_object, "__dict__", {})
        else:
            params = {}

        required_args = [arg for arg in required_args if arg not in params]

        weave_meta = WeaveMeta(
            _weave=True,
            _rargs=required_args,
            _oargs=optional_args,
            _outputs=outputs,
            _params=params,
        )
        setattr(f, "_weave_meta", weave_meta)

        # Wrap decoarated function
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    return decorator


def rethread(f: callable, meta: dict[str, str] = None) -> callable:
    """
    Transform the weave task by renaming the required arguments, optional arguments, and outputs.
    If the function is not a weave task, it will return the function unchanged.
    If the function is a weave task, it will rename the arguments and outputs according to the meta dictionary.
    """

    if not _is_weave(f):
        raise TypeError("Function must be a weave task to be transformed.")

    if not isinstance(meta, dict):
        return f

    # Get weave meta data from function
    weave_meta = getattr(f, "_weave_meta")
    setattr(weave_meta, "_meta_mapping", meta)

    return f
