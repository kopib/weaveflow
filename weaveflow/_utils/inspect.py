import inspect
from collections.abc import Callable


def _get_function_args(
    f: Callable, nrargs: int | None = None
) -> tuple[list[str], list[str]]:
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
                f"Function {f.__name__} requires at least {nrargs} inputs, "
                f"but only {len(required)} were found."
            )
        required = required[:nrargs]
        optional = optional[nrargs:]

    return required, optional


def _dump_object_to_dict(obj: object = None) -> dict:
    """
    Convert parameters from 'params_from' object to dict for passing to
    other objects.

    Args:
        params_from (object): Object to load parameters from. Object is expected
            to be a class with a __dict__ attribute.

    Returns:
        dict: Dictionary of parameters.
    """
    if obj is not None:
        params_object = obj()
        params = getattr(params_object, "__dict__", {})
    else:
        params = {}

    return params
