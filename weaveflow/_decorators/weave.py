"""
This module implements the '@weave' and '@rethread' decorators.

'@weave' is the primary decorator for defining data transformation tasks
that create new columns in a DataFrame. It captures metadata about a
function's inputs, outputs, and parameters.

'@rethread' allows for remapping the input and output names of a weave
task without altering its core logic, enhancing its reusability.
"""

from dataclasses import replace
import functools

from .meta import WeaveMeta
from weaveflow._errors import ParamsFromIsNotASpoolError
from weaveflow._utils import _get_function_args, _dump_object_to_dict


def _is_weave(f: callable) -> bool:
    """Check if a function is a weave task."""
    return callable(f) and hasattr(f, "_weave_meta")


def weave(outputs: str | list[str], nrargs: int = None, params_from: object = None) -> callable:
    # TODO: Infer number of inputs if not provided

    if params_from and nrargs is not None:
        raise ValueError(
            "Cannot use 'nrargs' and 'params_from' at the same time. "
            "Please specify either data inputs or parameter inputs, but not both ways."
        )
    ParamsFromIsNotASpoolError(params_from)

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

        # Set function attributes
        required_args, optional_args = _get_function_args(f, nrargs)

        params = _dump_object_to_dict(params_from)
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
    Return a new callable with remapped weave metadata, leaving the original untouched.

    - If 'f' is a weave task, create a thin wrapper that forwards to 'f'
      but carries a copied _weave_meta with an updated _meta_mapping.
    - If 'meta' is not a dict, return the original function unmodified.
    """

    if not _is_weave(f):
        raise TypeError("Function must be a weave task to be transformed.")

    if not isinstance(meta, dict):
        return f

    # Get weave meta data from function
    weave_meta = getattr(f, "_weave_meta")
    # Bind meta data with new meta mapping (defensive copy)
    new_meta = replace(weave_meta, _meta_mapping=dict(meta))

    # Return a new callable that forwards to 'f' and carries the new meta
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        return f(*args, **kwargs)

    setattr(wrapped, "_weave_meta", new_meta)

    return wrapped
