"""
This module implements the '@weave' and '@reweave' decorators.

`@weave`:
This is the primary decorator in `weaveflow` for defining feature engineering
tasks. It is designed to be applied to functions that perform column-wise
transformations on a pandas DataFrame. A `@weave` function typically takes one
or more pandas Series as input and returns one or more new Series (or values
that can be broadcast into a Series).

The decorator captures critical metadata about the function's signature, which
the `Loom` orchestrator uses to build a dependency graph:
- `outputs`: A required argument that specifies the name(s) of the new
  column(s) the function will create.
- `nrargs`: An optional integer to specify how many of the function's
  positional arguments are input columns from the DataFrame.
- `params_from`: An optional argument to specify a `@spool`-decorated object
  from which to inject parameters (e.g., constants, hyperparameters).

This metadata, stored in a `WeaveMeta` object, allows `weaveflow` to
automatically manage data flow, making the pipeline declarative and easy to
visualize.

`@reweave`:
This decorator provides a powerful mechanism for enhancing the reusability of
`@weave` tasks. It allows you to remap the input and output column names of a
decorated function at runtime without modifying its source code. This is
particularly useful for applying a generic transformation to DataFrames with
different column naming conventions. `@reweave` creates a new, wrapped version
of the function with the updated name mappings.
"""

import functools
from dataclasses import replace

from weaveflow._errors import ParamsFromIsNotASpoolError
from weaveflow._utils import _dump_object_to_dict, _get_function_args, _is_weave

from .meta import WeaveMeta


def weave(
    outputs: str | list[str], nrargs: int | None = None, params_from: object = None
) -> callable:
    """
    Decorator to mark a function as a 'weave' task for DataFrame transformations.

    A 'weave' task is a function that takes one or more pandas Series as input
    and returns one or more new Series (or values that can be broadcast into a
    Series). The decorator captures metadata about the function's signature,
    including input and output columns, and additional parameters that can be
    injected from a `@spool`-decorated object.

    Args:
        outputs (str | list[str]): The name(s) of the new column(s) the function
            will create.
        nrargs (int | None, optional): The number of required input arguments
            that are input columns from the DataFrame. If not provided, it is
            assumed that all arguments are input columns. Defaults to None.
        params_from (object | None, optional): An optional object decorated with
            `@spool` from which to inject parameters (e.g., constants, hyperparameters).
            Defaults to None.

    Returns:
        callable: The decorated function, enhanced with weave metadata.

    Raises:
        ValueError: If both 'nrargs' and 'params_from' are provided.
        TypeError: If 'params_from' is provided but the object is not
                   decorated with `@spool`.
        ValueError: If 'outputs' is not a string or a list of strings.
        ValueError: If 'nrargs' is not a non-negative integer.
    """
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
        f._weave_meta = weave_meta

        # Wrap decoarated function
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    return decorator


def reweave(f: callable, meta: dict[str, str] | None = None) -> callable:
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
    weave_meta = f._weave_meta
    # Bind meta data with new meta mapping (defensive copy)
    new_meta = replace(weave_meta, _meta_mapping=dict(meta))

    # Return a new callable that forwards to 'f' and carries the new meta
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        return f(*args, **kwargs)

    wrapped._weave_meta = new_meta

    return wrapped
