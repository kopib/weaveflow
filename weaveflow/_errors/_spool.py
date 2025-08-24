"""
This module defines custom exceptions related to the @spool decorator
and its associated functionality for parameter injection.

By centralizing these exceptions, `weaveflow` can provide consistent and
user-friendly error messages that guide developers toward correct usage of the
framework's dependency injection features.

`ParamsFromIsNotASpoolError`:
This `TypeError` is a crucial validation check within decorators like `@weave`
and `@refine`. It is raised when the `params_from` argument is provided with an
object that has not been decorated with `@spool` or `@spool_asset`. This
prevents runtime errors and immediately informs the user that their
configuration or parameter object is not correctly set up for injection,
providing a helpful suggestion to fix the issue.
"""


class ParamsFromIsNotASpoolError(TypeError):
    """
    Raised when an object passed to 'params_from' is not decorated with @spool.
    """

    def __init__(self, passed_object: object):
        if object is not None:
            # Get the type and name of the object that was passed
            obj_type = type(passed_object).__name__
            obj_name = getattr(passed_object, "__name__", str(passed_object))

            # Build a detailed, multi-line error message
            message = (
                f"Argument 'params_from' in the decorator for '{obj_name}' "
                f"received an invalid object."
                f"\n  - Expected: A function or class decorated with @spool."
                f"\n  - Received: An object of type '{obj_type}' named '{obj_name}'."
                f"\n\nSuggestion: Make sure you have decorated '{obj_name}' with "
                f"the @spool decorator."
            )

            # Call the parent class __init__ with the formatted message
            super().__init__(message)
