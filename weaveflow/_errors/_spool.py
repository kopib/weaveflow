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
                f"Argument 'params_from' in the decorator for '{obj_name}' received an invalid object."
                f"\n  - Expected: A function or class decorated with @spool."
                f"\n  - Received: An object of type '{obj_type}' named '{obj_name}'."
                f"\n\nSuggestion: Make sure you have decorated '{obj_name}' with the @spool decorator."
            )

            # Call the parent class __init__ with the formatted message
            super().__init__(message)
