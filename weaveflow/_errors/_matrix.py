"""
This module defines custom exceptions related to the `WeaveMatrix` class,
which is used for creating a tabular representation of a `weaveflow` pipeline.

Having specific exception types improves error handling and provides clearer,
more actionable feedback to the user when they misuse the API.

`InvalidTaskCollectionError`:
This `ValueError` is raised when the `WeaveMatrix` is initialized with a data
structure that does not conform to its expected input format. The matrix
builder requires a dictionary mapping task names to their metadata, and this
exception ensures that malformed inputs are caught early with a descriptive
error message.
"""


class InvalidTaskCollectionError(ValueError):
    """Raised when WeaveMatrix receives an invalid or malformed task collection.

    Expected a mapping of the form:
    {
        task_name: {
            "rargs": list[str],
            "oargs": list[str],
            "outputs": list[str],
            "params": list[str]
            }
    }
    - Missing keys are tolerated (treated as empty), but wrong types are not.
    - All present lists must contain strings only.
    """

    def __init__(self, detail: str):
        """Initialize the InvalidTaskCollectionError with a detailed message."""
        super().__init__(f"Invalid task collection for WeaveMatrix: {detail}")


def _validate_registry_type(registry: dict) -> dict:
    """Validate type of the registry.

    Iterates through the registry and checks that all keys are strings, all values are
    dictionaries, and all metadata keys are strings. Also checks that all metadata
    values are either strings or lists of strings, with the exception of "delta_time"
    which can be a number.

    Args:
        registry (dict): The registry to validate.

    Returns:
        dict: The validated registry.

    Raises:
        InvalidTaskCollectionError: If the registry is not a dictionary
    """
    if not isinstance(registry, dict):
        raise InvalidTaskCollectionError("Registry must be a dictionary")

    for task_name, meta in registry.items():
        if not isinstance(task_name, str):
            raise InvalidTaskCollectionError("Task names must be strings")
        if not isinstance(meta, dict):
            raise InvalidTaskCollectionError("Task metadata must be a dictionary")
        for key, value in meta.items():
            if not isinstance(key, str):
                raise InvalidTaskCollectionError("Metadata keys must be strings")

            if key == "delta_time" and not isinstance(value, (int, float)):
                raise InvalidTaskCollectionError("Delta time must be a number")

            if key != "delta_time" and not isinstance(value, (str, list)):
                raise InvalidTaskCollectionError("Metadata values must be Iterable or str")

            if (
                key != "delta_time"
                and isinstance(value, list)
                and not all(isinstance(v, str) for v in value)
            ):
                raise InvalidTaskCollectionError("Metadata lists must contain strings")

    return registry
