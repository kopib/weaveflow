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
        super().__init__(f"Invalid task collection for WeaveMatrix: {detail}")
