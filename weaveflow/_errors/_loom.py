"""
This module defines custom exceptions related to the `Loom` class,
which is the central orchestrator for executing `weaveflow` pipelines.

Having specific exception types improves error handling and provides clearer,
more actionable feedback to the user when they misuse the API.
"""

from collections.abc import Callable, Iterable

from pandas import DataFrame

from weaveflow._utils import _is_refine, _is_weave


class LoomValidator:
    """Validates that a Loom is properly initialized."""

    def __init__(self, database: DataFrame, optionals: dict, tasks: Iterable[Callable]):
        self.database = database
        self.optionals = optionals
        self.tasks = tasks

    def validate(self) -> None:
        """
        Performs all validation checks.

        Raises:
            TypeError: If the input is not an iterable.
            InvalidTaskTypeError: If any item is not a valid weave task.
        """
        if not isinstance(self.database, DataFrame):
            raise InvalidLoomError("Database must be a pandas DataFrame")
        if not isinstance(self.optionals, dict):
            raise InvalidLoomError("Optionals must be a dictionary")
        if not isinstance(self.tasks, Iterable):
            raise InvalidLoomError("'tasks' must be a Iterable of callables")
        for task in self.tasks:
            if not (_is_weave(task) or _is_refine(task)):
                raise InvalidLoomError(
                    f"Argument 'weave_tasks' contains a non-weave"
                    f"and non-refine task: {task!r}"
                )


class InvalidLoomError(Exception):
    """Raised when Loom receives an invalid or malformed task collection."""

    pass
