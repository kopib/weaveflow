from collections.abc import Iterable

# Explicit import to avoid circular import error
from weaveflow._utils import _is_weave


class WeaveTaskValidator:
    """Validates that an object is a proper list of weave tasks."""

    def __init__(self, tasks: any):
        self.tasks = tasks

    def validate(self) -> None:
        """
        Performs all validation checks.

        Raises:
            TypeError: If the input is not an iterable.
            InvalidTaskTypeError: If any item is not a valid weave task.
        """
        if not isinstance(self.tasks, Iterable):
            raise InvalidTaskTypeError("'weave_tasks' must be an Iterable.")

        for task in self.tasks:
            if not _is_weave(task):
                raise InvalidTaskTypeError(
                    f"The provided list contains a non-weave task: {task!r}"
                )


class InvalidTaskTypeError(TypeError):
    """Raised when a task list contains an invalid task type."""

    pass
