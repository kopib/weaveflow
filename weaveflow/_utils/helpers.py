"""
This module provides small, general-purpose helper functions that are shared
across the `weaveflow` package to perform common data manipulation and
type-checking tasks.

These utilities help to reduce code duplication and improve the robustness of
the decorators and core logic. For example, `_dump_str_to_list` is used to
standardize an argument that can be either a single string or a list of strings
into a consistent list format. This simplifies the internal logic of decorators
like `@weave`, which accept such flexible inputs for parameters like `outputs`.
"""


def _dump_str_to_list(s: str | list) -> list[str]:
    """Convert a string to a list of strings."""
    if isinstance(s, str):
        return [s]
    elif isinstance(s, list):
        return s
    else:
        raise TypeError("Argument must be a string or a list of strings.")
