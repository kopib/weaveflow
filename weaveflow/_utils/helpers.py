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


def _auto_convert_time_delta(delta_in_seconds: int | float) -> float:
    """Convert a time delta to human-readable format."""

    # Handle the sign separately
    if delta_in_seconds < 0:
        # Rerun the function with the positive equivalent and add a "-" prefix
        return f"-{_auto_convert_time_delta(abs(delta_in_seconds))}"

    # Convert to the appropriate unit
    if delta_in_seconds < 1:
        return f"{delta_in_seconds * 1000:.1f}ms"
    if delta_in_seconds < 60:
        return f"{delta_in_seconds:.1f}s"
    if delta_in_seconds < 3600:
        return f"{delta_in_seconds / 60:.1f}m"

    return f"{delta_in_seconds / 3600:.1f}h"


def _convert_large_int_to_human_readable(number: int) -> str:
    """Convert a large integer to a human-readable format."""

    if number < 0:
        return f"-{_convert_large_int_to_human_readable(abs(number))}"

    if number < 1_000:
        return str(number)
    if number < 1_000_000:
        return f"{number / 1000:,.1f}k"
    if number < 1_000_000_000:
        return f"{number / 1000000:,.1f}mn"
    return f"{number / 1_000_000_000:,.1f}bn"
