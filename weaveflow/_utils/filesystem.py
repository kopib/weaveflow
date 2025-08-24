from pathlib import Path
from typing import Iterable


def _handle_files_from_iterable(
    iterable: Iterable[str | Path],
    contain_matching: Iterable[str] | str = None,
    include: bool = True,
) -> list:  # Returning a list is more specific and often better
    """Includes or excludes elements from an iterable based on string matching."""

    if not contain_matching:
        return list(iterable)

    if isinstance(contain_matching, str):
        contain_matching = [contain_matching]

    if not isinstance(contain_matching, Iterable):
        raise TypeError("Argument 'contain_matching' must be a string or an iterable.")

    return [
        item
        for item in iterable
        # - If include=True, it keeps items where a match is found.
        # - If include=False, it keeps items where no match is found.
        if include is any(pattern in Path(item).name for pattern in contain_matching)
    ]

