"""
This module centralizes custom exception types for the weaveflow package,
making them easily importable from a single location.
"""

from ._matrix import InvalidTaskCollectionError, _validate_registry_type
from ._spool import ParamsFromIsNotASpoolError

__all__ = [
    "InvalidTaskCollectionError",
    "ParamsFromIsNotASpoolError",
    "_validate_registry_type",
]
