"""
This module centralizes custom exception types for the weaveflow package,
making them easily importable from a single location.
"""

from ._spool import ParamsFromIsNotASpoolError
from ._matrix import InvalidTaskCollectionError

__all__ = [
    "ParamsFromIsNotASpoolError",
    "InvalidTaskCollectionError",
]
