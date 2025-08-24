"""
This module centralizes custom exception types for the weaveflow package,
making them easily importable from a single location.
"""

from ._matrix import InvalidTaskCollectionError
from ._spool import ParamsFromIsNotASpoolError

__all__ = [
    "InvalidTaskCollectionError",
    "ParamsFromIsNotASpoolError",
]
