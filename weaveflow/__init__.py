"""
krystallizer module initialization

This module provides the main API for the krystallizer package, including the PandasWeave class
and the decorators for defining calculation tasks.

*** Importing the main classes and decorators: ***

>>> from krystallizer import weave, rethread
>>> from krystallizer import PandasWeave, PandasWeaveGraph

>>> import krystallizer as krysta
"""

from weaveflow.core import PandasWeave, PandasWeaveGraph
from weaveflow._decorators import weave, rethread, refine
from weaveflow._decorators import spool, spool_asset

# --- Define main API for krystallizer module ---
__all__ = [
    "PandasWeave",
    "PandasWeaveGraph",
    "weave",
    "refine",
    "rethread",
    "spool",
    "spool_asset",
]
