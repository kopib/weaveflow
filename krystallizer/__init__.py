"""
krystallizer module initialization

This module provides the main API for the krystallizer package, including the PandasWeave class
and the decorators for defining calculation tasks.

*** Importing the main classes and decorators: ***

>>> from krystallizer import weave, rethread
>>> from krystallizer import PandasWeave, PandasWeaveGraph

>>> import krystallizer as krysta
"""

from krystallizer.core import PandasWeave, PandasWeaveGraph
from krystallizer._decorators import weave, rethread, weave_refine
from krystallizer._decorators import spool, spool_asset

# --- Define main API for krystallizer module ---
__all__ = [
    "PandasWeave",
    "PandasWeaveGraph",
    "weave",
    "weave_refine",
    "rethread",
    "spool",
    "spool_asset",
]
