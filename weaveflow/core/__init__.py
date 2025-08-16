"""
core module initialization

This module provides the main API for the krystallizer package, including the PandasWeave class
and the decorators for defining calculation tasks.

*** Importing the main classes and decorators: ***

>>> from krystallizer import weave, rethread
>>> from krystallizer import PandasWeave
"""

from weaveflow.core.loom import PandasWeave, Loom
from weaveflow.core.nxgraph import WeaveGraph

# --- Define main API for krystallizer module ---
__all__ = ["Loom", "PandasWeave", "WeaveGraph"]
