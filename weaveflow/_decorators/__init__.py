"""
This module aggregates the core decorators from the sub-modules,
making them accessible under the 'weaveflow._decorators' namespace.
"""

from weaveflow._decorators.meta import WeaveMeta, RefineMeta
from weaveflow._decorators.spool import spool, spool_asset, SPoolRegistry
from weaveflow._decorators.weave import weave, rethread, _is_weave
from weaveflow._decorators.refine import refine, _is_refine


__all__ = [
    # meta.py
    "WeaveMeta",
    "RefineMeta",
    # weave.py
    "weave", 
    "rethread", 
    "_is_weave",
    # spool.py
    "spool", 
    "spool_asset",
    "SPoolRegistry", 
    # refine.py
    "refine",
    "_is_refine"
    ]
