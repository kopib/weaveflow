"""
This module defines the core metadata structures, `WeaveMeta` and `RefineMeta`,
which are used by the `weaveflow` decorators (`@weave`, `@refine`) to attach
essential information to decorated functions and classes.

These dataclasses are designed to be immutable containers for metadata
ensuring that the pipeline's definition remains consistent and safe from
unintended modifications during runtime. This immutability is enforced by using
`@dataclass(frozen=True)` and by returning defensive copies of mutable
attributes (lists, dicts) via a custom `__getattribute__` method.

`WeaveMeta`:
    Attached by the `@weave` decorator. It captures the signature of a feature
    engineering task, including:
    - `_rargs`: Required input columns from the DataFrame.
    - `_oargs`: Optional input columns.
    - `_outputs`: The names of the new columns the task will create.
    - `_params`: Parameters injected from a `@spool`-decorated object.
    - `_meta_mapping`: A dictionary for remapping input/output names, used by `@reweave`.

`RefineMeta`:
    Attached by the `@refine` decorator. It stores information about a
    DataFrame-level transformation, including:
    - `_refine_description`: A user-provided description of the task.
    - `_on_method`: The name of the method to execute when a class is decorated.
    - `_params`: Parameters injected from a `@spool`-decorated object.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class WeaveMeta:
    """
    Metadata for the Weave decorator.

    This class is frozen (attributes cannot be rebound). To provide strong immutability
    for inner containers, list/dict attributes are returned as copies when accessed,
    so external mutation does not affect the stored metadata.
    """

    _weave: bool
    _rargs: list[str]
    _oargs: list[str]
    _outputs: list[str]
    _params: dict[str, str]
    _meta_mapping: dict[str, str] = None

    def __getattribute__(self, name: str):
        # Intercept container access to return defensive copies
        val = super().__getattribute__(name)
        if name in {"_rargs", "_oargs", "_outputs"} and isinstance(val, list):
            return list(val)
        if name in {"_params", "_meta_mapping"} and isinstance(val, dict):
            return dict(val)
        return val


@dataclass(frozen=True)
class RefineMeta:
    """
    Metadata for the Refine decorator.

    This class is frozen. Container-like attributes (_params) are returned as copies
    when accessed to prevent external mutation. Large objects (_params_object) are
    passed through unchanged.
    """

    _refine: bool
    _refine_description: str
    _refine_name: str
    _on_method: str = None
    _params: dict[str, str] = None
    _params_object: object = None

    def __getattribute__(self, name: str):
        val = super().__getattribute__(name)
        if name == "_params" and isinstance(val, dict):
            return dict(val)
        return val
