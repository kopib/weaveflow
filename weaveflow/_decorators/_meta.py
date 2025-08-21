"""
This module defines the metadata structures used by the decorators.

These dataclasses are designed to be immutable containers for metadata
attached to decorated functions and classes, ensuring that the metadata
remains consistent and safe from unintended modifications.
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
