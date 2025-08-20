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
    """

    _refine: bool
    _refine_description: str
    _refine_name: str
    _on_method: str = None
    _params: dict[str, str] = None
    _params_object: object = None
