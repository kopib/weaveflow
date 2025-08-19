from dataclasses import dataclass


@dataclass
class WeaveMeta:
    """
    Metadata for the Weave decorator.
    """

    _weave: bool
    _rargs: list[str]
    _oargs: list[str]
    _outputs: list[str]
    _params: dict[str, str]
    _meta_mapping: dict[str, str] = None


@dataclass
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
