from dataclasses import dataclass


@dataclass
class WeaveMeta:
    """
    Metadata for the Weave class.
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
    Metadata for the Refine class.
    """

    _refine: bool
    _refine_description: str
