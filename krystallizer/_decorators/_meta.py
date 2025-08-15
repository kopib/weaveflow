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
class SutureMeta:
    """
    Metadata for the Suture class.
    """
    _suture: bool
    _suture_rargs: list[str]
    _suture_oargs: list[str]
    _suture_outputs: list[str]
    _suture_params: dict[str, str]
    _suture_meta: dict[str, str] = None


@dataclass
class RefineMeta:
    """
    Metadata for the Refine class.
    """
    _refine: bool
    _refine_description: str
