import pytest
from weaveflow._decorators._meta import WeaveMeta, RefineMeta


def test_weave_meta_immutable():
    """Tests that WeaveMeta is immutable."""
    meta = WeaveMeta(
        _weave=True,
        _rargs=["a", "b"],
        _oargs=["c"],
        _outputs=["d"],
        _params={"e": "f"},
        _meta_mapping={"g": "h"},
    )
    with pytest.raises(AttributeError):
        meta._weave = False
    with pytest.raises(AttributeError):
        meta._rargs = ["x", "y"]
    with pytest.raises(AttributeError):
        meta._oargs = ["x", "y"]
    with pytest.raises(AttributeError):
        meta._outputs = ["x", "y"]
    with pytest.raises(AttributeError):
        meta._params = {"x": "y"}
    with pytest.raises(AttributeError):
        meta._meta_mapping = {"x": "y"}


def test_refine_meta_immutable():
    """Tests that RefineMeta is immutable."""
    meta = RefineMeta(
        _refine=True,
        _refine_description="a",
        _refine_name="b",
        _on_method="c",
        _params={"d": "e"},
        _params_object="f",
    )
    with pytest.raises(AttributeError):
        meta._refine = False
    with pytest.raises(AttributeError):
        meta._refine_description = "x"
    with pytest.raises(AttributeError):
        meta._refine_name = "x"
    with pytest.raises(AttributeError):
        meta._on_method = "x"
    with pytest.raises(AttributeError):
        meta._params = {"x": "y"}
    with pytest.raises(AttributeError):
        meta._params_object = "x"
