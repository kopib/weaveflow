import pytest
from weaveflow._decorators._meta import WeaveMeta, RefineMeta
from weaveflow import weave, rethread


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


def test_weave_meta_list_copy_on_access_and_unchanged():
    """
    Tests that WeaveMeta's list attributes are returned as copies and are unchanged
    by external mutations.
    """
    meta = WeaveMeta(
        _weave=True,
        _rargs=["a", "b"],
        _oargs=["c"],
        _outputs=["d"],
        _params={"e": "f"},
        _meta_mapping={"g": "h"},
    )

    r1 = meta._rargs
    r1.append("x")
    r2 = meta._rargs
    assert r2 == ["a", "b"]  # original unchanged
    assert r1 is not r2  # fresh copy returned

    o1 = meta._oargs
    o1.clear()
    o2 = meta._oargs
    assert o2 == ["c"]
    assert o1 is not o2

    out1 = meta._outputs
    out1.pop()
    out2 = meta._outputs
    assert out2 == ["d"]
    assert out1 is not out2


def test_weave_meta_dict_copy_on_access_and_unchanged_with_none_mapping():
    """
    Tests that WeaveMeta's dict attributes are returned as copies and are unchanged
    by external mutations.
    """
    meta = WeaveMeta(
        _weave=True,
        _rargs=["a"],
        _oargs=[],
        _outputs=["y"],
        _params={"p": "q"},
        _meta_mapping=None,
    )

    p1 = meta._params
    p1["p"] = "changed"
    p2 = meta._params
    assert p1 is not p2
    assert p1 == {"p": "changed"}
    assert p2 == {"p": "q"}
    assert isinstance(p2, dict)

    # None meta_mapping stays None; accessing shouldn't crash
    assert meta._meta_mapping is None


def test_refine_meta_params_copy_on_access_and_object_passthrough():
    """
    Tests that RefineMeta's _params is returned as a copy and is unchanged
    by external mutations. Also tests that _params_object is passed through
    without copying.
    """
    ref_meta = RefineMeta(
        _refine=True,
        _refine_description="desc",
        _refine_name="name",
        _on_method=None,
        _params={"a": 1},
        _params_object={"big": "obj"},
    )

    d1 = ref_meta._params
    d1["a"] = 2
    d2 = ref_meta._params
    assert d2 == {"a": 1}
    assert isinstance(d2, dict)

    # params_object is passed through without copying
    obj1 = ref_meta._params_object
    obj2 = ref_meta._params_object
    assert obj1 is obj2


def test_rethread_defensive_copy_of_mapping():
    """
    Tests that rethreading a weave task with a meta mapping does not affect
    the original mapping.
    """

    # Ensure the meta mapping passed to rethread is defensively copied
    @weave(outputs="y")
    def f(x):
        return x

    m = {"x": "x_new", "y": "y_new"}
    g = rethread(f, meta=m)

    # Mutate original mapping after rethread
    m["x"] = "CHANGED"

    # Access the new meta and confirm mapping did not change internally
    new_meta = getattr(g, "_weave_meta")
    assert new_meta._meta_mapping == {"x": "x_new", "y": "y_new"}
    # And ensure type remains dict for compatibility
    assert isinstance(new_meta._meta_mapping, dict)
