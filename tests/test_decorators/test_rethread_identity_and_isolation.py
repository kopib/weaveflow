import inspect

import pandas as pd
import pytest

from weaveflow import Loom, WeaveGraph, reweave, weave


def test_reweave_preserves_identity_and_is_weave():
    """
    Tests that reweave preserves identity and is still a weave task.
    """

    @weave(outputs="y")
    def f(x: pd.Series) -> pd.Series:
        """Original docstring."""
        return x

    g = reweave(f, meta={"x": "x_new", "y": "y_new"})

    # functools.wraps should preserve name and doc
    assert g.__name__ == f.__name__
    assert g.__doc__ == f.__doc__

    # Signature should be identical
    assert inspect.signature(g) == inspect.signature(f)

    # _is_weave should hold for reweaveed callable
    # We infer via attribute since _is_weave is internal
    assert hasattr(g, "_weave_meta")

    # Original metadata untouched
    meta_f = f._weave_meta
    assert meta_f._meta_mapping is None

    # New metadata uses remapping
    meta_g = g._weave_meta
    assert meta_g._meta_mapping == {"x": "x_new", "y": "y_new"}


@pytest.mark.smoke
def test_multi_loom_isolation_between_original_and_reweaveed():
    """
    Tests that original and reweaveed tasks can coexist in different Looms.
    """

    @weave(outputs=["sum"])  # original names
    def add(a: pd.Series, b: pd.Series) -> pd.Series:
        return a + b

    # Create reweaveed variant with renamed input and output
    add2 = reweave(add, meta={"a": "x", "b": "y", "sum": "s"})

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "x": [10, 20], "y": [30, 40]})

    # Loom 1 uses original add
    loom1 = Loom(df, [add])
    loom1.run()

    # Loom 2 uses reweaveed add2
    loom2 = Loom(df, [add2])
    loom2.run()

    # Build matrices and ensure they reflect different names, asserting isolation
    mat1 = WeaveGraph(loom1).build_matrix()
    mat2 = WeaveGraph(loom2).build_matrix()

    # In mat1, a/b are required, sum is Output under column 'add'
    assert mat1.loc["a", "add"] == "required input"
    assert mat1.loc["b", "add"] == "required input"
    assert mat1.loc["sum", "add"] == "Output"

    # In mat2, x/y are required, s is Output under column 'add'
    assert mat2.loc["x", "add"] == "required input"
    assert mat2.loc["y", "add"] == "required input"
    assert mat2.loc["s", "add"] == "Output"

    # The matrices should not be identical
    assert not mat1.equals(mat2)
