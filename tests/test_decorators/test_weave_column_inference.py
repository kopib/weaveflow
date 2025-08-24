from weaveflow import weave
from weaveflow.core import PandasWeave


@weave(outputs="y1")
def f1(x1: float, x2: float, x3: float, c1: float = 0.0):
    return (x1 + x2) * x3 - c1


@weave(outputs="y2")
def f2(x1: float, x4: float, y1: float, c2: float = 10.0):
    return (x1 + x4) ** y1 - (x1 - x4) ** y1 - c2


def test_weave_inference():
    assert (PandasWeave._infer_columns_from_weaves([f1, f2])) == {
        "x1",
        "x2",
        "x3",
        "x4",
        "y1",
    }
