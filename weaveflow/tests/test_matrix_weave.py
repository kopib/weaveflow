import pandas as pd
from pandas.testing import assert_frame_equal

from weaveflow.core._matrix import WeaveMatrix


def test_weave_matrix_single_required_inputs():
    task_collection = {
        "t1": {
            "rargs": ["x1", "x2"],
            "oargs": [],
            "outputs": [],
            "params": [],
        }
    }
    df = WeaveMatrix(task_collection).build()

    expected = pd.DataFrame(
        {"t1": ["required input", "required input"]}, index=["x1", "x2"]
    )  # rows sorted
    assert_frame_equal(df, expected)


def test_weave_matrix_single_optional_inputs():
    task_collection = {
        "t1": {
            "rargs": [],
            "oargs": ["x1", "x3"],
            "outputs": [],
            "params": [],
        }
    }
    df = WeaveMatrix(task_collection).build()

    expected = pd.DataFrame(
        {"t1": ["optional input", "optional input"]}, index=["x1", "x3"]
    )  # rows sorted
    assert_frame_equal(df, expected)


def test_weave_matrix_single_outputs_only():
    task_collection = {
        "t1": {
            "rargs": [],
            "oargs": [],
            "outputs": ["y1", "y2"],
            "params": [],
        }
    }
    df = WeaveMatrix(task_collection).build()

    expected = pd.DataFrame(
        {"t1": ["Output", "Output"]}, index=["y1", "y2"]
    )  # rows sorted
    assert_frame_equal(df, expected)


def test_weave_matrix_mixed_inputs_and_outputs():
    task_collection = {
        "t1": {
            "rargs": ["a"],
            "oargs": ["b"],
            "outputs": ["c"],
            "params": [],
        }
    }
    df = WeaveMatrix(task_collection).build()

    # Rows are sorted alphabetically: a, b, c
    expected = pd.DataFrame(
        {"t1": ["required input", "optional input", "Output"]}, index=["a", "b", "c"]
    )
    assert_frame_equal(df, expected)


def test_weave_matrix_multiple_tasks_with_overlap():
    task_collection = {
        "t1": {
            "rargs": ["x1", "x2"],
            "oargs": ["x3"],
            "outputs": ["y1"],
            "params": [],
        },
        "t2": {
            "rargs": ["y1"],  # consumes output of t1
            "oargs": ["x2"],
            "outputs": ["z1"],
            "params": [],
        },
    }
    df = WeaveMatrix(task_collection).build()

    # Combined rows sorted: x1, x2, x3, y1, z1
    expected = pd.DataFrame(
        {
            "t1": [
                "required input",  # x1 in rargs(t1)
                "required input",  # x2 in rargs(t1)
                "optional input",  # x3 in oargs(t1)
                "Output",  # y1 in outputs(t1)
                "",  # z1 not in t1
            ],
            "t2": [
                "",  # x1 not in t2
                "optional input",  # x2 in oargs(t2)
                "",  # x3 not in t2
                "required input",  # y1 in rargs(t2)
                "Output",  # z1 in outputs(t2)
            ],
        },
        index=["x1", "x2", "x3", "y1", "z1"],
    )
    assert_frame_equal(df, expected)


def test_weave_matrix_precedence_input_vs_output_same_name():
    # If a name is both an input and an output for the same task, "required input" should win,
    # followed by "optional input", then "Output" (according to the implementation order).
    task_collection = {
        "t1": {
            "rargs": ["x"],
            "oargs": ["y"],
            "outputs": ["x", "y", "z"],  # overlaps on x (required) and y (optional)
            "params": [],
        }
    }
    df = WeaveMatrix(task_collection).build()

    # Rows sorted: x, y, z
    expected = pd.DataFrame(
        {"t1": ["required input", "optional input", "Output"]}, index=["x", "y", "z"]
    )
    assert_frame_equal(df, expected)


def test_weave_matrix_missing_keys_resilient():
    # Missing some keys in meta dict should not raise and should be treated as empty lists
    task_collection = {
        "t1": {"rargs": ["a"]},  # no oargs/outputs/params
        "t2": {"outputs": ["b"]},  # no rargs/oargs/params
    }
    df = WeaveMatrix(task_collection).build()

    # Rows sorted: a, b
    expected = pd.DataFrame(
        {"t1": ["required input", ""], "t2": ["", "Output"]}, index=["a", "b"]
    ).loc[:, ["t1", "t2"]]
    assert_frame_equal(df, expected)


def test_weave_matrix_empty_collection():
    task_collection = {}
    df = WeaveMatrix(task_collection).build()

    expected = pd.DataFrame()
    assert_frame_equal(df, expected)
