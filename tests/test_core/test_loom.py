import pandas as pd
import pytest

from weaveflow import Loom, rethread, weave
from weaveflow._decorators import WeaveMeta, _is_weave


@weave(outputs="sum")
def add_columns(col1: pd.Series, col2: pd.Series):
    return col1 + col2


@weave(outputs="diff")
def subtract_columns(col1: pd.Series, col2: pd.Series):
    return col2 - col1


@weave(outputs=["mul", "div"])
def calculate_stats(sum: pd.Series, diff: pd.Series, margin: int = 0):
    return pd.DataFrame(
        data={"mul": sum * diff + margin, "div": (sum / diff).astype(int) - margin},
        index=sum.index,
    )


@weave(outputs="scaled_mul")
def scale_sum(mul: pd.Series, scaler: float | int = 2):
    return mul * scaler


@weave(outputs="scaled_col1")
def margin_scaled(col1: pd.Series, scaler: int = 1, margin: int = 0):
    return col1 * scaler + margin


def test_weave_decorator_attributes():
    """Tests weave decorator attributes."""

    assert _is_weave(add_columns)
    assert _is_weave(scale_sum)

    add_columns_weave_meta = add_columns._weave_meta
    scale_sum_weave_meta = scale_sum._weave_meta

    assert isinstance(add_columns_weave_meta, WeaveMeta)
    assert isinstance(scale_sum_weave_meta, WeaveMeta)

    assert add_columns_weave_meta._rargs == ["col1", "col2"]
    assert add_columns_weave_meta._oargs == []
    assert add_columns_weave_meta._outputs == ["sum"]

    assert scale_sum_weave_meta._rargs == ["mul"]
    assert scale_sum_weave_meta._oargs == ["scaler"]
    assert scale_sum_weave_meta._outputs == ["scaled_mul"]


def test_error_on_non_weave(base_dataframe_input):
    """Tests handling of non-weave tasks."""

    # Raise KeyError if some required arguments are not found in database for `Loom`
    with pytest.raises(KeyError):
        Loom(database=base_dataframe_input, tasks=[calculate_stats]).run()

    def invalid_inputs_weave(col1: pd.Series):
        return col1 + 1

    def invalid_optional_arg_weave(constant: int = 42):
        return constant

    def invalid_optional_arg_weave_ninputs(constant: int):
        return constant

    # Raise ValueError if ninputs is not an integer or is negative
    with pytest.raises(ValueError):
        weave(outputs="col1_plus_1", nrargs=2.0)(invalid_inputs_weave)
    with pytest.raises(ValueError):
        # ninputs is not an integer
        weave(outputs="col1_plus_1", nrargs=-1)(invalid_inputs_weave)
    with pytest.raises(ValueError):
        # ninputs is negative
        weave(outputs="constant", nrargs=1)(invalid_optional_arg_weave)
    with pytest.raises(ValueError):
        # ninputs is specified but function has optional arguments
        weave(outputs="constant", nrargs=2)(
            invalid_optional_arg_weave_ninputs
        )  # ninputs greater than number of required arguments


@pytest.mark.parametrize("weave_func", [add_columns, subtract_columns])
def test_weave_runs(base_dataframe, base_dataframe_input, weave_func):
    """Tests weave 'add_columns' and 'subtract_columns' one-by-one."""
    # 'weave_func' is now the actual function object
    loom = Loom(database=base_dataframe_input, tasks=[weave_func])
    loom.run()
    meta = weave_func._weave_meta
    expected_df = base_dataframe[meta._rargs + meta._outputs]
    pd.testing.assert_frame_equal(loom.database, expected_df)


def test_weave_with_multiple_outputs(base_dataframe, base_dataframe_input):
    """Tests whole weave for defined weave tasks."""
    # Test whole weave
    loom = Loom(
        database=base_dataframe_input,
        tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
    )
    loom.run()
    pd.testing.assert_frame_equal(loom.database, base_dataframe)


def test_weave_with_optionals(base_dataframe, base_dataframe_input):
    """Tests whole weave with changed optional arguments for defined weave tasks."""

    # Define attributes for optional arguments
    margin = 2
    scaler = 1

    # Define expected output based on optional arguments
    base_dataframe_modified = base_dataframe.copy()
    base_dataframe_modified["mul"] += margin
    base_dataframe_modified["div"] -= margin
    base_dataframe_modified["scaled_mul"] = base_dataframe_modified["mul"] * scaler

    # --- Test task-specific optionals ---

    # Define optionals arg for 'Loom' via optionals dict
    optionals = {calculate_stats: {"margin": margin}, "scale_sum": {"scaler": scaler}}
    loom = Loom(
        database=base_dataframe_input,
        tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
        optionals=optionals,
    )
    loom.run()
    pd.testing.assert_frame_equal(loom.database, base_dataframe_modified)

    # --- Test global optionals ---

    # Define optionals arg for 'Loom' via kwargs
    loom = Loom(
        database=base_dataframe_input,
        tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
        margin=margin,
        scaler=scaler,
    )
    loom.run()
    pd.testing.assert_frame_equal(loom.database, base_dataframe_modified)

    # --- Test task-specific and global optionals ---

    # Create expected column output
    base_dataframe_modified["scaled_col1"] = base_dataframe_modified["col1"] * 2 + 1

    # Define optionals arg for 'Loom' via kwargs and optionals dict
    loom = Loom(
        database=base_dataframe_input,
        tasks=[
            add_columns,
            subtract_columns,
            calculate_stats,
            scale_sum,
            margin_scaled,
        ],
        optionals={
            margin_scaled: {"scaler": 2, "margin": 1}
        },  # Make task-specific optionals
        margin=margin,
        scaler=scaler,
    )
    loom.run()

    pd.testing.assert_frame_equal(loom.database, base_dataframe_modified)


def test_rethread(base_dataframe_input: pd.DataFrame):
    """Tests rethread function arguments."""

    meta = {"col1": "diff", "col2": "sum"}
    calculate_stats_t = rethread(calculate_stats, meta=meta)
    loom = Loom(database=base_dataframe_input, tasks=[calculate_stats_t])
    loom.run()

    assert (
        list(loom.database.columns)
        == list(meta.keys()) + calculate_stats_t._weave_meta._outputs
    )

    expected_df = base_dataframe_input.copy()
    expected_df["mul"] = expected_df["col1"] * expected_df["col2"]
    expected_df["div"] = (expected_df["col2"] / expected_df["col1"]).astype(int)

    pd.testing.assert_frame_equal(loom.database, expected_df)


def test_unknown_required_argument(base_dataframe_input: pd.DataFrame):
    """Tests handling of unknown required arguments."""

    @weave(outputs="doubled_unknown")
    def add_columns(unknown: int):
        return unknown * 2

    with pytest.raises(KeyError):
        Loom(database=base_dataframe_input, tasks=[add_columns]).run()


def test_unknown_optional_argument(base_dataframe_input: pd.DataFrame):
    """Tests handling of optional arguments."""

    @weave(outputs="modified_unknown")
    def add_columns(col1: int, col2: int, known: int = 1):
        return col1 + col2 + known * 2

    # Make sure no error is raised if unknown optional argument is provided
    loom = Loom(database=base_dataframe_input, tasks=[add_columns], unknown=2)
    loom.run()

    pd.testing.assert_series_equal(
        loom.database["modified_unknown"],
        base_dataframe_input["col1"] + base_dataframe_input["col2"] + 2,
        check_names=False,
    )

    loom = Loom(database=base_dataframe_input, tasks=[add_columns], known=2)
    loom.run()
    pd.testing.assert_series_equal(
        loom.database["modified_unknown"],
        base_dataframe_input["col1"] + base_dataframe_input["col2"] + 4,
        check_names=False,
    )

    loom = Loom(
        database=base_dataframe_input, tasks=[add_columns], optionals={"unknown": 2}
    )
    loom.run()
    pd.testing.assert_series_equal(
        loom.database["modified_unknown"],
        base_dataframe_input["col1"] + base_dataframe_input["col2"] + 2,
        check_names=False,
    )


def test_error_on_unknown_task_type(base_dataframe_input: pd.DataFrame):
    """Tests handling of unknown task type."""

    def unknown_task(col1: int):
        return col1**2

    with pytest.raises(TypeError):
        Loom(database=base_dataframe_input, tasks=[unknown_task])
