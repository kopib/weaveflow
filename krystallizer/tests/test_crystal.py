import pandas as pd
import pytest
from krystallizer import PandasWeave, weave, rethread
from krystallizer._decorators.weave import _is_weave


@weave(outputs="sum")
def add_columns(col1: pd.Series, col2: pd.Series):
    return col1 + col2


@weave(outputs="diff")
def subtract_columns(col1: pd.Series, col2: pd.Series):
    return col2 - col1


@weave(outputs=["mul", "div"])
def calculate_stats(sum: pd.Series, diff: pd.Series, margin: int = 0):
    return sum * diff + margin, (sum / diff).astype(int) - margin


@weave(outputs="scaled_mul")
def scale_sum(mul: pd.Series, scaler: float | int = 2):
    return mul * scaler


@weave(outputs="scaled_col1")
def margin_scaled(col1: pd.Series, scaler: int = 1, margin: int = 0):
    return col1 * scaler + margin


def test_weave_decorator_attributes():
    assert _is_weave(add_columns)
    assert add_columns._suture_rargs == ["col1", "col2"]
    assert add_columns._suture_oargs == []
    assert add_columns._suture_outputs == ["sum"]
    assert _is_weave(scale_sum)
    assert scale_sum._suture_rargs == ["mul"]
    assert scale_sum._suture_oargs == ["scaler"]
    assert scale_sum._suture_outputs == ["scaled_mul"]


def test_error_on_non_weave(base_dataframe_input):

    # Raise KeyError if some required arguments are not found in database for `PandasWeave`
    with pytest.raises(KeyError):
        PandasWeave(
            database=base_dataframe_input, weave_tasks=[calculate_stats]
        ).run()

    def invalid_inputs_weave(col1: pd.Series):
        return col1 + 1

    def invalid_optional_arg_weave(constant: int = 42):
        return constant

    def invalid_optional_arg_weave_ninputs(constant: int):
        return constant

    # Raise ValueError if ninputs is not an integer or is negative
    with pytest.raises(ValueError):
        weave(outputs="col1_plus_1", nrargs=2.0)(
            invalid_inputs_weave
        )  # ninputs is not an integer
        weave(outputs="col1_plus_1", nrargs=-1)(
            invalid_inputs_weave
        )  # ninputs is negative
        weave(outputs="constant", nrargs=1)(
            invalid_optional_arg_weave
        )  # ninputs is specified but function has optional arguments
        weave(outputs="constant", nrargs=2)(
            invalid_optional_arg_weave_ninputs
        )  # ninputs greater than number of required arguments


@pytest.mark.parametrize("weave_func", [add_columns, subtract_columns])
def test_weave_runs(base_dataframe, base_dataframe_input, weave_func):
    """Tests weave 'add_columns' and 'subtract_columns' one-by-one."""
    # 'weave_func' is now the actual function object
    weave = PandasWeave(
        database=base_dataframe_input, weave_tasks=[weave_func]
    )
    weave.run()
    expected_df = base_dataframe[
        weave_func._suture_rargs + weave_func._suture_outputs
    ]
    pd.testing.assert_frame_equal(weave.database, expected_df)


def test_weave_with_multiple_outputs(base_dataframe, base_dataframe_input):
    """Tests whole weave for defined weave tasks."""
    # Test whole weave
    weave = PandasWeave(
        database=base_dataframe_input,
        weave_tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
    )
    weave.run()
    pd.testing.assert_frame_equal(weave.database, base_dataframe)


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

    # Define optionals arg for 'PandasWeave' via optionals dict
    optionals = {calculate_stats: {"margin": margin}, "scale_sum": {"scaler": scaler}}
    weave = PandasWeave(
        database=base_dataframe_input,
        weave_tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
        optionals=optionals,
    )
    weave.run()
    pd.testing.assert_frame_equal(weave.database, base_dataframe_modified)

    # --- Test global optionals ---

    # Define optionals arg for 'PandasWeave' via kwargs
    weave = PandasWeave(
        database=base_dataframe_input,
        weave_tasks=[add_columns, subtract_columns, calculate_stats, scale_sum],
        margin=margin,
        scaler=scaler,
    )
    weave.run()
    pd.testing.assert_frame_equal(weave.database, base_dataframe_modified)

    # --- Test task-specific and global optionals ---

    # Create expected column output
    base_dataframe_modified["scaled_col1"] = base_dataframe_modified["col1"] * 2 + 1

    # Define optionals arg for 'PandasWeave' via kwargs and optionals dict
    weave = PandasWeave(
        database=base_dataframe_input,
        weave_tasks=[
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
    weave.run()

    pd.testing.assert_frame_equal(weave.database, base_dataframe_modified)


def test_rethread(base_dataframe_input: pd.DataFrame):

    meta = {"col1": "diff", "col2": "sum"}
    calculate_stats_t = rethread(calculate_stats, meta=meta)
    weave = PandasWeave(
        database=base_dataframe_input, weave_tasks=[calculate_stats_t]
    )
    weave.run()

    assert (
        list(weave.database.columns)
        == list(meta.keys()) + calculate_stats_t._suture_outputs
    )

    expected_df = base_dataframe_input.copy()
    expected_df["mul"] = expected_df["col1"] * expected_df["col2"]
    expected_df["div"] = (expected_df["col2"] / expected_df["col1"]).astype(int)

    pd.testing.assert_frame_equal(weave.database, expected_df)
