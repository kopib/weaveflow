"""
This module tests the column selection functionality of the Loom class.

It verifies that the Loom correctly handles the selection of columns for
weave and refine tasks, including the handling of special values like "all"
and "none".
"""

import pytest
from pandas import DataFrame

from weaveflow import Loom, refine, weave


@weave(outputs="z1")
def f1(col1: int):
    return col1 + 1


@weave(outputs="income_per_person")
def get_income_per_person(income_thousands: float, children: int, number_extra: int = 1):
    """Calculate income per person."""
    if not number_extra > 0:
        raise ValueError("Argument number_extra must be greater than 0")

    return income_thousands / (children + number_extra)


@weave(outputs="satisfaction_score_per_income")
def get_satisfaction_score_per_income(satisfaction_score: float, income_per_person: float):
    """Calculate satisfaction score per income."""
    return satisfaction_score / income_per_person


@refine
def filter_data(df: DataFrame) -> DataFrame:
    """Clean the data."""
    return df[df["age"] > 30].dropna()


def test_loom_selected_columns(base_dataframe_input):
    """Tests that Loom correctly selects columns."""
    loom = Loom(base_dataframe_input, [f1], columns=["col1"])
    assert loom.database.columns.tolist() == ["col1"]
    loom.run()
    assert loom.database.columns.tolist() == ["col1", "z1"]


def test_loom_columns_infer_weave(base_dataframe_input):
    """Tests that Loom correctly infers columns from weave tasks."""
    loom = Loom(base_dataframe_input, [f1], infer_weave_columns=True)
    assert loom.database.columns.tolist() == ["col1"]  # check data base preselected
    loom.run()
    assert loom.database.columns.tolist() == ["col1", "z1"]  # check output assigned


def test_loom_columns_infer_weave_multiple_tasks(personal_data):
    loom = Loom(
        personal_data,
        [get_income_per_person, get_satisfaction_score_per_income],
        infer_weave_columns=True,
    )
    assert sorted(loom.database.columns.tolist()) == [
        "children",
        "income_thousands",
        "satisfaction_score",
    ]
    loom.run()
    assert sorted(loom.database.columns.tolist()) == [
        "children",
        "income_per_person",
        "income_thousands",
        "satisfaction_score",
        "satisfaction_score_per_income",
    ]


def test_loom_columns_infer_refine(personal_data):
    """Tests that Loom correctly infers columns from refine tasks."""
    loom = Loom(
        personal_data,
        [filter_data, get_income_per_person, get_satisfaction_score_per_income],
        infer_weave_columns=True,
        refine_columns=["age"],  # take age as this is used for filtering data
    )
    # Assert weave columns are inferred correctly and refine columns are added
    assert sorted(loom.database.columns.tolist()) == [
        "age",
        "children",
        "income_thousands",
        "satisfaction_score",
    ]
    loom.run()
    assert all(loom.database["age"] > 30)
    assert sorted(loom.database.columns.tolist()) == [
        "age",
        "children",
        "income_per_person",
        "income_thousands",
        "satisfaction_score",
        "satisfaction_score_per_income",
    ]


def test_loom_columns_unknown_column_error(personal_data):
    """Tests that Loom correctly raises error on unknown column."""
    with pytest.raises(
        KeyError,
        match="Required columns not found in DataFrame: \\['unknown_columns'\\]",
    ):
        Loom(
            personal_data,
            [filter_data, get_income_per_person, get_satisfaction_score_per_income],
            infer_weave_columns=True,
            refine_columns=["unknown_columns"],
        )
