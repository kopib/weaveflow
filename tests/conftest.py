"""
This module contains shared fixtures for testing.
"""

from pathlib import Path

import pandas as pd
import pytest
from numpy import nan

_DF = pd.DataFrame(
    {
        "col1": [1, 2, 3],
        "col2": [4, 5, 6],
    },
    index=["id1", "id2", "id3"],
)


@pytest.fixture
def base_dataframe_input() -> pd.DataFrame:
    """A shared fixture for a baseline DataFrame."""
    return _DF


@pytest.fixture
def base_dataframe() -> pd.DataFrame:
    """A shared fixture for a baseline DataFrame."""
    expected_data = {
        "sum": [5, 7, 9],
        "diff": [3, 3, 3],
        "mul": [15, 21, 27],
        "div": [1, 2, 3],
        "scaled_mul": [30, 42, 54],
    }
    expected_df = pd.DataFrame(expected_data, index=["id1", "id2", "id3"])
    return pd.concat([_DF, expected_df], axis=1)


@pytest.fixture
def finacial_dataframe() -> pd.DataFrame:
    """A shared fixture for a baseline DataFrame."""
    df = pd.DataFrame(
        {
            "stocks": [149_523_331.5, 12_403.9, 210_430.3, 3_310, 340_000.6],
            "investments": [100_000, 100_000, 100_000, 100_000, 100_000],
            "rest": [-32_041_301.2, 2_051, 0.0, 17_019.21, -29_304.08],
            "something": [1, 2, 3, 4, 5],
        }
    )
    return df


@pytest.fixture
def test_data_path() -> Path:
    """Path to the test data directory."""
    return Path(__file__).parent / "data" / "dummy"


@pytest.fixture
def personal_data() -> pd.DataFrame:
    data = {
        "age": [28, 45, 33, 62, 21, 39, 41, 55, 29, 34, 48, 65, 24, 30, 51],
        "city": [
            "Cologne",
            "Berlin",
            "Munich",
            "Hamburg",
            "Cologne",
            "Berlin",
            "Frankfurt",
            "Munich",
            "Cologne",
            "Hamburg",
            "Berlin",
            "Frankfurt",
            "Munich",
            "Cologne",
            "Berlin",
        ],
        "satisfaction_score": [
            4.5,
            3.2,
            5.0,
            2.1,
            4.8,
            3.9,
            4.1,
            nan,
            3.5,
            2.8,
            4.2,
            3.0,
            4.9,
            3.8,
            2.5,
        ],
        "children": [0, 2, 1, 3, 0, 1, 2, 2, 0, 1, 3, 1, 0, 1, 2],
        "income_thousands": [
            55.5,
            89.0,
            72.1,
            105.5,
            42.0,
            75.8,
            81.2,
            95.0,
            61.5,
            nan,
            92.0,
            110.2,
            48.5,
            66.0,
            85.1,
        ],
        "has_subscription": [
            True,
            False,
            True,
            False,
            True,
            True,
            False,
            True,
            True,
            False,
            True,
            True,
            True,
            False,
            True,
        ],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def sample_profile_dataframe() -> pd.DataFrame:
    return pd.DataFrame({"a": range(100), "b": range(100, 200)})
