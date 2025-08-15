import pandas as pd
import pytest


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
