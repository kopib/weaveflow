# test_task_tracker.py

import time

import pytest
from pandas import DataFrame

from weaveflow._utils import TaskProfiler


def some_task() -> None:
    time.sleep(0.12)


def filter_function(df: DataFrame) -> DataFrame:
    return df[df["a"] > 49]  # 50 entries expected


def filter_timeout(df: DataFrame) -> DataFrame:
    time.sleep(0.08)
    return df[(df["a"] > 49) & (df["b"] < 190)]  # 40 entries expected


def test_task_profiler_time_tracking():
    """Test that the TaskProfiler correctly tracks execution time."""
    profiler = TaskProfiler(some_task, track_time=False)
    profiler.run()
    assert profiler.delta_time == 0.0

    profiler = TaskProfiler(some_task, track_time=True)
    profiler.run()
    pytest.approx(profiler.delta_time, 0.12, 0.0001)


def test_task_profiler_data_tracking(sample_profile_dataframe):
    """Test that the TaskProfiler correctly tracks DataFrame row reduction."""
    profiler = TaskProfiler(filter_function, sample_profile_dataframe, track_data=True)
    profiler.run()
    assert profiler.rows_reduced == 50


def test_task_profiler_data_tracking_no_data():
    """Test that the TaskProfiler correctly tracks DataFrame row reduction."""
    with pytest.raises(TypeError):
        TaskProfiler(filter_function, track_data=True)


def test_task_profiler_data_tracking_no_dataframe():
    """Test that the TaskProfiler correctly tracks DataFrame row reduction."""
    with pytest.raises(TypeError):
        TaskProfiler(filter_function, "not a dataframe", track_data=True)


def test_filter_timeout(sample_profile_dataframe):
    """Test that the TaskProfiler correctly tracks DataFrame row reduction."""
    profiler = TaskProfiler(
        filter_timeout, sample_profile_dataframe, track_data=True, track_time=True
    )
    profiler.run()
    pytest.approx(profiler.delta_time, 0.08, 0.0001)
    assert profiler.rows_reduced == 60  # 100 - 40 dropped


def test_filter_timeout_no_track_data(sample_profile_dataframe):
    """Test that the TaskProfiler correctly tracks DataFrame row reduction."""
    profiler = TaskProfiler(
        filter_timeout, sample_profile_dataframe, track_data=False, track_time=True
    )
    profiler.run()
    pytest.approx(profiler.delta_time, 0.08, 0.0001)
    assert profiler.rows_reduced == 0
