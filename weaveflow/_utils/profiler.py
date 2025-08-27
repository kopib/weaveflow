"""
This module provides a profiling utility for tasks, particularly useful for
tracking the performance and data flow of `@refine` tasks in `weaveflow`.
"""

import time
from collections.abc import Callable
from typing import Any

from pandas import DataFrame


class TaskProfiler:
    """
    An executor that runs a task and conditionally tracks its execution time
    and DataFrame row reduction.
    """

    def __init__(
        self,
        task: Callable,
        data: DataFrame = None,
        track_time: bool = False,
        track_data: bool = False,
    ):
        self._task = task
        self._initial_data = data
        self._track_time = track_time
        self._track_data = track_data

        # Initialize result attributes safely
        self.delta_time: float = 0
        self.rows_reduced: int = 0

        # Validation
        if self._track_data and not isinstance(self._initial_data, DataFrame):
            raise TypeError("If 'track_data' is True, a DataFrame must be provided.")

    def run(self, *args, **kwargs) -> Any:
        """
        Executes the task, records all metrics, and returns the result.
        """
        # Setup
        rows_before = len(self._initial_data) if self._track_data else 0

        if self._track_time:
            t0 = time.perf_counter()

        # The initial data is always passed if it exists, regardless of task signature
        # leads to more consistent API for tasks
        if self._initial_data is not None:
            result = self._task(self._initial_data, *args, **kwargs)
        else:
            result = self._task(*args, **kwargs)

        # Teardown & Metric Calculation
        if self._track_time:
            self.delta_time = time.perf_counter() - t0

        if self._track_data:
            # Correctly compare the initial row count with the RESULT's row count
            self.rows_reduced = rows_before - len(result)

        return result
