"""
This module provides abstract base classes for the core workflow
orchestration components of weaveflow. These classes define the interface and
core functionality for executing and managing `@weave` and `@refine` tasks.
"""

from abc import ABC, abstractmethod
from collections import defaultdict


class _BaseWeave(ABC):
    """Abstract base class for all weaves."""

    def __init__(self, weave_tasks: list[callable], weaveflow_name: str):
        """Initializes the _BaseWeave abstract base class.

        Args:
            weave_tasks (list[callable]): A list of weave tasks to be executed.
            weaveflow_name (str): The name of the weaveflow pipeline.
        """
        self.weave_tasks = weave_tasks
        self.weaveflow_name = weaveflow_name
        self.weave_collector = defaultdict(dict)

    @abstractmethod
    def run(self):
        """Abstract method to run the main application logic."""
        pass


__all__ = ["_BaseWeave"]
