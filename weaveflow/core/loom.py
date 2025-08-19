from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from typing import override
import pandas as pd

from weaveflow._decorators._weave import _is_weave
from weaveflow._decorators._refine import _is_refine


class _BaseWeave(ABC):
    """Abstract base class for all weaves."""

    def __init__(self, weave_tasks: list[callable], weaveflow_name: str):
        self.weave_tasks = weave_tasks
        self.weaveflow_name = weaveflow_name
        self.weave_collector = defaultdict(dict)

    def __pre_init__(self):

        if not isinstance(self.weave_tasks, Iterable):
            raise TypeError("'weave_tasks' must be a Iterable of weave tasks")

        for weave_task in self.weave_tasks:
            if not _is_weave(weave_task):
                raise TypeError(
                    f"Argument 'weave_tasks' contains a non-weave task: {weave_task!r}"
                )

    @abstractmethod
    def run(self):
        """Run the main application."""
        pass


class PandasWeave(_BaseWeave):
    """PandasWeave class."""

    def __init__(
        self,
        database: pd.DataFrame,
        weave_tasks: Iterable[callable],
        weaveflow_name: str = "default",
        optionals: dict[str, dict[str]] = None,
        **kwargs,
    ):
        super().__init__(weave_tasks, weaveflow_name)
        self.database = database
        self.optionals = optionals or {}
        self.global_optionals = kwargs

    def __pre_init__(self):
        if not isinstance(self.database, pd.DataFrame):
            raise TypeError("Database must be a pandas DataFrame")
        if not isinstance(self.optionals, dict):
            raise TypeError("Optionals must be a dictionary")

    @staticmethod
    def dump_to_frame(outputs: list[str], calculation_output: any, **kwargs):
        """
        Dump the calculation output to a pandas DataFrame that can be concatenated to the database.

        Args:
            outputs (list[str]): The names of the outputs.
            calculation_output (any): The output of the calculation.
            **kwargs: Additional keyword arguments to pass to the pandas DataFrame constructor.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the calculation output.
        """
        calculation_output_dict = (
            {outputs[0]: calculation_output}
            if len(outputs) == 1
            else dict(zip(outputs, calculation_output))
        )
        return pd.DataFrame(calculation_output_dict, **kwargs)

    @staticmethod
    def check_intersection_columns_dataframe(
        df: pd.DataFrame, expected_cols: list[str]
    ) -> None:
        """
        Check if the columns in the database intersect with the outputs.

        Args:
            database (pd.DataFrame): The database DataFrame.
            outputs (list[str]): The list of output column names.

        Raises:
            ValueError: If there is an intersection between the database columns and outputs.
        """
        missing_cols = set(expected_cols) - set(df.columns)

        if missing_cols:
            raise KeyError(
                f"Required columns not found in DataFrame: {sorted(list(missing_cols))}"
            )

    def extend_database(self, **kwargs) -> None:
        """Extend the database with the calculation output."""
        calculation_output_frame = self.dump_to_frame(**kwargs)
        self.database = pd.concat([self.database, calculation_output_frame], axis=1)

    @staticmethod
    def apply_weave_meta_to_df(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
        if not isinstance(meta, dict):
            return df

        return df.rename(columns=meta)

    def _optionals_from_kwargs(
        self, weave_name: str, weave_optionals: list[str]
    ) -> dict:
        for oarg in weave_optionals:
            if oarg in self.kwargs:
                self.optionals[weave_name].update({oarg: self.kwargs[oarg]})

    def _run_weave_task(self, weave_task: callable) -> None:
        # Get the function name
        weave_name = getattr(weave_task, "__name__")
        weave_meta = getattr(weave_task, "_weave_meta")
        # Get the meta information from meta object
        params = weave_meta._params
        required_args = weave_meta._rargs
        optional_args = weave_meta._oargs
        outputs = weave_meta._outputs
        # Collect all optional arguments from global setup and task-specific optionals
        oargs = {
            oarg: self.global_optionals[oarg]
            for oarg in optional_args
            if oarg in self.global_optionals
        }
        task_optionals = self.optionals.get(weave_name, {}) or self.optionals.get(
            weave_task, {}
        )
        oargs.update(task_optionals)

        # Transform data base based on meta from weave task and define final rargs
        database_t = self.apply_weave_meta_to_df(
            df=self.database.copy(),
            meta=weave_meta._meta_mapping,
        )
        self.check_intersection_columns_dataframe(
            df=database_t, expected_cols=required_args
        )

        # Prepare the required arguments for the weave task + optional arguments
        rargs = database_t[required_args].to_dict(orient="series")

        # Run calculation and build the graph
        self.weave_collector[self.weaveflow_name][weave_name] = {
            "outputs": outputs,
            "rargs": required_args,
            "oargs": optional_args,
            "params": list(params),
        }
        calculation_output = weave_task(**rargs, **oargs, **params)

        return calculation_output, outputs, weave_name

    def run(self):
        """Run the loomer on the database."""
        for weave_task in self.weave_tasks:

            calculation_output, outputs, weave_name = self._run_weave_task(weave_task)

            if calculation_output is None:
                self.weave_collector[self.weaveflow_name].pop(weave_name)
                continue

            # TODO: Add option to extend database 'database_t' or 'self.database'
            # TODO: Make timer work
            # Extend the database with the calculation output
            self.extend_database(
                outputs=outputs,
                calculation_output=calculation_output,
                index=self.database.index,
                columns=outputs,
            )


class Loom(PandasWeave):
    """Loom class."""

    def __init__(
        self,
        database: pd.DataFrame,
        tasks: Iterable[callable],
        weaveflow_name: str = "default",
        optionals: dict[str, dict[str]] = None,
        **kwargs,
    ):
        all_tasks = list(tasks)
        # Filter only weave tasks
        filtered_weave_tasks = [task for task in tasks if _is_weave(task)]
        # TODO: Rename weave_tasks to tasks
        # TODO: Loom being the main workflow orchestrator, differ between PandasWeave, DaskWeave, etc.
        super().__init__(
            database, filtered_weave_tasks, weaveflow_name, optionals, **kwargs
        )
        self.tasks = all_tasks  # All tasks
        self.refine_collector = defaultdict(dict)
        self.__pre_init__()

    @override
    def __pre_init__(self):
        """Pre-initialization checks."""
        if not isinstance(self.tasks, Iterable):
            raise TypeError("'tasks' must be a Iterable of callables")
        for task in self.tasks:
            if not (_is_weave(task) or _is_refine(task)):
                raise TypeError(
                    f"Argument 'weave_tasks' contains a non-weave and non-refine task: {task!r}"
                )

    def _run_refine_task(self, refine_task: callable) -> None:
        """Run refine task on the database. The data base is modified in-place.

        Args:
            refine_task (callable): The refine task to run. The task must
            take a pandas DataFrame as input and return a pandas DataFrame.
        """
        # Get the meta information from refine object
        refine_meta = getattr(refine_task, "_refine_meta")
        self.weave_collector[self.weaveflow_name][
            refine_meta._refine_name
        ] = refine_meta.__dict__
        # Run calculation and build the graph
        self.database = refine_task(self.database)

    def _run(self):

        for task in self.tasks:

            # If task is a weave task, calculate new columns and add them to the database
            if _is_weave(task):

                # Run weave task on task arguments according to meta information
                calculation_output, outputs, weave_name = self._run_weave_task(task)

                # If calculation output is None, skip the task
                if calculation_output is None:
                    self.weave_collector[self.weaveflow_name].pop(weave_name)
                    continue

                # Extend the database with the calculation output
                self.extend_database(
                    outputs=outputs,
                    calculation_output=calculation_output,
                    index=self.database.index,
                    columns=outputs,
                )

            # If task is a refine task, refine the database
            elif _is_refine(task):
                self._run_refine_task(task)

    @override
    def run(self):
        """Run the loomer on specified database."""
        self._run()
