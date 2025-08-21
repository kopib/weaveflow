"""
This module contains the core workflow orchestration logic for weaveflow.

The 'Loom' class is the main entry point for executing a pipeline of
'weave' and 'refine' tasks on a pandas DataFrame. It manages task
execution, data flow, and metadata collection.
"""
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from typing import override
import time
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
    def check_intersection_columns_dataframe(df: pd.DataFrame, expected_cols: list[str]) -> None:
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

    def _collect_optionals_for_task(
        self, weave_name: str, optional_args: list[str], weave_task: callable
    ) -> dict:
        """Collect optional arguments for a weave from global and task-specific sources."""
        oargs = {
            oarg: self.global_optionals[oarg]
            for oarg in optional_args
            if oarg in self.global_optionals
        }
        task_optionals = self.optionals.get(weave_name, {}) or self.optionals.get(weave_task, {})
        oargs.update(task_optionals)
        return oargs

    @staticmethod
    def _resolve_effective_names(
        weave_meta,
        required_args: list[str],
        optional_args: list[str],
        outputs: list[str],
    ) -> tuple[dict, list[str], list[str], list[str]]:
        """Resolve effective names using meta mapping without renaming the DataFrame.

        Returns:
            tuple[dict, list[str], list[str], list[str]]: A tuple containing
            the name mapping (name_map, rargs_m, oargs_m, outs_m).
        """
        name_map = weave_meta._meta_mapping or {}
        inv_map = {v: k for k, v in name_map.items()}
        # Support both directions: arg->dfcol or dfcol->arg
        rargs_m = [name_map.get(a, inv_map.get(a, a)) for a in required_args]
        oargs_m = [name_map.get(o, inv_map.get(o, o)) for o in optional_args]
        # Outputs map forward only (orig -> new)
        outs_m = [name_map.get(o, o) for o in outputs]
        return name_map, rargs_m, oargs_m, outs_m

    @staticmethod
    def _build_required_kwargs(
        df: pd.DataFrame, required_args: list[str], rargs_m: list[str]
    ) -> dict:
        """Build kwargs for calling the weave using original param names and mapped columns."""
        return {orig: df[mapped] for orig, mapped in zip(required_args, rargs_m)}

    def _record_weave_run(
        self,
        weave_name: str,
        outputs_m: list[str],
        rargs_m: list[str],
        oargs_m: list[str],
        params: dict,
        delta_time: float,
    ) -> None:
        """Record metadata about the weave run for downstream graph/matrix."""
        self.weave_collector[self.weaveflow_name][weave_name] = {
            "outputs": outputs_m,
            "rargs": rargs_m,
            "oargs": oargs_m,
            "params": list(params),
            "delta_time": delta_time,
        }

    @staticmethod
    def _call_weave(weave_task: callable, rargs: dict, oargs: dict, params: dict):
        """Execute the weave task with prepared arguments."""
        return weave_task(**rargs, **oargs, **params)

    def _optionals_from_kwargs(self, weave_name: str, weave_optionals: list[str]) -> dict:
        for oarg in weave_optionals:
            if oarg in self.kwargs:
                self.optionals[weave_name].update({oarg: self.kwargs[oarg]})

    def _run_weave_task(self, weave_task: callable) -> None:
        """Run a single weave task end-to-end on the current database."""
        weave_name = getattr(weave_task, "__name__")
        weave_meta = getattr(weave_task, "_weave_meta")

        # Extract meta pieces
        params = weave_meta._params
        required_args = weave_meta._rargs
        optional_args = weave_meta._oargs
        outputs = weave_meta._outputs

        # Collect optionals
        oargs = self._collect_optionals_for_task(weave_name, optional_args, weave_task)

        # Resolve names and validate presence
        _, rargs_m, oargs_m, outs_m = self._resolve_effective_names(
            weave_meta, required_args, optional_args, outputs
        )
        self.check_intersection_columns_dataframe(df=self.database, expected_cols=rargs_m)
        # Build kwargs and execute
        rargs = self._build_required_kwargs(self.database, required_args, rargs_m)
        # Execute with timing for graph edges
        t0 = time.perf_counter()
        calculation_output = self._call_weave(weave_task, rargs, oargs, params)
        dt = time.perf_counter() - t0
        # Record all relevant information for graph/matrix
        self._record_weave_run(
            weave_name=weave_name,
            outputs_m=outs_m,
            rargs_m=rargs_m,
            oargs_m=oargs_m,
            params=params,
            delta_time=dt,
        )

        return calculation_output, outs_m, weave_name

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
        super().__init__(database, filtered_weave_tasks, weaveflow_name, optionals, **kwargs)
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

    def _record_refine_run(
        self,
        refine_task_name: str,
        on_method: str,
        params: list[str],
        params_object: str,
        description: str,
        delta_time: float,
    ) -> None:
        """Record metadata about the refine run for downstream graph/matrix."""
        self.refine_collector[self.weaveflow_name][refine_task_name] = {
            "on_method": on_method,
            "params": params,
            "params_object": params_object,
            "description": description,
            "delta_time": delta_time,
        }

    def _run_refine_task(self, refine_task: callable) -> None:
        """Run refine task on the database. The data base is modified in-place.

        Args:
            refine_task (callable): The refine task to run. The task must
            take a pandas DataFrame as input and return a pandas DataFrame.
        """
        # Get the meta information from refine object
        refine_meta = getattr(refine_task, "_refine_meta")
        refine_name = refine_meta._refine_name
        # Run calculation and build the graph
        t0 = time.perf_counter()
        self.database = refine_task(self.database)
        dt = time.perf_counter() - t0
        # Record all relevant information for refine graph
        self._record_refine_run(
            refine_task_name=refine_name,
            on_method=refine_meta._on_method,
            params=list(refine_meta._params),
            params_object=refine_meta._params_object,
            description=refine_meta._refine_description,
            delta_time=dt,
        )

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
