"""
This module contains the core workflow orchestration logic for weaveflow.

The `Loom` class is the central orchestrator of the `weaveflow` library. It is
responsible for executing a data processing pipeline defined as a sequence of
`@weave` and `@refine` tasks on a pandas DataFrame.

Its key responsibilities include:
- **Task Execution**: It iterates through a user-provided list of tasks,
  executing them in the specified order. It intelligently distinguishes
  between `@weave` and `@refine` tasks and handles them accordingly.

- **Data Flow Management**:
  - For `@weave` tasks, it automatically inspects the task's metadata to
    identify the required input columns, supplies them from the main DataFrame,
    executes the function, and seamlessly concatenates the new output columns
    back to the DataFrame.
  - For `@refine` tasks, it passes the entire DataFrame to the task and
    replaces the `Loom`'s internal DataFrame with the transformed one returned
    by the task, thus managing the state of the data through sequential steps.

- **Metadata Collection**: As it runs, the `Loom` collects detailed metadata
  about each task execution (e.g., inputs, outputs, parameters, execution time)
  into its `weave_collector` and `refine_collector` dictionaries. This metadata
  is the foundation for the visualization capabilities provided by `WeaveGraph`
  and `RefineGraph`.
"""

import time
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import override

import pandas as pd

from weaveflow._decorators import _is_refine, _is_weave
from weaveflow._utils import _dump_str_to_list


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
        optionals: dict[str, dict[str]] | None = None,
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
    def _infer_columns_from_weaves(weave_tasks: Iterable[Callable]) -> set[str]:
        """
        Finds the first weave task from a iterable of weave tasks and returns its
        required arguments as a strating point for the whole execution process.

        Note: Sometimes the user has a data base for which only a specific set of
            columns is needed for a specific task/purpose. In this case, the user
            can either pre-select the needed columns or can infer them automatically.
            The automatic inference is based on the weave tasks (as this is the
            basis for graph creation and node inference). This is useful when the
            user is not sure about the required columns and does not want to care
            about it, which, obvisouyly I do not recommend. Ideally the user SHOULD
            be aware of the data. However, I (unfortunately) deem this to be a valid
            use case.
        """
        args = set()

        # Update args with the required arguments of the first weave task
        for weave_task in weave_tasks:
            if _is_weave(weave_task):
                _args = weave_task._weave_meta._rargs
                args.update(set(_args))

        # If no weave tasks found, raise an error
        if not args:
            raise ValueError("No arguments found")

        return args

    @staticmethod
    def dump_to_frame(outputs: list[str], calculation_output: any, **kwargs):
        """
        Dump the calculation output to a pandas DataFrame that can be concatenated
        to the database.

        Args:
            outputs (list[str]): The names of the outputs.
            calculation_output (any): The output of the calculation.
            **kwargs: Additional keyword arguments to pass to the pandas DataFrame
                constructor.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the calculation output.
        """
        calculation_output_dict = (
            {outputs[0]: calculation_output}
            if len(outputs) == 1
            else dict(zip(outputs, calculation_output, strict=False))
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
            ValueError: If there is an intersection between the database
                columns and outputs.
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
        task_optionals = self.optionals.get(weave_name, {}) or self.optionals.get(
            weave_task, {}
        )
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
        """
        Build kwargs for calling the weave using original param
        namesnames and mapped columns.
        """
        return {
            orig: df[mapped] for orig, mapped in zip(required_args, rargs_m, strict=False)
        }

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
        weave_name = weave_task.__name__
        weave_meta = weave_task._weave_meta

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
    """The central orchestrator for executing weaveflow pipelines on pandas DataFrames.

    Loom manages the execution of both `@weave` (column-wise transformations)
    and `@refine` (DataFrame-wide transformations) tasks in the correct order,
    handling data flow, parameter injection, and metadata collection for
    graph visualization.

    It extends `PandasWeave` to incorporate `refine` tasks and provides a
    unified interface for running complex data processing workflows.

    Attributes:
        database (pd.DataFrame): The pandas DataFrame on which the tasks operate.
        tasks (Iterable[Callable]): A list of `@weave` and `@refine` decorated
            functions or classes that constitute the pipeline.
        weaveflow_name (str): A unique name for this specific weaveflow pipeline,
            used for organizing collected metadata.
        optionals (dict[str, dict[str]]): A dictionary to provide optional
            arguments to specific weave tasks. Keys are task names, values are
            dictionaries of optional arguments.
        kwargs (Any): Additional keyword arguments passed to the Loom
            constructor, which are treated as global optional parameters
            available to weave tasks.
        refine_collector (defaultdict): Stores metadata about executed refine tasks.
    """

    def __init__(
        self,
        database: pd.DataFrame,
        tasks: Iterable[callable],
        weaveflow_name: str = "default",
        optionals: dict[str, dict[str]] | None = None,
        infer_weave_columns: str | bool = False,
        refine_columns: str | list[str] | None = None,
        weave_columns: str | list[str] | None = None,
        columns: str | list[str] | None = None,
        **kwargs,
    ):
        """Initializes the Loom orchestrator.

        Args:
            database (pd.DataFrame): The initial DataFrame to process.
            tasks (Iterable[Callable]): A sequence of `@weave` and `@refine`
                decorated functions or classes to be executed.
            weaveflow_name (str, optional): A name for this pipeline instance.
                Defaults to "default".
            optionals (dict[str, dict[str]] | None, optional): Task-specific
                optional arguments. Defaults to None.
            **kwargs: Global optional arguments accessible by weave tasks.
        """
        all_tasks = list(tasks)
        # Filter only weave tasks
        filtered_weave_tasks = [task for task in tasks if _is_weave(task)]
        # Pre-select columns if specified by user
        database = self._pre_select_columns(
            database=database,
            infer_weave_columns=infer_weave_columns,
            refine_columns=refine_columns,
            weave_columns=weave_columns,
            columns=columns,
        )
        # TODO: Loom being the main workflow orchestrator, differ between PandasWeave, DaskWeave, etc.  # noqa: E501
        super().__init__(
            database, filtered_weave_tasks, weaveflow_name, optionals, **kwargs
        )
        self.tasks = all_tasks  # All tasks
        self.refine_collector = defaultdict(dict)
        self.__pre_init__()

    def _pre_select_columns(
        self,
        database: pd.DataFrame,
        infer_weave_columns: str | bool = False,
        refine_columns: str | list[str] | None = None,
        weave_columns: str | list[str] | None = None,
        columns: str | list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Pre-select columns for the database based on user input.
        """
        # If columns argument is specified, ignore all other arguments and use only those
        if columns is not None:
            columns = _dump_str_to_list(columns)
            self.check_intersection_columns_dataframe(database, columns)
            return database[columns]

        # If refine columns are specified, use them as a starting point
        if refine_columns is not None:
            refine_columns = _dump_str_to_list(refine_columns)

            # If weave columns are specified, add them to the refine columns
            # and return the resulting columns
            if weave_columns is not None:
                weave_columns = _dump_str_to_list(weave_columns)
                columns = refine_columns + weave_columns
                self.check_intersection_columns_dataframe(database, columns)
                return database[columns]

            # If infer weave columns is True, infer the columns from the weave tasks
            # and add them to the refine columns
            if infer_weave_columns:
                inferred_weave_columns: set = self._infer_columns_from_weaves(
                    self.weave_tasks
                )
                inferred_weave_columns = [
                    col for col in inferred_weave_columns if col in database
                ]
                columns = refine_columns + inferred_weave_columns
                self.check_intersection_columns_dataframe(database, refine_columns)
                return database[columns]

        # If no columns are specified, return the database as is
        return database

    @override
    def __pre_init__(self):
        """Pre-initialization checks."""
        if not isinstance(self.tasks, Iterable):
            raise TypeError("'tasks' must be a Iterable of callables")
        for task in self.tasks:
            if not (_is_weave(task) or _is_refine(task)):
                raise TypeError(
                    f"Argument 'weave_tasks' contains a non-weave"
                    f"and non-refine task: {task!r}"
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
        refine_meta = refine_task._refine_meta
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
