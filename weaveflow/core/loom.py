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

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import override

import pandas as pd

from weaveflow._errors import LoomValidator, WeaveTaskValidator
from weaveflow._utils import TaskProfiler, _dump_str_to_list, _is_refine, _is_weave


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


class PandasWeave(_BaseWeave):
    """A weave that operates on pandas DataFrames.

    This class orchestrates the execution of a series of `@weave` tasks on a
    pandas DataFrame, managing data flow and collecting metadata.

    Attributes:
        database (pd.DataFrame): The DataFrame to be processed.
        weave_tasks (Iterable[callable]): A sequence of `@weave` tasks.
        weaveflow_name (str): Name for the pipeline instance.
        optionals (dict): Task-specific optional arguments.
        global_optionals (dict): Global optional arguments for all tasks.
        weave_collector (defaultdict): A dictionary to store metadata about
            each weave task execution.
    """

    def __init__(
        self,
        database: pd.DataFrame,
        weave_tasks: Iterable[callable],
        weaveflow_name: str = "default",
        optionals: dict[str, dict[str]] | None = None,
        **kwargs,
    ):
        """Initializes the PandasWeave orchestrator.

        Args:
            database (pd.DataFrame): The initial DataFrame to process.
            weave_tasks (Iterable[Callable]): A sequence of `@weave` decorated
                functions to be executed.
            weaveflow_name (str, optional): A name for this pipeline instance.
                Defaults to "default".
            optionals (dict[str, dict[str]] | None, optional): Task-specific
                optional arguments. Defaults to None.
            **kwargs: Global optional arguments accessible by all weave tasks.
        """
        # Validate weave tasks before proceeding
        WeaveTaskValidator(weave_tasks).validate()
        # Initialize base class and define attributes
        super().__init__(weave_tasks, weaveflow_name)
        self.database = database
        self.optionals = optionals or {}
        self.global_optionals = kwargs

    @staticmethod
    def _infer_columns_from_weaves(weave_tasks: Iterable[Callable]) -> set[str]:
        """Infers required columns from a collection of weave tasks.

        This method scans through the provided weave tasks and aggregates all
        required arguments (`_rargs`) into a set of column names. This is
        useful for automatically selecting a subset of columns from a larger
        DataFrame that are necessary for a specific pipeline.

        Note:
            This automatic inference is based on the weave tasks, which form
            the basis for graph creation and node inference. It is useful when
            the user is not sure about the required columns. However, it is
            recommended that the user be aware of their data and select
            columns explicitly where possible.

        Args:
            weave_tasks (Iterable[Callable]): A sequence of `@weave` decorated
                functions.

        Returns:
            set[str]: A set of required column names.

        Raises:
            ValueError: If no arguments are found in any of the weave tasks.
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
        """Dumps calculation output into a pandas DataFrame.

        This method takes the output of a weave task and organizes it into a
        DataFrame, which can then be concatenated with the main database.

        Args:
            outputs (list[str]): The names of the output columns.
            calculation_output (any): The result from a weave task. This can be
                a single Series-like object or a tuple of them.
            **kwargs: Additional keyword arguments to pass to the
                `pandas.DataFrame` constructor.

        Returns:
            pd.DataFrame: A DataFrame containing the calculation output, with
                columns named according to `outputs`.
        """
        # If calculation output is already a DataFrame, return it as it is
        if isinstance(calculation_output, pd.DataFrame):
            return calculation_output

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
        """Checks if a DataFrame contains all expected columns.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            expected_cols (list[str]): A list of column names that are
                expected to be in the DataFrame.

        Raises:
            KeyError: If any of the `expected_cols` are not found in the
                DataFrame's columns.
        """
        missing_cols = set(expected_cols) - set(df.columns)

        if missing_cols:
            raise KeyError(
                f"Required columns not found in DataFrame: {sorted(list(missing_cols))}"
            )

    def extend_database(self, **kwargs) -> None:
        """Extends the main DataFrame with new columns.

        This method takes the output of a weave task, converts it to a
        DataFrame using `dump_to_frame`, and concatenates it to the main
        `database`.

        Args:
            **kwargs: Keyword arguments to be passed to `dump_to_frame`.
                Typically includes `outputs` and `calculation_output`.
        """
        calculation_output_frame = self.dump_to_frame(**kwargs)
        self.database = pd.concat([self.database, calculation_output_frame], axis=1)

    def _collect_optionals_for_task(
        self, weave_name: str, optional_args: list[str], weave_task: callable
    ) -> dict:
        """Collects optional arguments for a specific weave task.

        It gathers optional arguments from both global `kwargs` provided to the
        Loom and task-specific `optionals`. Task-specific arguments override
        global ones.

        Args:
            weave_name (str): The name of the weave task.
            optional_args (list[str]): A list of optional argument names for the
                task.
            weave_task (callable): The weave task function itself.

        Returns:
            dict: A dictionary of resolved optional arguments for the task.
        """
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
        """Resolves effective input and output column names using reweave metadata.

        This method applies the `_meta_mapping` from a weave task's metadata
        (set by `@reweave`) to translate the function's internal argument
        names to the actual column names in the DataFrame.

        Args:
            weave_meta: The `WeaveMeta` object attached to the task.
            required_args (list[str]): The original required argument names.
            optional_args (list[str]): The original optional argument names.
            outputs (list[str]): The original output column names.

        Returns:
            A tuple containing:
                - name_map (dict): The mapping dictionary from `@reweave`.
                - rargs_m (list[str]): The mapped required argument names.
                - oargs_m (list[str]): The mapped optional argument names.
                - outs_m (list[str]): The mapped output column names.
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
        """Builds the keyword argument dictionary for calling a weave task.

        This method maps the original function parameter names to the
        corresponding (potentially remapped) DataFrame columns.

        Args:
            df (pd.DataFrame): The main DataFrame.
            required_args (list[str]): The original required argument names of
                the function.
            rargs_m (list[str]): The mapped column names from the DataFrame.

        Returns:
            dict: A dictionary of keyword arguments to be passed to the weave
                task, where keys are the original parameter names and values
                are the corresponding pandas Series.
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
        """Records metadata about a weave task's execution.

        This information is stored in the `weave_collector` and is used for
        generating graphs and matrices.

        Args:
            weave_name (str): The name of the executed weave task.
            outputs_m (list[str]): The list of (mapped) output column names.
            rargs_m (list[str]): The list of (mapped) required input columns.
            oargs_m (list[str]): The list of (mapped) optional input columns.
            params (dict): A dictionary of parameters injected from a `@spool`
                object.
            delta_time (float): The execution time of the task in seconds.
        """
        self.weave_collector[self.weaveflow_name][weave_name] = {
            "outputs": outputs_m,
            "rargs": rargs_m,
            "oargs": oargs_m,
            "params": list(params),
            "delta_time": delta_time,
        }

    @staticmethod
    def _call_weave(weave_task: callable, rargs: dict, oargs: dict, params: dict):
        """Executes a weave task with the prepared arguments.

        Args:
            weave_task (callable): The weave task function to execute.
            rargs (dict): A dictionary of required arguments (column Series).
            oargs (dict): A dictionary of optional arguments.
            params (dict): A dictionary of injected parameters from `@spool`.

        Returns:
            The result of the weave task execution.
        """
        return weave_task(**rargs, **oargs, **params)

    def _optionals_from_kwargs(self, weave_name: str, weave_optionals: list[str]) -> dict:
        """Populates task-specific optionals from global kwargs.

        This method is not currently used but is intended to update the
        `self.optionals` dictionary for a specific task with values found in
        the global `self.kwargs`.

        Args:
            weave_name (str): The name of the weave task.
            weave_optionals (list[str]): The list of optional argument names
                for the task.

        Returns:
            dict: The updated dictionary of optional arguments for the task.
        """
        for oarg in weave_optionals:
            if oarg in self.kwargs:
                self.optionals[weave_name].update({oarg: self.kwargs[oarg]})

    def _run_weave_task(self, weave_task: callable) -> None:
        """Executes a single weave task and records its metadata.

        This method orchestrates the entire lifecycle of a single weave task:
        1. Extracts metadata from the task.
        2. Resolves optional arguments.
        3. Remaps input/output names if necessary (`@reweave`).
        4. Validates that required columns exist.
        5. Prepares arguments and calls the task.
        6. Records execution time and metadata.

        Args:
            weave_task (callable): The `@weave` decorated function to run.

        Returns:
            A tuple containing:
                - calculation_output: The result from the weave task.
                - outs_m (list[str]): The list of (mapped) output column names.
                - weave_name (str): The name of the weave task.
        """
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
        task_profiler = TaskProfiler(
            self._call_weave,
            track_time=True,
        )
        calculation_output = task_profiler.run(weave_task, rargs, oargs, params)
        # Record all relevant information for graph/matrix
        self._record_weave_run(
            weave_name=weave_name,
            outputs_m=outs_m,
            rargs_m=rargs_m,
            oargs_m=oargs_m,
            params=params,
            delta_time=task_profiler.delta_time,
        )

        return calculation_output, outs_m, weave_name

    def run(self):
        """Executes the weave pipeline.

        This method iterates through all the `weave_tasks` provided during
        initialization, runs each one, and extends the main DataFrame with
        the results.
        """
        for weave_task in self.weave_tasks:
            calculation_output, outputs, weave_name = self._run_weave_task(weave_task)

            if calculation_output is None:
                self.weave_collector[self.weaveflow_name].pop(weave_name)
                continue

            # TODO: Add option to extend database 'database_t' or 'self.database'
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
        # TODO: Introduce verbose mode extend graphical information (e.g. add arg types)
        all_tasks = list(tasks)
        # Filter only weave tasks
        filtered_weave_tasks = [task for task in tasks if _is_weave(task)]
        # Pre-select columns if specified by user
        database = self._pre_select_columns(
            database=database,
            infer_weave_columns=infer_weave_columns,
            weave_tasks=filtered_weave_tasks,
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
        weave_tasks: Iterable[Callable] | None = None,
        refine_columns: str | list[str] | None = None,
        weave_columns: str | list[str] | None = None,
        columns: str | list[str] | None = None,
    ) -> pd.DataFrame:
        """Pre-selects columns from the input DataFrame based on user specifications.

        This method provides several ways to select a subset of columns to
        work on, which can improve performance and clarity. The selection
        priority is: `columns` > `refine_columns` + `weave_columns` /
        `infer_weave_columns`.

        Args:
            database (pd.DataFrame): The initial DataFrame.
            infer_weave_columns (str | bool, optional): If True, automatically
                infers required columns from `weave_tasks`. Defaults to False.
            weave_tasks (Iterable[Callable] | None, optional): The weave tasks
                to use for column inference. Required if `infer_weave_columns`
                is True. Defaults to None.
            refine_columns (str | list[str] | None, optional): A list of
                columns to keep for refine tasks. Defaults to None.
            weave_columns (str | list[str] | None, optional): A list of
                columns to keep for weave tasks. Defaults to None.
            columns (str | list[str] | None, optional): A list of columns to
                keep for the entire pipeline, overriding all other column
                selection arguments. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing only the selected columns.

        Raises:
            ValueError: If `infer_weave_columns` is True but `weave_tasks` is
                not provided.
            KeyError: If any of the specified columns are not in the
                DataFrame.
        """
        # If columns argument is specified, ignore all other arguments and use only those
        if columns is not None:
            columns = _dump_str_to_list(columns)
            self.check_intersection_columns_dataframe(database, columns)
            return database[columns]

        # Initialize lists for building up the final column set
        final_refine_cols = []
        final_weave_cols = []

        # If refine columns are specified, use them as a starting point
        if refine_columns is not None:
            final_refine_cols = _dump_str_to_list(refine_columns)

        # If weave columns are specified, add them to the refine columns
        # and return the resulting columns
        if weave_columns is not None:
            final_weave_cols = _dump_str_to_list(weave_columns)
        elif infer_weave_columns:
            # If we want to infer the weave columns, but no weave tasks
            # are provided, raise an error
            if weave_tasks is None:
                raise ValueError(
                    "Cannot infer weave columns without providing weave tasks."
                )
            inferred_cols = self._infer_columns_from_weaves(weave_tasks)
            # Filter to only columns that actually exist in the input DataFrame
            final_weave_cols = [col for col in inferred_cols if col in database.columns]

        final_cols = final_refine_cols + final_weave_cols

        # If after all that, no columns were selected, return the original DB
        if not final_cols:
            return database

        # Combine refine and weave columns
        columns = (refine_columns or []) + (weave_columns or [])

        # Remove duplicates while preserving order and validate
        unique_final_cols = list(dict.fromkeys(final_cols))
        self.check_intersection_columns_dataframe(database, unique_final_cols)

        # If no columns are specified, return the database as is
        return database[unique_final_cols]

    @override
    def __pre_init__(self):
        """Performs pre-initialization checks for the Loom.

        Ensures that the `tasks` attribute is an iterable of callables and
        that each task is decorated with either `@weave` or `@refine`.

        Raises:
            TypeError: If `tasks` is not an iterable or contains an invalid
                task type.
        """
        LoomValidator(self.database, self.optionals, self.tasks).validate()
        # if not isinstance(self.database, pd.DataFrame):
        #     raise TypeError("Database must be a pandas DataFrame")
        # if not isinstance(self.optionals, dict):
        #     raise TypeError("Optionals must be a dictionary")
        # if not isinstance(self.tasks, Iterable):
        #     raise TypeError("'tasks' must be a Iterable of callables")
        # for task in self.tasks:
        #     if not (_is_weave(task) or _is_refine(task)):
        #         raise TypeError(
        #             f"Argument 'weave_tasks' contains a non-weave"
        #             f"and non-refine task: {task!r}"
        #         )

    def _record_refine_run(
        self,
        refine_task_name: str,
        on_method: str,
        params: list[str],
        params_object: str,
        description: str,
        delta_time: float,
        rows_reduced: int,
    ) -> None:
        """Records metadata about a refine task's execution.

        This information is stored in the `refine_collector` and is used for
        generating the `RefineGraph`.

        Args:
            refine_task_name (str): The name of the executed refine task.
            on_method (str): The name of the method executed (for class-based
                tasks).
            params (list[str]): A list of parameter names injected from a
                `@spool` object.
            params_object (str): The name of the `@spool` object.
            description (str): The user-provided description of the task.
            delta_time (float): The execution time of the task in seconds.
        """
        self.refine_collector[self.weaveflow_name][refine_task_name] = {
            "on_method": on_method,
            "params": params,
            "params_object": params_object,
            "description": description,
            "delta_time": delta_time,
            "rows_reduced": rows_reduced,
        }

    def _run_refine_task(self, refine_task: callable) -> None:
        """Executes a single refine task and records its metadata.

        A refine task receives the entire DataFrame and is expected to return
        a transformed DataFrame, which replaces the Loom's internal `database`.

        Args:
            refine_task (callable): The `@refine` decorated function or class
                wrapper to run.
        """
        # Get the meta information from refine object
        refine_meta = refine_task._refine_meta
        refine_name = refine_meta._refine_name
        # Run calculation through profiler and build nodes for the graph
        task_profiler = TaskProfiler(
            refine_task,
            track_time=True,
            track_data=True,
            data=self.database,
        )
        self.database = task_profiler.run()
        # Record all relevant information for refine graph
        self._record_refine_run(
            refine_task_name=refine_name,
            on_method=refine_meta._on_method,
            params=list(refine_meta._params),
            params_object=refine_meta._params_object,
            description=refine_meta._refine_description,
            delta_time=task_profiler.delta_time,
            rows_reduced=task_profiler.rows_reduced,
        )

    def _run(self):
        """Internal method to execute the full pipeline of tasks."""
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
        """Executes the full weaveflow pipeline.

        This method iterates through the provided `tasks`, dispatching to
        `_run_weave_task` or `_run_refine_task` as appropriate, and manages
        the state of the internal DataFrame.
        """
        self._run()
