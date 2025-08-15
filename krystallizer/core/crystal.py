from abc import ABC, abstractmethod
from collections import defaultdict
import pandas as pd

from krystallizer._decorators._weave import _is_weave


class _BaseWeave(ABC):
    """Abstract base class for all krystallizer weaves."""

    def __init__(self, weave_tasks: list[callable], weave_name: str):
        self.weave_tasks = weave_tasks
        self.weave_name = weave_name
        self.weave_collector = defaultdict(dict)

    def __pre_init__(self):

        if not isinstance(self.weave_tasks, list):
            raise TypeError("'weave_tasks' must be a list of weave tasks")

        for weave_task in self.weave_tasks:
            if not _is_weave(weave_task):
                raise TypeError(
                    f"Argument 'weave_tasks' contains a non-weave task: {weave_task!r}"
                )

    @abstractmethod
    def run(self):
        """Run the main krystallizer application."""
        pass


class PandasWeave(_BaseWeave):
    """PandasWeave class."""

    def __init__(
        self,
        database: pd.DataFrame,
        weave_tasks: list[callable],
        weave_name: str = "default",
        optionals: dict[str, dict[str]] = None,
        **kwargs,
    ):
        super().__init__(weave_tasks, weave_name)
        self.database = database.copy()
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
    def apply_weave_meta_to_df(
        df: pd.DataFrame, transformed_weave: callable
    ) -> pd.DataFrame:
        meta = getattr(transformed_weave, "_suture_meta", None)

        if not isinstance(meta, dict):
            return df

        return df.rename(columns=meta)

    def _optionals_from_kwargs(
        self, weave_name: str, weave_optionals: list[str]
    ) -> dict:
        for oarg in weave_optionals:
            if oarg in self.kwargs:
                self.optionals[weave_name].update({oarg: self.kwargs[oarg]})

    def run(self):
        """Run the krystallizer on the database."""
        for weave_task in self.weave_tasks:

            # Get the function name
            weave_name = getattr(weave_task, "__name__")
            weave_meta = getattr(weave_task, "_weave_meta")

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
                transformed_weave=weave_task,
            )
            self.check_intersection_columns_dataframe(
                df=database_t, expected_cols=required_args
            )

            # Prepare the required arguments for the weave task + optional arguments
            rargs = database_t[required_args].to_dict(orient="series")

            # Run calculation and build the graph
            self.weave_collector[self.weave_name][weave_name] = {
                "outputs": outputs,
                "rargs": required_args,
                "oargs": optional_args,
                "params": list(params),
            }
            calculation_output = weave_task(**rargs, **oargs, **params)

            if calculation_output is None:
                self.weave_collector[self.weave_name].pop(weave_task)
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
