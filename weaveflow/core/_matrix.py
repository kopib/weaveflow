import pandas as pd


class WeaveMatrix:
    """Matrix view for weave tasks.

    This class builds a table (pd.DataFrame) where:
    - Rows = all argument names seen across tasks (required inputs, optional inputs, and outputs)
    - Columns = weave task names
    - Cell values per task/argument:
        * "required input" if argument is a required input of the task
        * "optional input" if argument is an optional input of the task
        * "Output" if argument is produced by the task
        * "" otherwise

    Note:
    - Unlike _BaseMatrix, this class does NOT receive the Loom. It receives a
      task collection (e.g., loom.weave_collector[weaveflow_name] filtered to weave tasks).
    - The task collection is expected to be a mapping: task_name -> {
        "outputs": list[str], "rargs": list[str], "oargs": list[str], "params": list[str]
      }
    """

    def __init__(self, task_collection: dict[str, dict]):
        self._tasks = task_collection or {}

    def build(self) -> pd.DataFrame:
        """Construct and return the weave matrix as a pandas DataFrame."""
        # Collect all row labels (arguments and outputs)
        row_names: set[str] = set()
        for meta in self._tasks.values():
            row_names.update(meta.get("rargs", []) or [])
            row_names.update(meta.get("oargs", []) or [])
            row_names.update(meta.get("outputs", []) or [])

        rows = sorted(row_names)
        cols = sorted(self._tasks.keys())

        # If nothing to show, return a truly empty DataFrame (RangeIndex for index/columns)
        if not rows and not cols:
            return pd.DataFrame()

        # Build column-wise data
        data: dict[str, list[str]] = {}
        for task_name in cols:
            meta = self._tasks.get(task_name, {})
            rargs = set(meta.get("rargs", []) or [])
            oargs = set(meta.get("oargs", []) or [])
            outputs = set(meta.get("outputs", []) or [])

            col_values: list[str] = []
            for arg in rows:
                if arg in rargs:
                    col_values.append("required input")
                elif arg in oargs:
                    col_values.append("optional input")
                elif arg in outputs:
                    col_values.append("Output")
                else:
                    col_values.append("")
            data[task_name] = col_values

        return pd.DataFrame(data, index=rows, columns=cols)
