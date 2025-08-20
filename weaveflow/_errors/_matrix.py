class InvalidTaskCollectionError(ValueError):
    """Raised when WeaveMatrix receives an invalid or malformed task collection.

    Expected a mapping of the form: {task_name: {"rargs": list[str], "oargs": list[str], "outputs": list[str], "params": list[str]}}
    - Missing keys are tolerated (treated as empty), but wrong types are not.
    - All present lists must contain strings only.
    """

    def __init__(self, detail: str):
        super().__init__(f"Invalid task collection for WeaveMatrix: {detail}")

