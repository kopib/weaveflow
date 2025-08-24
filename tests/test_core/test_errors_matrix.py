import pytest

from weaveflow._errors import InvalidTaskCollectionError
from weaveflow.core._matrix import WeaveMatrix


def test_weave_matrix_raises_on_non_mapping_task_collection():
    with pytest.raises(InvalidTaskCollectionError, match="must be a mapping"):
        WeaveMatrix(task_collection=[("t1", {})])  # type: ignore[arg-type]


def test_weave_matrix_accepts_empty_and_mapping():
    # Should not raise for empty dict
    WeaveMatrix({})
    # Should not raise for proper mapping even if incomplete
    WeaveMatrix({"t1": {}})
