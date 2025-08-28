from dataclasses import dataclass

import graphviz
import pandas as pd
import pytest

from weaveflow import Loom, RefineGraph, refine, spool


# Define some tasks for testing
@refine
def simple_refine_task(df: pd.DataFrame) -> pd.DataFrame:
    """A simple refine task that does nothing."""
    return df


@refine
def row_dropping_refine_task(df: pd.DataFrame) -> pd.DataFrame:
    """A refine task that drops some rows."""
    return df.head(len(df) // 2)


@spool(path="tests/data/dummy", file="dummy_spool.yaml")
@dataclass
class MyParams:
    a: int
    b: int


@refine(params_from=MyParams)
def refine_with_params(df: pd.DataFrame, a: int, b: str) -> pd.DataFrame:
    """A refine task that uses spooled parameters."""
    # The implementation doesn't matter for the graph test.
    return df


@pytest.mark.refinegraph
@pytest.mark.smoke
def test_refine_graph_smoke_test():
    """Test that the graph builds without errors."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    loom = Loom(df, [simple_refine_task])
    loom.run()

    g = RefineGraph(loom).build(legend=False)

    assert isinstance(g, graphviz.Digraph)
    src = g.source
    assert "simple_refine_task" in src
    assert '"Start DataFrame"' in src
    assert '"End DataFrame"' in src
    assert '"Start DataFrame" -> simple_refine_task' in src
    assert 'simple_refine_task -> "End DataFrame"' in src


@pytest.mark.refinegraph
@pytest.mark.flow
def test_refine_graph_sequential_flow():
    """Test that the graph shows the correct flow between tasks."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    tasks = [simple_refine_task]
    loom = Loom(df, tasks)
    loom.run()

    g = RefineGraph(loom).build(legend=False)
    src = g.source

    assert '"Start DataFrame" -> simple_refine_task' in src
    assert 'simple_refine_task -> "End DataFrame"' in src
    # Check for control flow style
    assert "penwidth=2.0" in src


@pytest.mark.refinegraph
@pytest.mark.profiler
def test_refine_graph_data_profiler():
    """Test that the data profiler feature works as expected."""
    df = pd.DataFrame({"a": range(10)})
    loom = Loom(df, [row_dropping_refine_task])
    loom.run()

    g = RefineGraph(loom).build(data_profiler=True, legend=False)
    src = g.source

    # The task drops 5 rows (10 -> 5)
    assert 'label="&#9662; 5 rows"' in src or 'label="🔻 5 rows"' in src


@pytest.mark.refinegraph
@pytest.mark.empty
def test_refine_graph_empty_tasks():
    """Test that the graph handles empty tasks gracefully."""
    df = pd.DataFrame({"a": [1, 2]})
    loom = Loom(df, [])
    loom.run()

    g = RefineGraph(loom).build(legend=False)
    src = g.source

    assert "->" not in src


@pytest.mark.refinegraph
@pytest.mark.params
def test_refine_graph_parameter_nodes():
    """Test that spooled assets and their params are graphed correctly."""
    df = pd.DataFrame({"a": [5, 15, 25]})
    loom = Loom(df, [refine_with_params])
    loom.run()

    g = RefineGraph(loom).build(legend=False)
    src = g.source

    # 1. Assert all expected nodes are present
    assert "refine_with_params" in src  # The refine task
    assert "MyParams" in src  # The spooled object
    assert "a" in src  # A spooled argument
    assert "b" in src  # Another spooled argument

    # 2. Assert the data flow edges are correct
    assert "a -> MyParams" in src
    assert "b -> MyParams" in src
    assert "MyParams -> refine_with_params" in src

    # 3. Assert the nodes have the correct styling (colors)
    assert 'MyParams [fillcolor="#f08080"' in src
    assert 'a [fillcolor="#ffb6c1"' in src
    assert 'b [fillcolor="#ffb6c1"' in src
