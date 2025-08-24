import graphviz
import pandas as pd
import pytest

from weaveflow import Loom, WeaveGraph, refine, weave


@pytest.mark.weavegraph
@pytest.mark.smoke
def test_weave_graph_build_smoke_no_legend():
    @weave(outputs="sum")
    def add(a: pd.Series, b: pd.Series):
        return a + b

    @weave(outputs="scaled")
    def scale(sum: pd.Series, k: int = 2):
        return sum * k

    @refine
    def touch(df: pd.DataFrame):
        return df

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    loom = Loom(df, [add, scale, touch])
    loom.run()

    g = WeaveGraph(loom).build(legend=False)

    assert isinstance(g, graphviz.Digraph)
    src = g.source
    assert "add" in src
    assert "scale" in src

    assert "sum" in src


@pytest.mark.weavegraph
@pytest.mark.legend
def test_weave_graph_legend_on_vs_off():
    @weave(outputs="out")
    def w(a: pd.Series):
        return a

    df = pd.DataFrame({"a": [1, 2]})
    loom = Loom(df, [w])
    loom.run()

    g_with = WeaveGraph(loom).build(legend=True)
    g_without = WeaveGraph(loom).build(legend=False)

    assert isinstance(g_with, graphviz.Digraph)
    assert isinstance(g_without, graphviz.Digraph)

    # legend subgraph appears only when legend=True
    assert "cluster_legend" in g_with.source
    assert "cluster_legend" not in g_without.source


@pytest.mark.weavegraph
@pytest.mark.edges
def test_weave_graph_edges_and_colors():
    @weave(outputs="y")
    def sum_ab(a: pd.Series, b: pd.Series, opt: int = 0):
        return a + b + opt

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    loom = Loom(df, [sum_ab])
    loom.run()

    g = WeaveGraph(loom).build(legend=False)
    assert isinstance(g, graphviz.Digraph)
    src = g.source

    # Edges: required inputs -> weave, weave -> output, optional -> weave
    assert "a -> sum_ab" in src
    assert "b -> sum_ab" in src
    assert "opt -> sum_ab" in src
    assert "sum_ab -> y" in src
    # Colors for node types should be present in DOT
    assert 'fillcolor="#9999ff"' in src  # weave nodes
    assert 'fillcolor="#f08080"' in src  # required args
    assert 'fillcolor="#99ff99"' in src  # optional args
    assert 'fillcolor="#fbec5d"' in src  # outputs


@pytest.mark.weavegraph
@pytest.mark.empty
def test_weave_graph_empty_tasks_smoke():
    df = pd.DataFrame({"a": [1, 2]})
    loom = Loom(df, [])
    loom.run()

    g = WeaveGraph(loom).build(legend=False)
    assert isinstance(g, graphviz.Digraph)
    # With no tasks, we expect no edges in the DOT source
    assert "->" not in g.source
