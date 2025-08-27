"""
This module provides the graph visualization capabilities for weaveflow.

It is the visualization heart of the library, responsible for transforming the
metadata collected by the `Loom` orchestrator into insightful, easy-to-read
graphs. It uses the `networkx` library for graph data structure construction
and the `graphviz` library for rendering the final visual output.

This module provides two main classes for visualization:

- **`WeaveGraph`**: This class visualizes the data lineage of the feature
  engineering steps defined by `@weave` tasks. It generates a directed acyclic
  graph (DAG) where nodes represent data columns (inputs, outputs, parameters)
  and the `@weave` functions themselves. The edges clearly show how data flows
  from input columns, through transformation tasks, to produce new output
  columns. This is invaluable for understanding how features are derived and
  for debugging complex dependencies. It can also generate a `WeaveMatrix` for
  a tabular view of the dependencies.

- **`RefineGraph`**: This class visualizes the high-level, sequential flow of
  the data refinement process. It creates a linear graph that shows the order
  in which `@refine` tasks are executed, illustrating the transformation of the
  DataFrame from its initial state ("Start DataFrame") to its final state
  ("End DataFrame"). It also visualizes any `@spool`-decorated objects that
  provide parameters to these refinement steps.

Both graph classes are highly customizable, offering control over size, layout,
the display of execution timers, and the inclusion of a legend.
"""

from abc import ABC, abstractmethod

import graphviz
import networkx as nx
from pandas import DataFrame

from weaveflow._utils import _auto_convert_time_delta, _convert_large_int_to_human_readable

from ._matrix import WeaveMatrix
from .loom import Loom


def _get_graph_attr(attrs: dict[str, str] | None = None):
    """Sets default attributes for a graphviz graph.

    Args:
        attrs (dict[str, str] | None, optional): A dictionary of attributes
            to override the defaults. Defaults to None.

    Returns:
        dict: A dictionary of graph attributes.
    """
    graph_attr = {
        "rankdir": "LR",
        "nodesep": "0.2",
        "ranksep": "1.0",
        "fontname": "Helvetica",
        "fontsize": "10",
        "concentrate": "true",
    }
    return graph_attr | (attrs or {})


def _add_graph_nodes(graph: nx.DiGraph, nodes: str | list[str], **attrs) -> None:
    """Adds one or more nodes to a networkx graph with specified attributes.

    If a node already exists and the new attributes are different, it updates
    the node's type to 'arg_req', which is useful for highlighting nodes that
    serve as both inputs and outputs.

    Args:
        graph (nx.DiGraph): The networkx graph to modify.
        nodes (str | list[str]): A single node name or a list of node names.
        **attrs: Attributes to assign to the node(s).
    """

    if nodes is None:
        return

    if isinstance(nodes, (list, tuple)):
        for node in nodes:
            _add_graph_nodes(graph, node, **attrs)
        return

    node = nodes

    # If argument label changed, assign required (or hybrid) color
    if node in graph.nodes:
        if graph.nodes[node] != attrs:
            graph.add_node(node, type="arg_req")
    else:
        graph.add_node(node, **attrs)


def _set_graph_legend(
    graph: graphviz.Digraph, names: list[str], colors: list[str]
) -> graphviz.Digraph:
    """Adds a legend to a graphviz graph.

    Creates a subgraph cluster for the legend with styled nodes representing
    different components of the graph.

    Args:
        graph (graphviz.Digraph): The graph to add the legend to.
        names (list[str]): A list of labels for the legend items.
        colors (list[str]): A list of colors corresponding to the labels.

    Returns:
        graphviz.Digraph: The graph with the legend added.
    """
    with graph.subgraph(name="cluster_legend") as c:
        c.attr(
            label="<<b>Legend</b>>",
            fontsize="12",
            color="black",
            style="rounded",
            nodesep="0.15",
            ranksep="0.01",
            padding="0.05",
        )
        for name, color in zip(names, colors, strict=False):
            c.node(
                name,
                shape="box",
                style="filled",
                fillcolor=color,
                height="0.12",
                fontsize="10",
            )

    return graph


class _BaseGraph(ABC):
    """Abstract base class for all weaveflow graphs."""

    def __init__(self, loom: Loom):
        """Initializes the _BaseGraph.

        Args:
            loom (Loom): The `Loom` instance containing the execution
                metadata to be visualized.
        """
        self.loom = loom
        self.graph = nx.DiGraph()

    @abstractmethod
    def _setup(self):
        """Abstract method to set up the graph structure.

        This method should be implemented by subclasses to populate the
        `self.graph` with nodes and edges based on the loom's collectors.
        """
        pass

    @abstractmethod
    def build(self):
        """Abstract method to build and return the visual graph.

        This method should be implemented by subclasses to take the
        `self.graph` and render it into a `graphviz.Digraph` object.
        """
        pass

    @staticmethod
    def _extract_date_from_collection(node: str, collection: dict[str, dict]) -> str:
        """Extracts and formats the execution time for a task node.

        Args:
            node (str): The name of the task node.
            collection (dict[str, dict]): The metadata collector (e.g.,
                `weave_collector` or `refine_collector`).

        Returns:
            str: The formatted execution time string (e.g., "12.3") or None
                if not available.
        """
        dt = collection[node]["delta_time"] if node in collection else None
        if dt is None:
            return None
        return _auto_convert_time_delta(dt)

    def _style_graph_nodes(self, g: graphviz.Digraph, shapes: dict, colors: dict):
        """Applies styles (shape, color) to all nodes in the graph.

        Args:
            g (graphviz.Digraph): The graphviz graph to style.
            shapes (dict): A dictionary mapping node types to shape names.
            colors (dict): A dictionary mapping node types to color codes.
        """
        for k, v in self.graph.nodes.items():
            node_type = v.get("type")
            if node_type in shapes:
                g.node(
                    k,
                    shape=shapes[node_type],
                    style="filled",
                    fillcolor=colors[node_type],
                    height="0.35",
                )

    def _rank_source_and_sink_nodes(self, g: graphviz.Digraph) -> None:
        """Ranks source and sink nodes to improve graph layout.

        This can improve readability by aligning start and end nodes, but may
        also clutter the graph.

        Args:
            g (graphviz.Digraph): The graphviz graph to modify.
        """
        # Identify and rank source and sink nodes
        source_nodes = [n for n, d in self.graph.in_degree() if d == 0]
        sink_nodes = [n for n, d in self.graph.out_degree() if d == 0]

        with g.subgraph() as s:
            s.attr(rank="source")
            for node in source_nodes:
                s.node(node)

        with g.subgraph() as s:
            s.attr(rank="sink")
            for node in sink_nodes:
                s.node(node)


class WeaveGraph(_BaseGraph):
    """Generates and visualizes the dependency graph for 'weave' tasks.

    This class uses networkx to build a directed graph representing the flow
    of data through `weave` tasks within a `Loom` instance. It highlights
    inputs, outputs, and parameters, and can optionally display execution times.

    Attributes:
        loom (Loom): The Loom instance containing the executed weave tasks.
        weave_collector (defaultdict): A collection of metadata for weave tasks.
    """

    def __init__(self, loom: Loom):
        """Initializes the WeaveGraph with a Loom instance.

        Args:
            loom (Loom): The Loom instance whose weave tasks will be visualized.
        """
        super().__init__(loom)
        self.weave_collector = loom.weave_collector

    def _setup(self, weaveflow_name: str):
        """Builds the internal networkx graph from weave task metadata.

        Iterates through the `weave_collector` for the specified `weaveflow_name`
        and adds nodes for tasks, inputs, outputs, and parameters, along with
        the corresponding edges representing data flow.

        Args:
            weaveflow_name (str): The name of the weaveflow pipeline to build
                the graph for.
        """

        for fn, vals in self.weave_collector[weaveflow_name].items():
            # Get all relevant nodes
            outputs = vals["outputs"]
            rargs = vals["rargs"]
            oargs = vals["oargs"]
            params = vals["params"]

            # Assign node types to access them later
            _add_graph_nodes(self.graph, fn, type="weave")
            _add_graph_nodes(self.graph, outputs, type="outputs")
            _add_graph_nodes(self.graph, rargs, type="arg_req")
            _add_graph_nodes(self.graph, params, type="arg_param")
            _add_graph_nodes(self.graph, oargs, type="arg_opt")

            # Add edges
            self.graph.add_edges_from([(v, fn) for v in rargs])
            self.graph.add_edges_from([(v, fn) for v in params])
            self.graph.add_edges_from([(v, fn) for v in oargs])
            self.graph.add_edges_from([(fn, v) for v in outputs])

    def _create_task_clusters(self, g: graphviz.Digraph, weave_collector: dict):
        """Creates visually distinct clusters for each weave task in the graph.

        This helps to group a task with its direct parameter inputs, improving
        readability.

        Args:
            g (graphviz.Digraph): The graphviz graph to modify.
            weave_collector (dict): The metadata collector for weave tasks.
        """
        # TODO: Fix warning
        for fn, vals in weave_collector.items():
            with g.subgraph(name=f"cluster_{fn}") as c:
                c.attr(
                    label=fn,
                    style="rounded",
                    color="gray",
                    fontcolor="gray",
                    fontsize="10",
                )
                # Add the main task node and its direct parameter nodes to the cluster
                c.node(fn)
                for p_node in vals.get("params", []):
                    c.node(p_node)

    def _style_graph_edges(self, g: graphviz.Digraph, weave_collector: dict, timer: bool):
        """Applies styles to edges in the graph.

        This includes making edges for optional arguments dashed and adding
        execution time labels if `timer` is enabled.

        Args:
            g (graphviz.Digraph): The graphviz graph to style.
            weave_collector (dict): The metadata collector for weave tasks.
            timer (bool): If True, adds execution time labels to edges
                originating from weave tasks.
        """
        for n1, n2 in self.graph.edges():
            edge_attrs = {}
            node1_type = self.graph.nodes[n1].get("type")

            if node1_type == "arg_opt":
                edge_attrs["style"] = "dashed"

            if node1_type == "weave" and timer:
                label = self._extract_date_from_collection(n1, weave_collector)
                if label:
                    edge_attrs.update(
                        {
                            "label": label,
                            "fontsize": "10",
                            "fontname": "Helvetica",
                            "labeldistance": "0.5",
                            "decorate": "False",
                        }
                    )
            g.edge(n1, n2, **edge_attrs)

    def build(
        self,
        size: int = 12,
        timer: bool = False,
        mindist: float = 1.2,
        legend: bool = True,
        sink_source: bool = False,
        cluster_tasks: bool = False,
    ) -> graphviz.Digraph:
        """Builds and returns the Graphviz Digraph object for the weave tasks.

        This method constructs a visual representation of the weave task
        dependencies, showing how data flows between different operations.

        Args:
            size (int, optional): The size of the graph in inches (e.g., "12,12!").
                Defaults to 12.
            timer (bool, optional): If True, displays the execution time for
                each weave task on the edges connecting to its outputs.
                Defaults to False.
            mindist (float, optional): Minimum distance between nodes in the graph.
                Adjusting this can help with graph layout. Defaults to 1.2.
            legend (bool, optional): If True, includes a color-coded legend
                explaining the different node types (e.g., required inputs, outputs).
                Defaults to True.
            sink_source (bool, optional): If True, ranks source and sink nodes.
                Defaults to False.
            cluster_tasks (bool, optional): If True, groups each weave task and
                its parameters into a distinct visual cluster. Defaults to True.

        Returns:
            graphviz.Digraph: A Graphviz Digraph object representing the weave graph.
                This object can be rendered to various image formats (e.g., PNG, SVG)
                using its `.render()` method.

        Example:
            ```python
            import pandas as pd
            import weaveflow as wf

            # Assume 'loomer' is an initialized and run Loom instance
            # from a pipeline with weave tasks.
            # For example:
            # df = pd.DataFrame(...)
            # loomer = wf.Loom(df, tasks=[my_weave_task_1, my_weave_task_2])
            # loomer.run()

            weave_graph = wf.WeaveGraph(loomer)
            g = weave_graph.build(timer=True, size=20)
            g.render("assets/output/graphs/weave_graph", format="png", cleanup=True)
            ```

        Returns:
            graphviz.Digraph: Plotted graph.
        """
        self._setup(self.loom.weaveflow_name)

        weave_collector = self.weave_collector[self.loom.weaveflow_name]
        # TODO: Make this configurable
        weave_nodes_style = {
            "weave": ("box", "#9999ff"),
            "arg_req": ("box", "#f08080"),
            "arg_opt": ("box", "#99ff99"),
            "outputs": ("box", "#fbec5d"),
            "arg_param": ("box", "#ffb6c1"),
        }
        clrs = {k: v[1] for k, v in weave_nodes_style.items()}
        shapes = {k: v[0] for k, v in weave_nodes_style.items()}

        # Define attributes for final directed graph
        graph_attr = _get_graph_attr(
            {
                "size": f"{size},{size}!",
                "mindist": str(mindist),
                "label": f"<<b>Weave Graph for {self.loom.weaveflow_name!r}</b>>",
                "labelloc": "t",
            }
        )

        g = graphviz.Digraph(graph_attr=graph_attr)

        # Rank source and sink nodes if specified
        if sink_source:
            self._rank_source_and_sink_nodes(g)

        # Create clusters for each weave task
        if cluster_tasks:
            self._create_task_clusters(g, weave_collector)

        # Style nodes and edges
        self._style_graph_nodes(g, shapes, clrs)
        self._style_graph_edges(g, weave_collector, timer)

        # Add legend if specified
        if legend:
            _set_graph_legend(
                g,
                [
                    "Required Arguments",
                    "Optional Arguments",
                    "Weaves",
                    "Outputs",
                    "SPool Arguments",
                ],
                list(clrs.values()),
            )

        return g

    def build_matrix(self) -> DataFrame:
        """Construct and return a WeaveMatrix for the current weaveflow.

        The matrix provides a tabular view of the dependencies between weave
        tasks and their arguments (inputs and outputs).

        The matrix is built only from weave tasks. If refine entries exist in
        the weave_collector (due to upstream population), they are filtered out
        by requiring the presence of the "outputs" key.

        Returns:
            pd.DataFrame: A pandas DataFrame representing the WeaveMatrix.

        Example:
            ```python
            # Assuming 'loomer' is an initialized and run Loom instance
            weave_graph = wf.WeaveGraph(loomer)
            weave_matrix = weave_graph.build_matrix()
            print(weave_matrix.head())
            ```
        """
        weaveflow_name = self.loom.weaveflow_name
        task_collection = self.weave_collector.get(weaveflow_name, {})
        # Filter to weave-only entries expected by WeaveMatrix
        weave_only = {
            name: vals
            for name, vals in task_collection.items()
            if isinstance(vals, dict) and "outputs" in vals
        }
        return WeaveMatrix(weave_only).build()


class RefineGraph(_BaseGraph):
    """Generates and visualizes the sequential flow graph for 'refine' tasks.

    This class uses networkx to build a directed graph representing the ordered
    execution of `refine` tasks within a `Loom` instance. It shows the flow
    from an initial DataFrame state through various refinement steps to a final state.

    Attributes:
        loom (Loom): The Loom instance containing the executed refine tasks.
        refine_collector (defaultdict): A collection of metadata for refine tasks.
    """

    def __init__(self, loom: Loom):
        """Initializes the RefineGraph with a Loom instance.

        Args:
            loom (Loom): The Loom instance whose refine tasks will be visualized.
        """
        super().__init__(loom)
        self.refine_collector = loom.refine_collector

    def _setup(self, weaveflow_name: str):
        """Builds the internal networkx graph from refine task metadata.

        Iterates through the `refine_collector` and creates a sequential
        graph representing the flow of DataFrame transformations. It adds
        nodes for tasks, parameters, and start/end boundaries.

        Args:
            weaveflow_name (str): The name of the weaveflow pipeline to build
                the graph for.
        """
        refine_collector = self.refine_collector[weaveflow_name]
        # Add nodes and edges for each refine task
        for fn, vals in refine_collector.items():
            params = vals["params"]
            params_object = vals["params_object"]
            # on_method = vals["on_method"]
            # description = vals["description"]

            _add_graph_nodes(self.graph, fn, type="refine")
            _add_graph_nodes(self.graph, params, type="arg_param")
            _add_graph_nodes(self.graph, params_object, type="obj_param")
            # _add_graph_nodes(self.graph, on_method, type="on_method")
            # _add_graph_nodes(self.graph, description, type="description")

            # TODO: Integrate description as tooltip
            # TODO: Intergate on_method argument as label

            if params and params_object:
                # Add connection between params (config file args) and params_object
                self.graph.add_edges_from(
                    [(v, params_object) for v in params], flow="data"
                )
                # Add connection between params_object and refine task
                self.graph.add_edge(params_object, fn, flow="data")

        # Add edges between refine tasks and connect to Start/End
        refine_tasks = list(refine_collector)
        if refine_tasks:
            # Add edges between refine tasks
            if len(refine_tasks) > 1:
                self.graph.add_edges_from(
                    [(i, j) for i, j in zip(refine_tasks, refine_tasks[1:], strict=False)],
                    flow="control",
                )

            # Add Start/End nodes and connect them to the flow
            _add_graph_nodes(self.graph, "Start DataFrame", type="boundary")
            _add_graph_nodes(self.graph, "End DataFrame", type="boundary")
            self.graph.add_edge("Start DataFrame", refine_tasks[0], flow="control")
            self.graph.add_edge(refine_tasks[-1], "End DataFrame", flow="control")

    def _create_task_clusters(self, g: graphviz.Digraph, refine_collector: dict) -> None:
        """Creates visually distinct clusters for each refine task in the graph.

        This groups a task with its associated `@spool` object and parameters.

        Args:
            g (graphviz.Digraph): The graphviz graph to modify.
            refine_collector (dict): The metadata collector for refine tasks.
        """
        for fn, vals in refine_collector.items():
            with g.subgraph(name=f"cluster_{fn}") as c:
                c.attr(
                    label=fn,
                    style="rounded",
                    color="gray",
                    fontcolor="gray",
                    fontsize="10",
                )
                # Add the main task node and its parameter objects to the cluster
                c.node(fn)
                if vals.get("params_object"):
                    c.node(vals["params_object"])
                for p_node in vals.get("params", []):
                    c.node(p_node)

    @staticmethod
    def _extract_profiler_from_collection(node: str, collection: dict) -> str | None:
        """Extracts row reduction info and formats it for the edge label."""
        rows_reduced: int = (
            collection[node]["rows_reduced"] if node in collection else None
        )
        if rows_reduced is None:
            return None

        return _convert_large_int_to_human_readable(rows_reduced)

    def _style_graph_edges(
        self,
        g: graphviz.Digraph,
        refine_collector: dict,
        timer: bool,
        data_profiler: bool,
    ):
        """Applies styles (penwidth, labels) to all edges in the graph."""

        # Iterate through all existing edges in the graph
        for n1, n2 in self.graph.edges():
            edge_attrs = {}
            edge_data = self.graph.edges[n1, n2]

            # Set edge width for control flow edges
            if edge_data.get("flow") == "control":
                edge_attrs["penwidth"] = "2.0"

            # Build label from multiple parts
            label_parts = []

            # Add execution time label if enabled
            if timer:
                _time_label = self._extract_date_from_collection(n1, refine_collector)
                if _time_label:
                    label_parts.append(_time_label)

            # Add row reduction label if enabled
            if data_profiler:
                _profiler_label = self._extract_profiler_from_collection(
                    n1, refine_collector
                )
                if _profiler_label:
                    label_parts.append(f"🔻 {_profiler_label} rows")

            # Add label to edge if parts exist
            if label_parts:
                final_label = " | ".join(label_parts)
                edge_attrs.update(
                    {
                        "label": final_label,
                        "fontsize": "10",
                        "fontname": "Helvetica",
                        "labeldistance": "0.5",
                        "decorate": "False",
                    }
                )
            # Update the edge with the new attributes
            g.edge(n1, n2, **edge_attrs)

    def build(
        self,
        size: int = 12,
        timer: bool = False,
        data_profiler: bool = False,
        mindist: float = 1.2,
        legend: bool = True,
        sink_source: bool = False,
        cluster_tasks: bool = False,
    ) -> graphviz.Digraph:
        """Builds and returns the Graphviz Digraph object for the refine tasks.

        This method constructs a visual representation of the sequential refine
        tasks, showing the order of operations on the DataFrame.

        Args:
            size (int, optional): The size of the graph in inches (e.g., "12,12!").
                Defaults to 12.
                TODO: Implement intelligent sizing based on the number of tasks.
            timer (bool, optional): If True, displays the execution time for
                each refine task on the edges between tasks. Defaults to False.
            database_profiler (bool, optional): If True, displays the row reduction
                for each refine task on the edges between tasks. Defaults to False.
            mindist (float, optional): Minimum distance between nodes in the graph.
                Adjusting this can help with graph layout. Defaults to 1.2.
            legend (bool, optional): If True, includes a color-coded legend
                explaining the different node types (e.g., refine tasks, spool objects).
                Defaults to True.
            sink_source (bool, optional): If True, ranks source and sink nodes.
                Defaults to False.
            cluster_tasks (bool, optional): If True, groups each refine task and
                its parameters into a distinct visual cluster. Defaults to True.

        Returns:
            graphviz.Digraph: A Graphviz Digraph object representing the refine graph.
                This object can be rendered to various image formats (e.g., PNG, SVG)
                using its `.render()` method.

        Example:
            ```python
            import pandas as pd
            import weaveflow as wf

            # Assume 'loomer' is an initialized and run Loom instance
            # from a pipeline with refine tasks.
            # df = pd.DataFrame(...)
            # loomer = wf.Loom(df, tasks=[my_refine_task_1, my_refine_task_2])
            # loomer.run()

            refine_graph = wf.RefineGraph(loomer)
            g = refine_graph.build(timer=True)
            g.render("assets/output/graphs/refine_graph", format="png", cleanup=True)
            ```
        """
        self._setup(self.loom.weaveflow_name)

        # Get refine collector for current weaveflow
        refine_collector = self.refine_collector[self.loom.weaveflow_name]

        # TODO: Make this configurable
        refine_nodes_style = {
            "refine": ("box", "#9999ff"),
            "obj_param": ("box", "#f08080"),
            "arg_param": ("box", "#ffb6c1"),
            "boundary": ("box", "#fbec5d"),
        }
        clrs = {k: v[1] for k, v in refine_nodes_style.items()}
        shapes = {k: v[0] for k, v in refine_nodes_style.items()}

        # Define attributes for final directed graph
        graph_attr = _get_graph_attr(
            {
                "size": f"{size},{size}!",
                "mindist": str(mindist),
                "label": f"<<b>Refine Graph for {self.loom.weaveflow_name!r}</b>>",
                "labelloc": "t",
            }
        )

        g = graphviz.Digraph(graph_attr=graph_attr)

        # Rank source and sink nodes if specified
        if sink_source:
            self._rank_source_and_sink_nodes(g)

        # Create clusters for each refine task
        if cluster_tasks:
            self._create_task_clusters(g, refine_collector)

        # Style nodes and edges
        self._style_graph_nodes(g, shapes, clrs)
        self._style_graph_edges(g, refine_collector, timer, data_profiler)

        if legend:
            _set_graph_legend(
                g,
                [
                    "Refine Tasks",
                    "SPool Objects",
                    "SPool Arguments",
                    "DataFrame State",
                ],
                [
                    clrs["refine"],
                    clrs["obj_param"],
                    clrs["arg_param"],
                    clrs["boundary"],
                ],
            )

        return g
