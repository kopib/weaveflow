"""
This module provides abstract base classes for the graph visualization
components of weaveflow. These classes define the interface and core
functionality for building and styling the dependency graphs of `@weave` and
`@refine` tasks.
"""

from abc import ABC, abstractmethod

import graphviz
from networkx import DiGraph

from weaveflow._utils import _auto_convert_time_delta


class _BaseGraph(ABC):
    """Abstract base class for all weaveflow graphs."""

    def __init__(self):
        """Initializes the _BaseGraph.

        Args:
            loom (Loom): The `Loom` instance containing the execution
                metadata to be visualized.
        """
        self.graph = DiGraph()

    @property
    @abstractmethod
    def _collector(self):
        """Abstract property to get the task collector for the current loom instance."""
        pass

    @property
    @abstractmethod
    def _legend_details(self) -> tuple[list[str], list[str]]:
        """Return the names and colors for the legend."""
        pass

    @property
    @abstractmethod
    def _node_styles(self) -> dict:
        """Return the node style dictionary (shapes and colors)."""
        pass

    @abstractmethod
    def _style_graph_edges(self, g: graphviz.Digraph, collector: dict, **kwargs):
        """Abstract method to style the graph edges."""
        pass

    @abstractmethod
    def _setup(self):
        """Abstract method to set up the graph structure.

        This method should be implemented by subclasses to populate the
        `self.graph` with nodes and edges based on the loom's collectors.
        """
        pass

    def _create_task_clusters(self, g: graphviz.Digraph):
        """Creates visually distinct clusters for each weave task in the graph.

        This helps to group a task with its direct parameter inputs, improving
        readability.

        Args:
            g (graphviz.Digraph): The graphviz graph to modify.
        """
        for fn, vals in self._collector.items():
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

    def build(
        self,
        graph: graphviz.Digraph | None = None,
        additional_graph_attr: dict[str, str] | None = None,
        size: int = 12,
        mindist: float = 1.2,
        legend: bool = True,
        sink_source: bool = False,
    ) -> graphviz.Digraph:
        """Builds and returns the Graphviz Digraph object.

        This method is the skeleton for building the graph. Subclasses should
        implement the `_setup` method to populate the `self.graph` with nodes
        and edges based on the loom's collectors.

        Args:
            graph (graphviz.Digraph | None, optional): An existing graphviz
                graph to add nodes and edges to. If None, a new graph is created.
                Defaults to None.
            size (int, optional): The size of the graph in inches (e.g., "12,12!").
                Defaults to 12.
            mindist (float, optional): Minimum distance between nodes in the graph.
                Adjusting this can help with graph layout. Defaults to 1.2.
            legend (bool, optional): If True, includes a color-coded legend
                explaining the different node types (e.g., required inputs, outputs).
                Defaults to True.
            sink_source (bool, optional): If True, ranks source and sink nodes.
                Defaults to False.

        Returns:
            graphviz.Digraph: A Graphviz Digraph object representing the weave graph.
                This object can be rendered to various image formats (e.g., PNG, SVG)
                using its `.render()` method.
        """
        self._setup(self.loom.weaveflow_name)

        node_styles = self._node_styles
        clrs = {k: v[1] for k, v in node_styles.items()}
        shapes = {k: v[0] for k, v in node_styles.items()}

        graph_attr = _BaseGraph._get_graph_attr(
            {
                "size": f"{size},{size}!",
                "mindist": str(mindist),
                "label": f"<<b>{self.__class__.__name__} for "
                f"{self.loom.weaveflow_name!r}</b>>",
                "labelloc": "t",
            }
        )

        # Update graph attributes with any additional ones provided
        if isinstance(additional_graph_attr, dict):
            graph_attr.update(additional_graph_attr)

        g = graph or graphviz.Digraph(graph_attr=graph_attr)

        # Rank source and sink nodes
        if sink_source:
            self._rank_source_and_sink_nodes(g)
        # Style nodes according to their type
        self._style_graph_nodes(g, shapes, clrs)
        # Add legend
        if legend:
            legend_names, legend_colors = self._legend_details
            _BaseGraph._set_graph_legend(g, legend_names, legend_colors)

        return g

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

    @staticmethod
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

    @staticmethod
    def _add_graph_nodes(graph: DiGraph, nodes: str | list[str], **attrs) -> None:
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
                _BaseGraph._add_graph_nodes(graph, node, **attrs)
            return

        node = nodes

        # If argument label changed, assign required (or hybrid) color
        if node in graph.nodes:
            if graph.nodes[node] != attrs:
                graph.add_node(node, type="arg_req")
        else:
            graph.add_node(node, **attrs)

    @staticmethod
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


__all__ = ["_BaseGraph"]
