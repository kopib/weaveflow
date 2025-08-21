from abc import ABC, abstractmethod
from typing import Union
import graphviz

import networkx as nx
from weaveflow.core.loom import Loom, _BaseWeave
from weaveflow.core._matrix import WeaveMatrix


def _get_graph_attr(attrs: dict[str, str] = None):
    """Get graph attributes with optional overrides."""
    graph_attr = {
        "rankdir": "LR",
        "nodesep": "0.2",
        "ranksep": "1.0",
        "fontname": "Helvetica",
        "fontsize": "10",
        "concentrate": "true",
    }
    return graph_attr | (attrs or {})


def _add_graph_nodes(graph: nx.DiGraph, nodes: Union[str, list[str]], **attrs) -> None:
    """Updates edges in graph. Throws an error if a key is present and values don't match.

    Args:
        graph (network.DiGraph): Graph to be updated.
        nodes (str, list): Node(s) to be updated
        **attrs: Attributes(s)
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
        for name, color in zip(names, colors):
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
        """Create a graph for a given weaveflow."""
        self.loom = loom
        self.graph = nx.DiGraph()

    @abstractmethod
    def _setup(self):
        """Method to setup the graph."""
        pass

    @abstractmethod
    def build(self):
        """Method to build the graph."""
        pass

    @staticmethod
    def _extract_date_from_collection(node: str, collection: dict[str, dict]) -> str:
        """Extract delta time from collection if available."""
        dt = collection[node]["delta_time"] if node in collection else None
        return f"{dt:,.1f}" if dt else None


class WeaveGraph(_BaseGraph):
    """Object to create a final graph for a given pandas weaveflow."""

    def __init__(self, loom: Loom):
        super().__init__(loom)
        self.weave_collector = loom.weave_collector

    def _setup(self, weaveflow_name: str):
        """Create graph by adding all relevant nodes and edges."""

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

    def build(
        self,
        size: int = 12,
        timer: bool = False,
        mindist: float = 1.2,
        legend: bool = True,
    ):
        """Plots final graph for a given weaveflow.

        Args:
            size (int): Size of figure. Defaults to 12.
            timer (bool): Flag whether timer should also be plotted. Defaults to False.
            mindist (float): Minimum distance between nodes. Defaults to 1.2.
            legend (bool): Legend for graph. Defaults to True.

        Returns:
            graphviz.Digraph: Plotted graph.
        """
        self._setup(self.loom.weaveflow_name)

        weave_collector = self.weave_collector[self.loom.weaveflow_name]

        clrs = {
            "weave": "#9999ff",
            "arg_req": "#f08080",
            "arg_opt": "#99ff99",
            "outputs": "#fbec5d",
            "arg_param": "#ffb6c1",
        }
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

        for k, v in self.graph.nodes.items():
            g.node(k, shape="box", style="filled", fillcolor=clrs[v["type"]], height="0.35")

        for n1, n2 in self.graph.edges():
            # If left node is weave and produces output
            if self.graph.nodes[n1]["type"] == "weave" and timer:
                # Extract time of function execution
                label = self._extract_date_from_collection(n1, weave_collector)
                if label:
                    g.edge(
                        n1,
                        n2,
                        label=label,
                        fontsize="10",
                        fontname="Helvetica",
                        labeldistance="0.5",
                        decorate="False",
                    )
                else:
                    g.edge(n1, n2)
            else:
                g.edge(n1, n2)

        # Plot legend if specified
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

    def build_matrix(self) -> WeaveMatrix:
        """Construct and return a WeaveMatrix for the current weaveflow.

        The matrix is built only from weave tasks. If refine entries exist in
        the weave_collector (due to upstream population), they are filtered out
        by requiring the presence of the "outputs" key.
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
    def __init__(self, loom: Loom):
        super().__init__(loom)
        self.refine_collector = loom.refine_collector

    def _setup(self, weaveflow_name: str):
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
            # TODO: Add timer to edges between tasks

            if params:
                # Add connection between params (config file args) and params_object
                self.graph.add_edges_from([(v, params_object) for v in params])
                # Add connection between params_object and refine task
                self.graph.add_edges_from([(params_object, fn)])

        # Add edges between refine tasks and connect to Start/End
        refine_tasks = list(refine_collector)
        if refine_tasks:
            # Add edges between refine tasks
            if len(refine_tasks) > 1:
                self.graph.add_edges_from([(i, j) for i, j in zip(refine_tasks, refine_tasks[1:])])

            # Add Start/End nodes and connect them to the flow
            _add_graph_nodes(self.graph, "Start DataFrame", type="boundary")
            _add_graph_nodes(self.graph, "End DataFrame", type="boundary")
            self.graph.add_edge("Start DataFrame", refine_tasks[0])
            self.graph.add_edge(refine_tasks[-1], "End DataFrame")

    def build(
        self,
        size: int = 12, # TODO: Make intelligent sizing depending on the number of tasks
        timer: bool = False,
        mindist: float = 1.2,
        legend: bool = True,
    ):
        self._setup(self.loom.weaveflow_name)

        # Get refine collector for current weaveflow
        refine_collector = self.refine_collector[self.loom.weaveflow_name]
        clrs = {
            "refine": "#9999ff",
            "obj_param": "#f08080",
            "arg_param": "#ffb6c1",
            "boundary": "#fbec5d",
        }
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

        for k, v in self.graph.nodes.items():
            g.node(k, shape="box", style="filled", fillcolor=clrs[v["type"]], height="0.35")

        for n1, n2 in self.graph.edges():
            if timer:
                # Extract time of function execution
                label = self._extract_date_from_collection(n1, refine_collector)
                if label:
                    g.edge(
                        n1,
                        n2,
                        label=label,
                        fontsize="10",
                        fontname="Helvetica",
                        labeldistance="0.5",
                        decorate="False",
                    )
                else:
                    g.edge(n1, n2)
            else:
                g.edge(n1, n2)

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
