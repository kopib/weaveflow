from abc import ABC, abstractmethod
from typing import Union

import networkx as nx
from weaveflow.core.loom import Loom, _BaseWeave


class _WeaveGraph(ABC):
    """Abstract base class for all krystallizer weave graphs."""

    def __init__(self, weave: _BaseWeave):
        """Create a graph for a given weave.

        Args:
            weave (_BaseWeave): Weave to create graph for.
        """
        self.weave = weave
        self.graph = nx.DiGraph()

    @abstractmethod
    def build(self):
        """Method to build the graph."""
        pass


class Tapestry(_WeaveGraph):
    """Object to create a final graph for a given pandas weave."""

    def __init__(self, weave: Loom):
        super().__init__(weave)
        # TODO: Think about whether to use a dataclass here and whether to put into abstract class
        self.weave_collector = weave.weave_collector

    @staticmethod
    def _add_weave_nodes(
        graph: nx.DiGraph, nodes: Union[str, list[str]], **attrs
    ):
        """Updates edges in graph. Throws an error if a key is present and values don't match.

        Args:
            graph (network.DiGraph): Graph to be updated.
            nodes (str, list): Node(s) to be updated
            **attrs: Attributes(s)
        """
        if isinstance(nodes, (list, tuple)):
            for node in nodes:
                Tapestry._add_weave_nodes(graph, node, **attrs)
            return

        node = nodes

        # If argument label changed, assign required (or hybrid) color
        if node in graph.nodes:
            if graph.nodes[node] != attrs:
                graph.add_node(node, type="arg_req")
        else:
            graph.add_node(node, **attrs)

    def _setup(self, weave_name: str):
        """Create graph by adding all relevant nodes and edges."""

        for fn, vals in self.weave_collector[weave_name].items():

            # Get all relevant nodes
            outputs = vals["outputs"]
            rargs = vals["rargs"]
            oargs = vals["oargs"]
            params = vals["params"]

            # Assign node types to access them later
            self._nx_add_nodes_checking(self.graph, fn, type="suture")
            self._nx_add_nodes_checking(self.graph, outputs, type="outputs")
            self._nx_add_nodes_checking(self.graph, rargs, type="arg_req")
            self._nx_add_nodes_checking(self.graph, params, type="arg_param")
            self._nx_add_nodes_checking(self.graph, oargs, type="arg_opt")

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
        """Plots final graph for a given weave.

        Args:
            size (int): Size of figure. Defaults to 12.
            timer (bool): Flag whether timer should also be plotted. Defaults to False.
            mindist (float): Minimum distance between nodes. Defaults to 1.2.
            legend (bool): Legend for graph. Defaults to True.

        Returns:
            graphviz.Digraph: Plotted graph.
        """
        import graphviz

        self._setup(self.weave.weave_name)

        clrs = {
            "suture": "#9999ff",
            "arg_req": "#f08080",
            "arg_opt": "#99ff99",
            "outputs": "#fbec5d",
            "arg_param": "#ffb6c1",
        }
        # Define attributes for final directed graph
        size_str = f"{size},{size}!"
        graph_attr = {
            "rankdir": "LR",
            "size": size_str,
            "mindist": str(mindist),
            "nodesep": "0.2",
            "label": f"<<b>Graph for weave {self.weave.weave_name!r}</b>>",
            "ranksep": "1.0",
            "fontname": "Helvetica",
            "fontsize": "10",
            "concentrate": "true",
        }

        g = graphviz.Digraph(graph_attr=graph_attr)

        for k, v in self.graph.nodes.items():
            g.node(
                k, shape="box", style="filled", fillcolor=clrs[v["type"]], height="0.35"
            )

        for n1, n2 in self.graph.edges():
            # If left node is suture and produces output
            if self.graph.nodes[n1]["type"] == "suture" and timer:
                # Extract time of function execution
                dt = self.weave_collector[n1]["time"]
                # Create edge label (if rounded time equals zero, not print)
                label = f" {dt}s" if dt else ""
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

        # Plot legend if specified
        if legend:
            with g.subgraph(name="cluster_legend") as c:
                c.attr(
                    label="<<b>Legend</b>>",
                    fontsize="12",
                    color="black",
                    style="rounded",
                    nodesep="0.15",
                    ranksep="0.01",
                    padding="0.05",
                )
                c.node(
                    "Required Arguments",
                    shape="box",
                    style="filled",
                    fillcolor=clrs["arg_req"],
                    height="0.12",
                    fontsize="10",
                )
                c.node(
                    "Optional Arguments",
                    shape="box",
                    style="filled",
                    fillcolor=clrs["arg_opt"],
                    height="0.12",
                    fontsize="10",
                )
                c.node(
                    "Suture",
                    shape="box",
                    style="filled",
                    fillcolor=clrs["suture"],
                    height="0.12",
                    fontsize="10",
                )
                c.node(
                    "Final Outputs",
                    shape="box",
                    style="filled",
                    fillcolor=clrs["outputs"],
                    height="0.12",
                    fontsize="10",
                )
                c.node(
                    "Registry",
                    shape="box",
                    style="filled",
                    fillcolor=clrs["arg_param"],
                    height="0.12",
                    fontsize="10",
                )

        return g


class WeaveGraph(Tapestry):
    def __init__(self, weave: Loom):
        super().__init__(weave)
