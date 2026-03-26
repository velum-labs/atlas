from __future__ import annotations

from alma_graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    extract_lineage_subgraph,
    to_networkx_digraph,
)


def make_graph() -> LineageGraph:
    return LineageGraph(
        nodes=[
            LineageNode(
                id="proj.raw.orders",
                node_type="table",
                name="orders",
                metadata={"project_id": "proj", "dataset_id": "raw", "table_id": "orders"},
            ),
            LineageNode(
                id="proj.analytics.orders_view",
                node_type="view",
                name="orders_view",
                metadata={
                    "project_id": "proj",
                    "dataset_id": "analytics",
                    "table_id": "orders_view",
                },
            ),
            LineageNode(
                id="consumer:analyst@example.com",
                node_type="consumer",
                name="analyst@example.com",
                metadata={"project_id": "proj", "consumer_kind": "user"},
            ),
        ],
        edges=[
            LineageEdge(
                id="e1",
                source="proj.raw.orders",
                target="proj.analytics.orders_view",
                edge_type="view_depends_on",
                metadata={"query_count": 1},
            ),
            LineageEdge(
                id="e2",
                source="proj.analytics.orders_view",
                target="consumer:analyst@example.com",
                edge_type="reads",
                metadata={"query_count": 1},
            ),
        ],
    )


def test_to_networkx_digraph_filters_edge_types() -> None:
    graph = make_graph()
    nx_graph = to_networkx_digraph(graph, edge_types=["reads"])

    assert nx_graph.number_of_nodes() == 3
    assert nx_graph.number_of_edges() == 1
    edge = next(iter(nx_graph.edges(data=True)))
    assert edge[2]["edge_type"] == "reads"


def test_extract_lineage_subgraph_by_asset() -> None:
    graph = make_graph()
    subgraph = extract_lineage_subgraph(
        graph,
        asset_id="proj.analytics.orders_view",
        edge_types=["reads", "view_depends_on"],
        hop_depth=1,
        max_nodes=10,
    )

    assert subgraph.truncated is False
    assert set(subgraph.graph.nodes) == {
        "proj.raw.orders",
        "proj.analytics.orders_view",
        "consumer:analyst@example.com",
    }
