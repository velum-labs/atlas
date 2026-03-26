from __future__ import annotations

import json

import networkx as nx

from alma_graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    flatten_graph_attributes,
    to_asset_only_multidigraph,
    write_full_graph_exports,
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
                metadata={"query_count": 1, "provenance_sources": {"ddl"}},
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


def test_flatten_graph_attributes() -> None:
    flattened = flatten_graph_attributes(
        {
            "project_id": "proj",
            "query_count": 2,
            "provenance_sources": ["ddl", "query_logs"],
            "nested": {"table_id": "orders"},
        }
    )
    assert flattened["meta_project_id"] == "proj"
    assert flattened["meta_query_count"] == 2
    assert flattened["meta_nested_table_id"] == "orders"
    assert json.loads(flattened["meta_json"])["nested"]["table_id"] == "orders"


def test_write_full_graph_exports(tmp_path) -> None:
    graph = make_graph()
    artifacts = write_full_graph_exports(
        graph,
        output_dir=tmp_path,
        write_graphml=True,
        write_gexf=True,
        write_ndjson=True,
        write_chunked_json=True,
        chunk_size=2,
    )

    assert artifacts.graphml_path and artifacts.graphml_path.exists()
    assert artifacts.asset_graphml_path and artifacts.asset_graphml_path.exists()
    assert artifacts.gexf_path and artifacts.gexf_path.exists()
    assert artifacts.asset_gexf_path and artifacts.asset_gexf_path.exists()

    graphml_graph = nx.read_graphml(artifacts.graphml_path)
    assert len(graphml_graph.nodes) == 3

    asset_graph = to_asset_only_multidigraph(graph)
    manifest = json.loads(artifacts.manifest_path.read_text())
    assert manifest["graph_stats"]["asset_only_nodes"] == asset_graph.number_of_nodes()
    assert manifest["graph_stats"]["asset_only_edges"] == asset_graph.number_of_edges()
