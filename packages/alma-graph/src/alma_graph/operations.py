"""Pure graph operations over the canonical lineage graph types."""

from __future__ import annotations

from typing import Any

from alma_graph.types import LineageGraph, LineageSubgraph


def to_networkx_digraph(
    lineage_graph: LineageGraph,
    *,
    edge_types: list[str] | None = None,
) -> Any:
    """Convert a lineage graph into a `networkx.DiGraph`."""

    import networkx as nx

    allowed_edge_types = set(edge_types or [])
    graph = nx.DiGraph()

    for node in lineage_graph.nodes:
        graph.add_node(
            node.id,
            node_type=node.node_type,
            name=node.name,
            metadata=node.metadata,
        )

    for edge in lineage_graph.edges:
        if allowed_edge_types and edge.edge_type not in allowed_edge_types:
            continue
        graph.add_edge(
            edge.source,
            edge.target,
            id=edge.id,
            edge_type=edge.edge_type,
            metadata=edge.metadata,
        )

    return graph


def extract_lineage_subgraph(
    lineage_graph: LineageGraph,
    *,
    focus_node_ids: list[str] | None = None,
    project_id: str | None = None,
    dataset_id: str | None = None,
    consumer: str | None = None,
    asset_id: str | None = None,
    edge_types: list[str] | None = None,
    hop_depth: int = 1,
    max_nodes: int = 120,
) -> LineageSubgraph:
    """Build a filtered lineage subgraph around a focused neighborhood."""

    import networkx as nx

    if hop_depth < 0:
        raise ValueError("hop_depth must be >= 0")
    if max_nodes <= 0:
        raise ValueError("max_nodes must be > 0")

    graph = to_networkx_digraph(lineage_graph, edge_types=edge_types)
    seed_ids = _resolve_seed_nodes(
        graph,
        focus_node_ids=focus_node_ids or [],
        project_id=project_id,
        dataset_id=dataset_id,
        consumer=consumer,
        asset_id=asset_id,
    )
    if not seed_ids:
        return LineageSubgraph(
            graph=graph.__class__(),
            seed_node_ids=[],
            node_limit=max_nodes,
            truncated=False,
            omitted_nodes=0,
            filters={
                "project_id": project_id or "",
                "dataset_id": dataset_id or "",
                "consumer": consumer or "",
                "asset_id": asset_id or "",
                "edge_types": edge_types or [],
                "hop_depth": hop_depth,
            },
        )

    candidate_nodes = set(seed_ids)
    frontier = set(seed_ids)
    for _ in range(hop_depth):
        next_frontier: set[str] = set()
        for node_id in frontier:
            if node_id not in graph:
                continue
            next_frontier.update(graph.predecessors(node_id))
            next_frontier.update(graph.successors(node_id))
        next_frontier -= candidate_nodes
        candidate_nodes.update(next_frontier)
        frontier = next_frontier

    truncated = False
    omitted_nodes = 0
    if len(candidate_nodes) > max_nodes:
        ranked_nodes = sorted(
            candidate_nodes,
            key=lambda node_id: _subgraph_rank_key(graph, node_id, seed_ids),
        )
        kept_nodes = set(ranked_nodes[:max_nodes])
        omitted_nodes = len(candidate_nodes) - len(kept_nodes)
        candidate_nodes = kept_nodes
        truncated = True

    subgraph = nx.DiGraph(graph.subgraph(candidate_nodes).copy())
    return LineageSubgraph(
        graph=subgraph,
        seed_node_ids=sorted(seed_ids),
        node_limit=max_nodes,
        truncated=truncated,
        omitted_nodes=omitted_nodes,
        filters={
            "project_id": project_id or "",
            "dataset_id": dataset_id or "",
            "consumer": consumer or "",
            "asset_id": asset_id or "",
            "edge_types": edge_types or [],
            "hop_depth": hop_depth,
        },
    )


def _resolve_seed_nodes(
    graph: Any,
    *,
    focus_node_ids: list[str],
    project_id: str | None,
    dataset_id: str | None,
    consumer: str | None,
    asset_id: str | None,
) -> list[str]:
    seed_nodes: set[str] = {node_id for node_id in focus_node_ids if node_id in graph}

    if project_id:
        for node_id, data in graph.nodes(data=True):
            metadata = data.get("metadata", {})
            if metadata.get("project_id") == project_id:
                seed_nodes.add(node_id)

    if dataset_id:
        for node_id, data in graph.nodes(data=True):
            metadata = data.get("metadata", {})
            if node_id == dataset_id or metadata.get("dataset_id") == dataset_id.split(".")[-1]:
                if metadata.get("project_id") and "." in dataset_id:
                    project_part = dataset_id.split(".", 1)[0]
                    if metadata.get("project_id") != project_part and node_id != dataset_id:
                        continue
                seed_nodes.add(node_id)

    if consumer:
        consumer_node = consumer if consumer.startswith("consumer:") else f"consumer:{consumer.lower()}"
        if consumer_node in graph:
            seed_nodes.add(consumer_node)

    if asset_id and asset_id in graph:
        seed_nodes.add(asset_id)

    return sorted(seed_nodes)


def _subgraph_rank_key(graph: Any, node_id: str, seed_ids: list[str]) -> tuple[int, float, str]:
    metadata = graph.nodes[node_id].get("metadata", {})
    total_weight = 0.0
    for _, _, data in graph.in_edges(node_id, data=True):
        total_weight += _edge_weight(data)
    for _, _, data in graph.out_edges(node_id, data=True):
        total_weight += _edge_weight(data)

    return (
        0 if node_id in seed_ids else 1,
        -total_weight,
        node_id,
    )


def _edge_weight(edge_data: dict[str, Any]) -> float:
    metadata = edge_data.get("metadata", {})
    if not isinstance(metadata, dict):
        return 0.0
    query_count = _float_value(metadata.get("query_count")) or 0.0
    execution_count = _float_value(metadata.get("execution_count")) or 0.0
    return query_count + execution_count


def _float_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
