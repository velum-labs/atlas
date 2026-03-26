"""Reusable graph transform contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

InputGraphT = TypeVar("InputGraphT")
OutputGraphT = TypeVar("OutputGraphT")
GraphT = TypeVar("GraphT")


class GraphTransform(Protocol[InputGraphT, OutputGraphT]):
    """A reusable graph-to-graph transform."""

    @property
    def name(self) -> str: ...

    def apply(self, graph: InputGraphT) -> OutputGraphT: ...


@dataclass
class TransformPipeline(Generic[GraphT]):
    """Apply a sequence of graph transforms in order."""

    transforms: list[GraphTransform[GraphT, GraphT]]

    def apply(self, graph: GraphT) -> GraphT:
        current = graph
        for transform in self.transforms:
            current = transform.apply(current)
        return current

    @property
    def applied_names(self) -> list[str]:
        return [transform.name for transform in self.transforms]
