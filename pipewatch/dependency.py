"""Pipeline dependency tracking and health propagation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from pipewatch.metrics import MetricStatus


@dataclass
class DependencyNode:
    pipeline: str
    depends_on: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "depends_on": self.depends_on}


@dataclass
class PropagatedStatus:
    pipeline: str
    own_status: MetricStatus
    propagated_status: MetricStatus
    blocking_pipelines: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "own_status": self.own_status.value,
            "propagated_status": self.propagated_status.value,
            "blocking_pipelines": self.blocking_pipelines,
        }


class DependencyGraph:
    def __init__(self) -> None:
        self._nodes: Dict[str, DependencyNode] = {}

    def add(self, pipeline: str, depends_on: Optional[List[str]] = None) -> None:
        self._nodes[pipeline] = DependencyNode(
            pipeline=pipeline, depends_on=depends_on or []
        )

    def get(self, pipeline: str) -> Optional[DependencyNode]:
        return self._nodes.get(pipeline)

    def all_pipelines(self) -> List[str]:
        return list(self._nodes.keys())

    def to_dict(self) -> dict:
        return {p: n.to_dict() for p, n in self._nodes.items()}


def propagate_status(
    graph: DependencyGraph,
    statuses: Dict[str, MetricStatus],
) -> Dict[str, PropagatedStatus]:
    """Compute effective status for each pipeline considering upstream health."""
    result: Dict[str, PropagatedStatus] = {}
    _status_rank = {MetricStatus.OK: 0, MetricStatus.WARNING: 1, MetricStatus.CRITICAL: 2}

    for pipeline in graph.all_pipelines():
        node = graph.get(pipeline)
        own = statuses.get(pipeline, MetricStatus.OK)
        effective = own
        blockers: List[str] = []

        for dep in (node.depends_on if node else []):
            dep_status = statuses.get(dep, MetricStatus.OK)
            if _status_rank[dep_status] > _status_rank[effective]:
                effective = dep_status
            if dep_status != MetricStatus.OK:
                blockers.append(dep)

        result[pipeline] = PropagatedStatus(
            pipeline=pipeline,
            own_status=own,
            propagated_status=effective,
            blocking_pipelines=blockers,
        )
    return result
