"""Pipeline topology: map metric flow paths and detect cycles."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class TopologyNode:
    pipeline: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "upstream": list(self.upstream),
            "downstream": list(self.downstream),
        }


class TopologyGraph:
    def __init__(self) -> None:
        self._nodes: Dict[str, TopologyNode] = {}

    def add_edge(self, upstream: str, downstream: str) -> None:
        if upstream not in self._nodes:
            self._nodes[upstream] = TopologyNode(pipeline=upstream)
        if downstream not in self._nodes:
            self._nodes[downstream] = TopologyNode(pipeline=downstream)
        if downstream not in self._nodes[upstream].downstream:
            self._nodes[upstream].downstream.append(downstream)
        if upstream not in self._nodes[downstream].upstream:
            self._nodes[downstream].upstream.append(upstream)

    def get(self, pipeline: str) -> Optional[TopologyNode]:
        return self._nodes.get(pipeline)

    def all_pipelines(self) -> List[str]:
        return list(self._nodes.keys())

    def has_cycle(self) -> bool:
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def _dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            for neighbour in self._nodes.get(node, TopologyNode(node)).downstream:
                if neighbour not in visited:
                    if _dfs(neighbour):
                        return True
                elif neighbour in rec_stack:
                    return True
            rec_stack.discard(node)
            return False

        for pipeline in self._nodes:
            if pipeline not in visited:
                if _dfs(pipeline):
                    return True
        return False

    def to_dict(self) -> dict:
        return {p: n.to_dict() for p, n in self._nodes.items()}


def build_topology_from_config(config: dict) -> TopologyGraph:
    """Build a TopologyGraph from a config dict with an 'edges' list."""
    graph = TopologyGraph()
    for edge in config.get("edges", []):
        graph.add_edge(edge["upstream"], edge["downstream"])
    return graph
