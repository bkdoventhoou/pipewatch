from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TaggedMetric:
    metric: PipelineMetric
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = self.metric.to_dict()
        d["tags"] = self.tags
        return d

    def has_tag(self, key: str, value: Optional[str] = None) -> bool:
        if key not in self.tags:
            return False
        if value is not None:
            return self.tags[key] == value
        return True


class TagRegistry:
    def __init__(self):
        self._store: Dict[str, Dict[str, str]] = {}

    def tag(self, pipeline: str, name: str, tags: Dict[str, str]) -> None:
        key = f"{pipeline}::{name}"
        self._store.setdefault(key, {}).update(tags)

    def get_tags(self, pipeline: str, name: str) -> Dict[str, str]:
        return dict(self._store.get(f"{pipeline}::{name}", {}))

    def apply(self, metrics: List[PipelineMetric]) -> List[TaggedMetric]:
        result = []
        for m in metrics:
            tags = self.get_tags(m.pipeline, m.name)
            result.append(TaggedMetric(metric=m, tags=tags))
        return result

    def filter_by_tag(self, tagged: List[TaggedMetric], key: str, value: Optional[str] = None) -> List[TaggedMetric]:
        return [t for t in tagged if t.has_tag(key, value)]
