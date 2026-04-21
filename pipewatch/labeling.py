"""Metric labeling — attach and query free-form key/value labels on metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class LabeledMetric:
    metric: PipelineMetric
    labels: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.metric.pipeline,
            "name": self.metric.name,
            "value": self.metric.value,
            "status": self.metric.status.value,
            "labels": self.labels,
        }

    def get(self, key: str) -> Optional[str]:
        """Return the label value for *key*, or None."""
        return self.labels.get(key)

    def has_label(self, key: str, value: Optional[str] = None) -> bool:
        """Return True if the label *key* exists (and optionally matches *value*)."""
        if key not in self.labels:
            return False
        if value is not None:
            return self.labels[key] == value
        return True


class LabelRegistry:
    """Attach labels to metrics and look them up by label."""

    def __init__(self) -> None:
        self._store: List[LabeledMetric] = []

    def label(self, metric: PipelineMetric, **labels: str) -> LabeledMetric:
        """Wrap *metric* with the supplied labels and register it."""
        lm = LabeledMetric(metric=metric, labels=dict(labels))
        self._store.append(lm)
        return lm

    def query(self, key: str, value: Optional[str] = None) -> List[LabeledMetric]:
        """Return all labeled metrics that carry the given label key/value."""
        return [lm for lm in self._store if lm.has_label(key, value)]

    def all(self) -> List[LabeledMetric]:
        return list(self._store)

    def clear(self) -> None:
        self._store.clear()
