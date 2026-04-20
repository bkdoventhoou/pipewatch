"""Group metrics by arbitrary fields and compute per-group summaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class MetricGroup:
    key: str
    metrics: List[PipelineMetric] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.metrics)

    @property
    def ok_count(self) -> int:
        return sum(1 for m in self.metrics if m.status == MetricStatus.OK)

    @property
    def warning_count(self) -> int:
        return sum(1 for m in self.metrics if m.status == MetricStatus.WARNING)

    @property
    def critical_count(self) -> int:
        return sum(1 for m in self.metrics if m.status == MetricStatus.CRITICAL)

    @property
    def health(self) -> str:
        if self.critical_count > 0:
            return "critical"
        if self.warning_count > 0:
            return "warning"
        return "ok"

    @property
    def avg_value(self) -> Optional[float]:
        values = [m.value for m in self.metrics if m.value is not None]
        if not values:
            return None
        return sum(values) / len(values)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "count": self.count,
            "ok": self.ok_count,
            "warning": self.warning_count,
            "critical": self.critical_count,
            "health": self.health,
            "avg_value": self.avg_value,
        }


def group_by(metrics: List[PipelineMetric], field_name: str) -> Dict[str, MetricGroup]:
    """Group a list of metrics by a named attribute (e.g. 'pipeline', 'name')."""
    groups: Dict[str, MetricGroup] = {}
    for metric in metrics:
        key = str(getattr(metric, field_name, "unknown"))
        if key not in groups:
            groups[key] = MetricGroup(key=key)
        groups[key].metrics.append(metric)
    return groups


def group_by_pipeline(metrics: List[PipelineMetric]) -> Dict[str, MetricGroup]:
    return group_by(metrics, "pipeline")


def group_by_metric_name(metrics: List[PipelineMetric]) -> Dict[str, MetricGroup]:
    return group_by(metrics, "name")
