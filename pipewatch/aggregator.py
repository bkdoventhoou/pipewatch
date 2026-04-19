"""Aggregation utilities for pipeline metrics over time windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean, median
from typing import List, Dict, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class AggregatedMetric:
    pipeline: str
    metric_name: str
    count: int
    min_value: float
    max_value: float
    mean_value: float
    median_value: float
    statuses: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "count": self.count,
            "min": self.min_value,
            "max": self.max_value,
            "mean": self.mean_value,
            "median": self.median_value,
            "statuses": self.statuses,
        }


def aggregate_metrics(metrics: List[PipelineMetric]) -> List[AggregatedMetric]:
    """Group and aggregate a list of PipelineMetric by (pipeline, metric_name)."""
    groups: Dict[tuple, List[PipelineMetric]] = {}
    for m in metrics:
        key = (m.pipeline, m.name)
        groups.setdefault(key, []).append(m)

    results = []
    for (pipeline, name), group in groups.items():
        values = [m.value for m in group]
        status_counts: Dict[str, int] = {}
        for m in group:
            label = m.status.value if m.status else MetricStatus.OK.value
            status_counts[label] = status_counts.get(label, 0) + 1
        results.append(AggregatedMetric(
            pipeline=pipeline,
            metric_name=name,
            count=len(values),
            min_value=min(values),
            max_value=max(values),
            mean_value=round(mean(values), 4),
            median_value=round(median(values), 4),
            statuses=status_counts,
        ))
    return results
