"""Summary statistics helpers built on top of aggregated metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.aggregator import AggregatedMetric


@dataclass
class PipelineSummary:
    pipeline: str
    total_metrics: int
    total_ok: int
    total_warning: int
    total_critical: int
    overall_mean: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_metrics": self.total_metrics,
            "ok": self.total_ok,
            "warning": self.total_warning,
            "critical": self.total_critical,
            "overall_mean": self.overall_mean,
        }

    @property
    def health(self) -> str:
        if self.total_critical > 0:
            return "critical"
        if self.total_warning > 0:
            return "warning"
        return "ok"


def summarize_by_pipeline(aggregated: List[AggregatedMetric]) -> List[PipelineSummary]:
    """Roll up aggregated metrics into per-pipeline summaries."""
    groups: dict[str, List[AggregatedMetric]] = {}
    for a in aggregated:
        groups.setdefault(a.pipeline, []).append(a)

    summaries = []
    for pipeline, items in groups.items():
        total_ok = sum(a.statuses.get("ok", 0) for a in items)
        total_warning = sum(a.statuses.get("warning", 0) for a in items)
        total_critical = sum(a.statuses.get("critical", 0) for a in items)
        total_metrics = sum(a.count for a in items)
        means = [a.mean_value for a in items if a.count > 0]
        overall_mean = round(sum(means) / len(means), 4) if means else 0.0
        summaries.append(PipelineSummary(
            pipeline=pipeline,
            total_metrics=total_metrics,
            total_ok=total_ok,
            total_warning=total_warning,
            total_critical=total_critical,
            overall_mean=overall_mean,
        ))
    return summaries
