"""Capacity planning module: tracks metric usage against defined limits."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CapacityEntry:
    pipeline: str
    metric_name: str
    current: float
    limit: float
    utilization: float  # 0.0 – 1.0
    breached: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "current": self.current,
            "limit": self.limit,
            "utilization": round(self.utilization, 4),
            "breached": self.breached,
        }


@dataclass
class CapacityReport:
    entries: List[CapacityEntry] = field(default_factory=list)

    def breached_entries(self) -> List[CapacityEntry]:
        return [e for e in self.entries if e.breached]

    def to_dict(self) -> dict:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "total": len(self.entries),
            "breached": len(self.breached_entries()),
        }


def evaluate_capacity(
    metrics: List[PipelineMetric],
    limits: Dict[str, float],
    breach_threshold: float = 1.0,
) -> CapacityReport:
    """Evaluate each metric value against its configured limit.

    Args:
        metrics: List of PipelineMetric instances to evaluate.
        limits: Mapping of metric_name -> numeric limit.
        breach_threshold: Fraction of limit at which breach is flagged (default 1.0).

    Returns:
        CapacityReport containing one CapacityEntry per matched metric.
    """
    report = CapacityReport()
    for m in metrics:
        if m.name not in limits:
            continue
        limit = limits[m.name]
        if limit <= 0:
            continue
        utilization = m.value / limit
        entry = CapacityEntry(
            pipeline=m.pipeline,
            metric_name=m.name,
            current=m.value,
            limit=limit,
            utilization=utilization,
            breached=utilization >= breach_threshold,
        )
        report.entries.append(entry)
    return report
