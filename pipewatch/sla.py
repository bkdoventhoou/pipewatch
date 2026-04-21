"""SLA (Service Level Agreement) tracking for pipeline metrics.

Tracks whether pipelines are meeting defined SLA targets based on
the ratio of OK vs non-OK metric evaluations over a rolling window.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import MetricStatus, PipelineMetric


@dataclass
class SLATarget:
    """Defines an SLA target for a pipeline."""

    pipeline: str
    target_pct: float          # e.g. 99.0 means 99% of evaluations must be OK
    window_seconds: float = 3600.0  # rolling window to evaluate over

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "target_pct": self.target_pct,
            "window_seconds": self.window_seconds,
        }


@dataclass
class SLAResult:
    """Result of evaluating a pipeline against its SLA target."""

    pipeline: str
    target_pct: float
    actual_pct: float
    total_evaluations: int
    ok_evaluations: int
    breached: bool
    evaluated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "target_pct": self.target_pct,
            "actual_pct": round(self.actual_pct, 4),
            "total_evaluations": self.total_evaluations,
            "ok_evaluations": self.ok_evaluations,
            "breached": self.breached,
            "evaluated_at": self.evaluated_at,
        }


def evaluate_sla(
    target: SLATarget,
    metrics: List[PipelineMetric],
    now: Optional[float] = None,
) -> Optional[SLAResult]:
    """Evaluate a pipeline's SLA compliance against a list of metrics.

    Only metrics belonging to the target pipeline and within the rolling
    window are considered. Returns None if there are no qualifying metrics.
    """
    if now is None:
        now = time.time()

    cutoff = now - target.window_seconds
    window_metrics = [
        m for m in metrics
        if m.pipeline == target.pipeline and m.timestamp >= cutoff
    ]

    if not window_metrics:
        return None

    ok_count = sum(1 for m in window_metrics if m.status == MetricStatus.OK)
    total = len(window_metrics)
    actual_pct = (ok_count / total) * 100.0

    return SLAResult(
        pipeline=target.pipeline,
        target_pct=target.target_pct,
        actual_pct=actual_pct,
        total_evaluations=total,
        ok_evaluations=ok_count,
        breached=actual_pct < target.target_pct,
        evaluated_at=now,
    )


def evaluate_all_slas(
    targets: List[SLATarget],
    metrics: List[PipelineMetric],
    now: Optional[float] = None,
) -> Dict[str, Optional[SLAResult]]:
    """Evaluate all SLA targets and return a mapping of pipeline -> SLAResult.

    Pipelines with no qualifying metrics within the window will map to None.
    """
    if now is None:
        now = time.time()

    return {
        target.pipeline: evaluate_sla(target, metrics, now=now)
        for target in targets
    }
