"""Degradation detection: identifies pipelines whose health is steadily worsening."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


_STATUS_SCORE: Dict[MetricStatus, int] = {
    MetricStatus.OK: 0,
    MetricStatus.WARNING: 1,
    MetricStatus.CRITICAL: 2,
}


@dataclass
class DegradationResult:
    pipeline: str
    metric_name: str
    score_slope: float          # positive = worsening, negative = improving
    degrading: bool
    sample_count: int
    latest_status: MetricStatus

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "score_slope": round(self.score_slope, 6),
            "degrading": self.degrading,
            "sample_count": self.sample_count,
            "latest_status": self.latest_status.value,
        }


def _slope(values: List[float]) -> float:
    """Least-squares slope for an evenly-spaced series."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    den = sum((x - mean_x) ** 2 for x in xs)
    return num / den if den != 0.0 else 0.0


def detect_degradation(
    metrics: List[PipelineMetric],
    min_samples: int = 4,
    slope_threshold: float = 0.1,
) -> Optional[DegradationResult]:
    """Return a DegradationResult if the series shows a worsening trend."""
    if len(metrics) < min_samples:
        return None
    scores = [float(_STATUS_SCORE[m.status]) for m in metrics]
    s = _slope(scores)
    latest = metrics[-1].status
    return DegradationResult(
        pipeline=metrics[-1].pipeline,
        metric_name=metrics[-1].name,
        score_slope=s,
        degrading=s >= slope_threshold,
        sample_count=len(metrics),
        latest_status=latest,
    )


def detect_all_degradations(
    history: Dict[str, List[PipelineMetric]],
    min_samples: int = 4,
    slope_threshold: float = 0.1,
) -> List[DegradationResult]:
    results = []
    for series in history.values():
        r = detect_degradation(series, min_samples, slope_threshold)
        if r is not None:
            results.append(r)
    return results
