"""Metric drift detection: compare recent average to a historical baseline window."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class DriftResult:
    pipeline: str
    metric_name: str
    baseline_avg: float
    recent_avg: float
    drift_pct: float          # signed percentage change relative to baseline
    drifted: bool
    threshold_pct: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "baseline_avg": round(self.baseline_avg, 4),
            "recent_avg": round(self.recent_avg, 4),
            "drift_pct": round(self.drift_pct, 4),
            "drifted": self.drifted,
            "threshold_pct": self.threshold_pct,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def detect_drift(
    metrics: List[PipelineMetric],
    baseline_n: int = 20,
    recent_n: int = 5,
    threshold_pct: float = 20.0,
) -> Optional[DriftResult]:
    """Detect drift for a homogeneous list of metrics (same pipeline + name).

    Returns None if there are not enough data points to compare.
    """
    if len(metrics) < baseline_n + recent_n:
        return None

    values = [m.value for m in metrics]
    baseline_values = values[-(baseline_n + recent_n): -recent_n]
    recent_values = values[-recent_n:]

    baseline_avg = _mean(baseline_values)
    recent_avg = _mean(recent_values)

    if baseline_avg == 0.0:
        drift_pct = 0.0 if recent_avg == 0.0 else float("inf")
    else:
        drift_pct = ((recent_avg - baseline_avg) / abs(baseline_avg)) * 100.0

    sample = metrics[0]
    return DriftResult(
        pipeline=sample.pipeline,
        metric_name=sample.name,
        baseline_avg=baseline_avg,
        recent_avg=recent_avg,
        drift_pct=drift_pct,
        drifted=abs(drift_pct) >= threshold_pct,
        threshold_pct=threshold_pct,
    )


def detect_all_drifts(
    history: dict,
    baseline_n: int = 20,
    recent_n: int = 5,
    threshold_pct: float = 20.0,
) -> List[DriftResult]:
    """Run drift detection over every (pipeline, metric_name) key in a collector history."""
    results: List[DriftResult] = []
    for metrics in history.values():
        result = detect_drift(
            metrics,
            baseline_n=baseline_n,
            recent_n=recent_n,
            threshold_pct=threshold_pct,
        )
        if result is not None:
            results.append(result)
    return results
