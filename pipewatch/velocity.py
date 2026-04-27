"""Velocity analysis: measures the rate of change of metric values over time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class VelocityResult:
    pipeline: str
    metric_name: str
    velocity: float          # units per second
    direction: str           # "rising", "falling", "stable"
    sample_count: int
    span_seconds: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "velocity": round(self.velocity, 6),
            "direction": self.direction,
            "sample_count": self.sample_count,
            "span_seconds": round(self.span_seconds, 3),
        }


_STABLE_THRESHOLD = 1e-9


def _direction(velocity: float) -> str:
    if velocity > _STABLE_THRESHOLD:
        return "rising"
    if velocity < -_STABLE_THRESHOLD:
        return "falling"
    return "stable"


def compute_velocity(
    metrics: List[PipelineMetric],
) -> Optional[VelocityResult]:
    """Compute average rate-of-change (value/second) for a metric series."""
    if len(metrics) < 2:
        return None

    ordered = sorted(metrics, key=lambda m: m.timestamp)
    span = (ordered[-1].timestamp - ordered[0].timestamp).total_seconds()
    if span <= 0:
        return None

    delta_value = ordered[-1].value - ordered[0].value
    velocity = delta_value / span

    return VelocityResult(
        pipeline=ordered[0].pipeline,
        metric_name=ordered[0].name,
        velocity=velocity,
        direction=_direction(velocity),
        sample_count=len(ordered),
        span_seconds=span,
    )


def compute_all_velocities(
    history: Dict[str, List[PipelineMetric]],
) -> List[VelocityResult]:
    """Compute velocity for every (pipeline, metric) key in *history*."""
    results: List[VelocityResult] = []
    for metrics in history.values():
        result = compute_velocity(metrics)
        if result is not None:
            results.append(result)
    return results
