"""Momentum analysis: measures rate-of-change acceleration across metric history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MomentumResult:
    pipeline: str
    metric_name: str
    first_derivative: float   # velocity (slope of values)
    second_derivative: float  # acceleration (slope of velocities)
    accelerating: bool        # True if |second_derivative| > threshold
    samples: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "first_derivative": round(self.first_derivative, 6),
            "second_derivative": round(self.second_derivative, 6),
            "accelerating": self.accelerating,
            "samples": self.samples,
        }


def _slope(xs: List[float], ys: List[float]) -> float:
    """Least-squares slope."""
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    return num / den if den != 0.0 else 0.0


def analyze_momentum(
    metrics: List[PipelineMetric],
    accel_threshold: float = 0.01,
) -> Optional[MomentumResult]:
    """Compute first and second derivative for a single metric series."""
    if len(metrics) < 3:
        return None

    sorted_m = sorted(metrics, key=lambda m: m.timestamp)
    ts = [m.timestamp for m in sorted_m]
    vs = [m.value for m in sorted_m]

    # first derivative: velocities between consecutive points
    velocities = [
        (vs[i] - vs[i - 1]) / max(ts[i] - ts[i - 1], 1e-9)
        for i in range(1, len(vs))
    ]
    vel_ts = [ts[i] for i in range(1, len(ts))]

    first_d = _slope(ts, vs)
    second_d = _slope(vel_ts, velocities) if len(velocities) >= 2 else 0.0
    accelerating = abs(second_d) > accel_threshold

    return MomentumResult(
        pipeline=sorted_m[0].pipeline,
        metric_name=sorted_m[0].name,
        first_derivative=first_d,
        second_derivative=second_d,
        accelerating=accelerating,
        samples=len(sorted_m),
    )


def analyze_all_momentum(
    history: Dict[str, List[PipelineMetric]],
    accel_threshold: float = 0.01,
) -> List[MomentumResult]:
    results = []
    for metrics in history.values():
        r = analyze_momentum(metrics, accel_threshold=accel_threshold)
        if r is not None:
            results.append(r)
    return results
