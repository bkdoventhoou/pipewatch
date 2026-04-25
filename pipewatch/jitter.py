"""Jitter detection: flags metrics whose values fluctuate erratically
between consecutive observations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class JitterResult:
    pipeline: str
    metric_name: str
    flip_count: int          # number of direction reversals
    flip_rate: float         # flips per observation
    max_swing: float         # largest single-step absolute change
    jittery: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "flip_count": self.flip_count,
            "flip_rate": round(self.flip_rate, 4),
            "max_swing": round(self.max_swing, 4),
            "jittery": self.jittery,
        }


def _count_flips(values: List[float]) -> int:
    """Count direction reversals in a sequence of values."""
    if len(values) < 3:
        return 0
    deltas = [values[i + 1] - values[i] for i in range(len(values) - 1)]
    flips = 0
    for i in range(len(deltas) - 1):
        if deltas[i] != 0 and deltas[i + 1] != 0:
            if (deltas[i] > 0) != (deltas[i + 1] > 0):
                flips += 1
    return flips


def detect_jitter(
    metrics: List[PipelineMetric],
    flip_rate_threshold: float = 0.5,
    min_points: int = 4,
) -> Optional[JitterResult]:
    """Analyse a single metric series for jitter.

    Returns None if there are too few points to evaluate.
    """
    if len(metrics) < min_points:
        return None

    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    values = [m.value for m in metrics]

    flip_count = _count_flips(values)
    flip_rate = flip_count / (len(values) - 1) if len(values) > 1 else 0.0
    swings = [abs(values[i + 1] - values[i]) for i in range(len(values) - 1)]
    max_swing = max(swings) if swings else 0.0
    jittery = flip_rate >= flip_rate_threshold

    return JitterResult(
        pipeline=pipeline,
        metric_name=metric_name,
        flip_count=flip_count,
        flip_rate=flip_rate,
        max_swing=max_swing,
        jittery=jittery,
    )


def detect_all_jitter(
    history: Dict[str, List[PipelineMetric]],
    flip_rate_threshold: float = 0.5,
    min_points: int = 4,
) -> List[JitterResult]:
    """Run jitter detection across all metric series in a history dict."""
    results: List[JitterResult] = []
    for series in history.values():
        result = detect_jitter(series, flip_rate_threshold, min_points)
        if result is not None:
            results.append(result)
    return results
