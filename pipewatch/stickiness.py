"""Stickiness analysis: measures how long a pipeline stays in a non-OK status."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class StickinessResult:
    pipeline: str
    metric_name: str
    status: MetricStatus
    streak: int          # consecutive non-OK readings
    duration_seconds: float  # wall-clock span of the streak
    is_stuck: bool       # True when streak >= threshold

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "status": self.status.value,
            "streak": self.streak,
            "duration_seconds": round(self.duration_seconds, 3),
            "is_stuck": self.is_stuck,
        }


def analyze_stickiness(
    metrics: List[PipelineMetric],
    streak_threshold: int = 3,
) -> Optional[StickinessResult]:
    """Analyse a single metric series for status stickiness.

    Returns a StickinessResult if the *latest* reading is non-OK, else None.
    """
    if not metrics:
        return None

    pipeline = metrics[-1].pipeline
    name = metrics[-1].name
    current_status = metrics[-1].status

    if current_status == MetricStatus.OK:
        return None

    streak = 0
    for m in reversed(metrics):
        if m.status == current_status:
            streak += 1
        else:
            break

    first_ts = metrics[-streak].timestamp if streak > 0 else metrics[-1].timestamp
    last_ts = metrics[-1].timestamp
    duration = last_ts - first_ts

    return StickinessResult(
        pipeline=pipeline,
        metric_name=name,
        status=current_status,
        streak=streak,
        duration_seconds=duration,
        is_stuck=streak >= streak_threshold,
    )


def analyze_all_stickiness(
    history: Dict[str, List[PipelineMetric]],
    streak_threshold: int = 3,
) -> List[StickinessResult]:
    """Run stickiness analysis across all (pipeline, metric) series."""
    results: List[StickinessResult] = []
    for series in history.values():
        result = analyze_stickiness(series, streak_threshold=streak_threshold)
        if result is not None:
            results.append(result)
    return results
