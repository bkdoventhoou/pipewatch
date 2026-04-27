"""Cadence analysis: detect irregular or missed metric emission intervals."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CadenceResult:
    pipeline: str
    metric_name: str
    expected_interval: float   # seconds
    actual_intervals: List[float]
    mean_interval: float
    max_gap: float
    missed_count: int          # gaps > 1.5 * expected_interval
    irregular: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "expected_interval": self.expected_interval,
            "mean_interval": round(self.mean_interval, 3),
            "max_gap": round(self.max_gap, 3),
            "missed_count": self.missed_count,
            "irregular": self.irregular,
        }


def _compute_intervals(metrics: List[PipelineMetric]) -> List[float]:
    sorted_m = sorted(metrics, key=lambda m: m.timestamp)
    return [
        sorted_m[i + 1].timestamp - sorted_m[i].timestamp
        for i in range(len(sorted_m) - 1)
    ]


def analyze_cadence(
    metrics: List[PipelineMetric],
    expected_interval: float,
    irregularity_ratio: float = 0.25,
) -> Optional[CadenceResult]:
    """Analyse emission cadence for a single metric series."""
    if len(metrics) < 2:
        return None
    if expected_interval <= 0:
        return None

    intervals = _compute_intervals(metrics)
    mean_iv = sum(intervals) / len(intervals)
    max_gap = max(intervals)
    threshold = expected_interval * 1.5
    missed = sum(1 for iv in intervals if iv > threshold)
    irregular = abs(mean_iv - expected_interval) / expected_interval > irregularity_ratio

    sample = metrics[0]
    return CadenceResult(
        pipeline=sample.pipeline,
        metric_name=sample.name,
        expected_interval=expected_interval,
        actual_intervals=intervals,
        mean_interval=mean_iv,
        max_gap=max_gap,
        missed_count=missed,
        irregular=irregular,
    )


def analyze_all_cadences(
    history: Dict[str, List[PipelineMetric]],
    expected_interval: float,
    irregularity_ratio: float = 0.25,
) -> List[CadenceResult]:
    """Run cadence analysis across all metric series in a collector history."""
    results = []
    for metrics in history.values():
        result = analyze_cadence(metrics, expected_interval, irregularity_ratio)
        if result is not None:
            results.append(result)
    return results
