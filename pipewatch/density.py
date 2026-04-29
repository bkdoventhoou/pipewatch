"""Metric density analysis: measures how frequently metrics are recorded over time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class DensityResult:
    pipeline: str
    metric_name: str
    total_points: int
    window_seconds: float
    points_per_minute: float
    is_sparse: bool
    sparse_threshold: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "total_points": self.total_points,
            "window_seconds": self.window_seconds,
            "points_per_minute": round(self.points_per_minute, 4),
            "is_sparse": self.is_sparse,
            "sparse_threshold": self.sparse_threshold,
        }


def _compute_window(metrics: List[PipelineMetric]) -> float:
    """Return elapsed seconds between first and last metric."""
    if len(metrics) < 2:
        return 0.0
    return metrics[-1].timestamp - metrics[0].timestamp


def analyze_density(
    metrics: List[PipelineMetric],
    sparse_threshold: float = 1.0,
) -> Optional[DensityResult]:
    """Compute density for a single (pipeline, metric_name) series.

    sparse_threshold: minimum points-per-minute considered healthy.
    Returns None if fewer than 2 data points.
    """
    if len(metrics) < 2:
        return None

    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    window_seconds = _compute_window(metrics)

    if window_seconds <= 0.0:
        return None

    points_per_minute = len(metrics) / (window_seconds / 60.0)
    is_sparse = points_per_minute < sparse_threshold

    return DensityResult(
        pipeline=pipeline,
        metric_name=metric_name,
        total_points=len(metrics),
        window_seconds=window_seconds,
        points_per_minute=points_per_minute,
        is_sparse=is_sparse,
        sparse_threshold=sparse_threshold,
    )


def analyze_all_densities(
    history: Dict[str, List[PipelineMetric]],
    sparse_threshold: float = 1.0,
) -> List[DensityResult]:
    """Analyze density for every series in a collector history dict."""
    results: List[DensityResult] = []
    for series in history.values():
        result = analyze_density(series, sparse_threshold=sparse_threshold)
        if result is not None:
            results.append(result)
    return results
