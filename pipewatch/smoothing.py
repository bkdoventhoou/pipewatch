"""Exponential moving average smoothing for pipeline metric series."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SmoothedSeries:
    pipeline: str
    metric_name: str
    alpha: float
    smoothed_values: List[float] = field(default_factory=list)
    raw_values: List[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "alpha": self.alpha,
            "raw_values": self.raw_values,
            "smoothed_values": self.smoothed_values,
            "latest_smoothed": self.smoothed_values[-1] if self.smoothed_values else None,
        }


def smooth_series(
    metrics: List[PipelineMetric],
    alpha: float = 0.3,
) -> Optional[SmoothedSeries]:
    """Apply exponential moving average to a list of metrics.

    Args:
        metrics: Ordered list of PipelineMetric (oldest first).
        alpha:   Smoothing factor in (0, 1]. Higher = less smoothing.

    Returns:
        SmoothedSeries or None when the input is empty.
    """
    if not metrics:
        return None
    if not (0 < alpha <= 1.0):
        raise ValueError(f"alpha must be in (0, 1], got {alpha}")

    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    raw = [m.value for m in metrics]
    smoothed: List[float] = []
    ema = raw[0]
    smoothed.append(ema)
    for v in raw[1:]:
        ema = alpha * v + (1.0 - alpha) * ema
        smoothed.append(ema)

    return SmoothedSeries(
        pipeline=pipeline,
        metric_name=metric_name,
        alpha=alpha,
        smoothed_values=smoothed,
        raw_values=raw,
    )


def smooth_all(
    history: dict,
    alpha: float = 0.3,
) -> List[SmoothedSeries]:
    """Smooth every (pipeline, metric_name) series found in a collector history.

    Args:
        history: Mapping of metric key -> list of PipelineMetric.
        alpha:   EMA smoothing factor.

    Returns:
        List of SmoothedSeries, one per non-empty series.
    """
    results = []
    for metrics in history.values():
        if not metrics:
            continue
        result = smooth_series(metrics, alpha=alpha)
        if result is not None:
            results.append(result)
    return results
