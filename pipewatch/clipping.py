"""Clipping: detect and report metric values that are clamped at min/max bounds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class ClippingResult:
    pipeline: str
    metric_name: str
    clipped_low: int
    clipped_high: int
    total: int
    low_bound: float
    high_bound: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "clipped_low": self.clipped_low,
            "clipped_high": self.clipped_high,
            "total": self.total,
            "low_bound": self.low_bound,
            "high_bound": self.high_bound,
            "clip_rate": round((self.clipped_low + self.clipped_high) / self.total, 4)
            if self.total > 0
            else 0.0,
        }


def detect_clipping(
    metrics: List[PipelineMetric],
    low_bound: float,
    high_bound: float,
) -> Optional[ClippingResult]:
    """Detect how many values in *metrics* are clamped at the given bounds."""
    if not metrics:
        return None
    if low_bound >= high_bound:
        raise ValueError("low_bound must be strictly less than high_bound")

    clipped_low = sum(1 for m in metrics if m.value <= low_bound)
    clipped_high = sum(1 for m in metrics if m.value >= high_bound)

    return ClippingResult(
        pipeline=metrics[0].pipeline,
        metric_name=metrics[0].name,
        clipped_low=clipped_low,
        clipped_high=clipped_high,
        total=len(metrics),
        low_bound=low_bound,
        high_bound=high_bound,
    )


def detect_all_clippings(
    collector: MetricCollector,
    low_bound: float,
    high_bound: float,
) -> Dict[str, ClippingResult]:
    """Run clipping detection for every (pipeline, metric) key in *collector*."""
    results: Dict[str, ClippingResult] = {}
    for key, history in collector.get_history().items():
        result = detect_clipping(history, low_bound, high_bound)
        if result is not None:
            results[key] = result
    return results
