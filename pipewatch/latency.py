"""Latency tracking for pipeline metrics — measures time between consecutive records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class LatencyResult:
    pipeline: str
    metric_name: str
    min_gap: float
    max_gap: float
    avg_gap: float
    sample_count: int  # number of gaps computed (len(metrics) - 1)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "min_gap_seconds": round(self.min_gap, 4),
            "max_gap_seconds": round(self.max_gap, 4),
            "avg_gap_seconds": round(self.avg_gap, 4),
            "sample_count": self.sample_count,
        }


def _compute_gaps(metrics: List[PipelineMetric]) -> List[float]:
    """Return list of time gaps (seconds) between consecutive metric timestamps."""
    sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)
    return [
        (sorted_metrics[i + 1].timestamp - sorted_metrics[i].timestamp)
        for i in range(len(sorted_metrics) - 1)
    ]


def analyze_latency(
    pipeline: str,
    metric_name: str,
    metrics: List[PipelineMetric],
) -> Optional[LatencyResult]:
    """Compute latency statistics for a single pipeline/metric series."""
    relevant = [
        m for m in metrics
        if m.pipeline == pipeline and m.name == metric_name
    ]
    gaps = _compute_gaps(relevant)
    if not gaps:
        return None
    return LatencyResult(
        pipeline=pipeline,
        metric_name=metric_name,
        min_gap=min(gaps),
        max_gap=max(gaps),
        avg_gap=sum(gaps) / len(gaps),
        sample_count=len(gaps),
    )


def analyze_all_latencies(
    history: Dict[str, List[PipelineMetric]],
) -> List[LatencyResult]:
    """Analyze latency for every (pipeline, metric_name) pair in the collector history."""
    results: List[LatencyResult] = []
    seen: Dict[tuple, List[PipelineMetric]] = {}

    for metrics in history.values():
        for m in metrics:
            key = (m.pipeline, m.name)
            seen.setdefault(key, []).append(m)

    for (pipeline, metric_name), metrics in seen.items():
        result = analyze_latency(pipeline, metric_name, metrics)
        if result is not None:
            results.append(result)

    return results
