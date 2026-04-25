"""Saturation analysis: measures how close metric values are to their critical thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.collector import MetricCollector


@dataclass
class SaturationResult:
    pipeline: str
    metric_name: str
    current_value: float
    critical_threshold: float
    saturation_pct: float   # 0.0 – 100.0+; 100 means at/above threshold
    status: MetricStatus

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "critical_threshold": self.critical_threshold,
            "saturation_pct": round(self.saturation_pct, 2),
            "status": self.status.value,
        }


def compute_saturation(
    metric: PipelineMetric,
    critical_threshold: float,
) -> Optional[SaturationResult]:
    """Return saturation for a single metric against its critical threshold.

    Returns None when the threshold is zero (avoid division by zero).
    """
    if critical_threshold == 0.0:
        return None

    saturation_pct = (metric.value / critical_threshold) * 100.0

    if saturation_pct >= 100.0:
        status = MetricStatus.CRITICAL
    elif saturation_pct >= 75.0:
        status = MetricStatus.WARNING
    else:
        status = MetricStatus.OK

    return SaturationResult(
        pipeline=metric.pipeline,
        metric_name=metric.name,
        current_value=metric.value,
        critical_threshold=critical_threshold,
        saturation_pct=saturation_pct,
        status=status,
    )


def analyze_saturation(
    collector: MetricCollector,
    thresholds: Dict[str, float],
) -> List[SaturationResult]:
    """Analyze saturation for all pipelines/metrics that have a critical threshold.

    *thresholds* maps ``"pipeline.metric_name"`` to a critical threshold value.
    Only the most recent recorded value per pipeline+metric is evaluated.
    """
    results: List[SaturationResult] = []

    for key, threshold in thresholds.items():
        history = collector.get_history(key)
        if not history:
            continue
        latest: PipelineMetric = history[-1]
        result = compute_saturation(latest, threshold)
        if result is not None:
            results.append(result)

    return results
