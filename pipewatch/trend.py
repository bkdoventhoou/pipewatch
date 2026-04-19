"""Trend analysis for pipeline metrics over time."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TrendResult:
    pipeline: str
    metric_name: str
    values: List[float]
    direction: str  # 'up', 'down', 'stable'
    slope: float
    avg: float
    min: float
    max: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "direction": self.direction,
            "slope": round(self.slope, 4),
            "avg": round(self.avg, 4),
            "min": self.min,
            "max": self.max,
            "sample_count": len(self.values),
        }


def _compute_slope(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


def analyze_trend(metrics: List[PipelineMetric], threshold: float = 0.01) -> Optional[TrendResult]:
    if not metrics:
        return None
    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    values = [m.value for m in metrics]
    slope = _compute_slope(values)
    if slope > threshold:
        direction = "up"
    elif slope < -threshold:
        direction = "down"
    else:
        direction = "stable"
    return TrendResult(
        pipeline=pipeline,
        metric_name=metric_name,
        values=values,
        direction=direction,
        slope=slope,
        avg=sum(values) / len(values),
        min=min(values),
        max=max(values),
    )


def analyze_all_trends(history: dict, threshold: float = 0.01) -> List[TrendResult]:
    """history: {(pipeline, metric_name): [PipelineMetric, ...]}"""
    results = []
    for metrics in history.values():
        result = analyze_trend(metrics, threshold)
        if result:
            results.append(result)
    return results
