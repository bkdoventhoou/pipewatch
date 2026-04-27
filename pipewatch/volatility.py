"""Volatility analysis: measures how erratically a metric's values fluctuate over time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class VolatilityResult:
    pipeline: str
    metric_name: str
    sample_count: int
    mean: float
    std_dev: float
    coefficient_of_variation: float  # std_dev / mean, expressed as a ratio
    is_volatile: bool
    threshold_cv: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "sample_count": self.sample_count,
            "mean": round(self.mean, 6),
            "std_dev": round(self.std_dev, 6),
            "coefficient_of_variation": round(self.coefficient_of_variation, 6),
            "is_volatile": self.is_volatile,
            "threshold_cv": self.threshold_cv,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def analyze_volatility(
    metrics: List[PipelineMetric],
    threshold_cv: float = 0.5,
    min_samples: int = 4,
) -> Optional[VolatilityResult]:
    """Compute volatility for a list of metrics sharing the same pipeline/name.

    Returns None if there are too few samples or the mean is zero.
    """
    if len(metrics) < min_samples:
        return None

    values = [m.value for m in metrics]
    mean = _mean(values)

    if mean == 0.0:
        return None

    std = _std(values, mean)
    cv = std / abs(mean)

    sample = metrics[0]
    return VolatilityResult(
        pipeline=sample.pipeline,
        metric_name=sample.name,
        sample_count=len(values),
        mean=mean,
        std_dev=std,
        coefficient_of_variation=cv,
        is_volatile=cv > threshold_cv,
        threshold_cv=threshold_cv,
    )


def analyze_all_volatility(
    collector: MetricCollector,
    threshold_cv: float = 0.5,
    min_samples: int = 4,
) -> Dict[str, VolatilityResult]:
    """Run volatility analysis across all (pipeline, metric) pairs in the collector."""
    results: Dict[str, VolatilityResult] = {}
    for key, history in collector.get_history().items():
        result = analyze_volatility(history, threshold_cv=threshold_cv, min_samples=min_samples)
        if result is not None:
            results[key] = result
    return results
