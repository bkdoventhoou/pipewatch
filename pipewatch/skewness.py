"""Skewness analysis for pipeline metric distributions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SkewnessResult:
    pipeline: str
    metric_name: str
    sample_count: int
    mean: float
    skewness: float
    interpretation: str  # "left", "right", "symmetric"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "sample_count": self.sample_count,
            "mean": round(self.mean, 4),
            "skewness": round(self.skewness, 4),
            "interpretation": self.interpretation,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float], mu: float) -> float:
    variance = sum((v - mu) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _skewness(values: List[float]) -> Optional[float]:
    """Compute Pearson's moment coefficient of skewness."""
    n = len(values)
    if n < 3:
        return None
    mu = _mean(values)
    sigma = _std(values, mu)
    if sigma == 0.0:
        return None
    return sum((v - mu) ** 3 for v in values) / (n * sigma ** 3)


def _interpret(skew: float, threshold: float = 0.5) -> str:
    if skew > threshold:
        return "right"
    if skew < -threshold:
        return "left"
    return "symmetric"


def analyze_skewness(
    metrics: List[PipelineMetric],
    threshold: float = 0.5,
) -> Optional[SkewnessResult]:
    """Analyze skewness for a flat list of metrics sharing the same pipeline/name."""
    if not metrics:
        return None
    values = [m.value for m in metrics]
    skew = _skewness(values)
    if skew is None:
        return None
    mu = _mean(values)
    return SkewnessResult(
        pipeline=metrics[0].pipeline,
        metric_name=metrics[0].name,
        sample_count=len(values),
        mean=mu,
        skewness=skew,
        interpretation=_interpret(skew, threshold),
    )


def analyze_all_skewness(
    history: Dict[str, List[PipelineMetric]],
    threshold: float = 0.5,
) -> List[SkewnessResult]:
    """Run skewness analysis across all metric series in a collector history."""
    results: List[SkewnessResult] = []
    for metrics in history.values():
        result = analyze_skewness(metrics, threshold=threshold)
        if result is not None:
            results.append(result)
    return results
