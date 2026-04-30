"""Dispersion analysis: measures spread of metric values over a history window."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class DispersionResult:
    pipeline: str
    metric_name: str
    count: int
    mean: float
    variance: float
    std_dev: float
    range_: float          # max - min
    cv: Optional[float]    # coefficient of variation (std/mean); None when mean==0
    is_high: bool
    cv_threshold: float

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "count": self.count,
            "mean": round(self.mean, 6),
            "variance": round(self.variance, 6),
            "std_dev": round(self.std_dev, 6),
            "range": round(self.range_, 6),
            "cv": round(self.cv, 6) if self.cv is not None else None,
            "is_high": self.is_high,
            "cv_threshold": self.cv_threshold,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _variance(values: List[float], mean: float) -> float:
    return sum((v - mean) ** 2 for v in values) / len(values)


def analyze_dispersion(
    metrics: List[PipelineMetric],
    cv_threshold: float = 0.5,
) -> Optional[DispersionResult]:
    """Analyse value dispersion for a single (pipeline, metric_name) series."""
    if len(metrics) < 2:
        return None

    values = [m.value for m in metrics]
    mean = _mean(values)
    var = _variance(values, mean)
    std = var ** 0.5
    rng = max(values) - min(values)
    cv: Optional[float] = (std / mean) if mean != 0 else None
    is_high = cv is not None and cv > cv_threshold

    sample = metrics[0]
    return DispersionResult(
        pipeline=sample.pipeline,
        metric_name=sample.name,
        count=len(values),
        mean=mean,
        variance=var,
        std_dev=std,
        range_=rng,
        cv=cv,
        is_high=is_high,
        cv_threshold=cv_threshold,
    )


def analyze_all_dispersions(
    history: Dict[str, List[PipelineMetric]],
    cv_threshold: float = 0.5,
) -> List[DispersionResult]:
    """Run dispersion analysis over every key in a collector history dict."""
    results: List[DispersionResult] = []
    for metrics in history.values():
        result = analyze_dispersion(metrics, cv_threshold=cv_threshold)
        if result is not None:
            results.append(result)
    return results
