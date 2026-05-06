"""Symmetry analysis: measures how symmetric a metric's value distribution is
around its mean using a simple above/below ratio."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class SymmetryResult:
    pipeline: str
    metric_name: str
    mean: float
    above_count: int
    below_count: int
    ratio: float          # above / (above + below); 0.5 = perfectly symmetric
    is_symmetric: bool    # True when ratio is within tolerance of 0.5
    sample_count: int

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "mean": round(self.mean, 4),
            "above_count": self.above_count,
            "below_count": self.below_count,
            "ratio": round(self.ratio, 4),
            "is_symmetric": self.is_symmetric,
            "sample_count": self.sample_count,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def analyze_symmetry(
    metrics: List[PipelineMetric],
    tolerance: float = 0.15,
) -> Optional[SymmetryResult]:
    """Analyse symmetry for a single series of metrics.

    Returns None when fewer than 3 data points are available or when all
    values are identical (above + below == 0).
    """
    if len(metrics) < 3:
        return None

    values = [m.value for m in metrics]
    mu = _mean(values)

    above = sum(1 for v in values if v > mu)
    below = sum(1 for v in values if v < mu)
    total = above + below

    if total == 0:
        return None

    ratio = above / total
    is_symmetric = abs(ratio - 0.5) <= tolerance

    first = metrics[0]
    return SymmetryResult(
        pipeline=first.pipeline,
        metric_name=first.name,
        mean=mu,
        above_count=above,
        below_count=below,
        ratio=ratio,
        is_symmetric=is_symmetric,
        sample_count=len(values),
    )


def analyze_all_symmetries(
    collector: MetricCollector,
    tolerance: float = 0.15,
) -> List[SymmetryResult]:
    """Run symmetry analysis across every pipeline/metric key in the collector."""
    results: List[SymmetryResult] = []
    for key, history in collector.get_all_history().items():
        result = analyze_symmetry(history, tolerance=tolerance)
        if result is not None:
            results.append(result)
    return results
