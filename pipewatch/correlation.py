"""Correlation analysis between pipeline metrics."""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from pipewatch.metrics import PipelineMetric


@dataclass
class CorrelationResult:
    metric_a: str
    metric_b: str
    pipeline: str
    coefficient: float
    sample_size: int

    def to_dict(self) -> dict:
        return {
            "metric_a": self.metric_a,
            "metric_b": self.metric_b,
            "pipeline": self.pipeline,
            "coefficient": round(self.coefficient, 4),
            "sample_size": self.sample_size,
        }


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = sum((x - mx) ** 2 for x in xs) ** 0.5
    den_y = sum((y - my) ** 2 for y in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def correlate_metrics(
    metrics: List[PipelineMetric],
    pipeline: str,
    metric_a: str,
    metric_b: str,
) -> Optional[CorrelationResult]:
    a_vals = [m.value for m in metrics if m.pipeline == pipeline and m.name == metric_a]
    b_vals = [m.value for m in metrics if m.pipeline == pipeline and m.name == metric_b]
    n = min(len(a_vals), len(b_vals))
    if n < 2:
        return None
    coef = _pearson(a_vals[:n], b_vals[:n])
    if coef is None:
        return None
    return CorrelationResult(metric_a, metric_b, pipeline, coef, n)


def correlate_all(
    metrics: List[PipelineMetric],
    pipeline: str,
) -> List[CorrelationResult]:
    names = list({m.name for m in metrics if m.pipeline == pipeline})
    results = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            r = correlate_metrics(metrics, pipeline, names[i], names[j])
            if r is not None:
                results.append(r)
    return results
