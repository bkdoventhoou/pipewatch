"""Cycle detection: identify pipelines whose metric values repeat in a regular pattern."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class CycleResult:
    pipeline: str
    metric_name: str
    period: int          # detected period length in number of samples
    confidence: float    # 0.0 – 1.0
    is_cyclic: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "period": self.period,
            "confidence": round(self.confidence, 4),
            "is_cyclic": self.is_cyclic,
        }


def _autocorrelation(values: List[float], lag: int) -> Optional[float]:
    """Pearson autocorrelation at a given lag."""
    n = len(values)
    if n <= lag:
        return None
    xs = values[: n - lag]
    ys = values[lag:]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((a - mean_x) * (b - mean_y) for a, b in zip(xs, ys))
    den_x = sum((a - mean_x) ** 2 for a in xs) ** 0.5
    den_y = sum((b - mean_y) ** 2 for b in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def detect_cycle(
    metrics: List[PipelineMetric],
    min_samples: int = 8,
    confidence_threshold: float = 0.75,
) -> Optional[CycleResult]:
    if len(metrics) < min_samples:
        return None
    pipeline = metrics[0].pipeline
    name = metrics[0].metric_name
    values = [m.value for m in metrics]
    max_period = len(values) // 2
    best_period = 0
    best_conf = 0.0
    for lag in range(2, max_period + 1):
        r = _autocorrelation(values, lag)
        if r is not None and r > best_conf:
            best_conf = r
            best_period = lag
    is_cyclic = best_conf >= confidence_threshold
    return CycleResult(
        pipeline=pipeline,
        metric_name=name,
        period=best_period,
        confidence=best_conf,
        is_cyclic=is_cyclic,
    )


def detect_all_cycles(
    collector: MetricCollector,
    min_samples: int = 8,
    confidence_threshold: float = 0.75,
) -> Dict[str, CycleResult]:
    results: Dict[str, CycleResult] = {}
    for key, history in collector.get_history().items():
        result = detect_cycle(history, min_samples, confidence_threshold)
        if result is not None:
            results[key] = result
    return results
