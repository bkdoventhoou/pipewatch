"""Metric value normalization: scale values to [0, 1] or z-score."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class NormalizedMetric:
    pipeline: str
    metric_name: str
    original_value: float
    normalized_value: float
    strategy: str  # "minmax" or "zscore"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "original_value": self.original_value,
            "normalized_value": self.normalized_value,
            "strategy": self.strategy,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float]) -> float:
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / len(values)
    return variance ** 0.5


def normalize_minmax(metrics: List[PipelineMetric]) -> List[NormalizedMetric]:
    """Scale values linearly to [0, 1] using min-max normalization."""
    if not metrics:
        return []
    values = [m.value for m in metrics]
    lo, hi = min(values), max(values)
    span = hi - lo
    results = []
    for m in metrics:
        norm = 0.0 if span == 0 else (m.value - lo) / span
        results.append(
            NormalizedMetric(
                pipeline=m.pipeline,
                metric_name=m.name,
                original_value=m.value,
                normalized_value=round(norm, 6),
                strategy="minmax",
            )
        )
    return results


def normalize_zscore(metrics: List[PipelineMetric]) -> List[NormalizedMetric]:
    """Standardize values using z-score (mean=0, std=1)."""
    if not metrics:
        return []
    values = [m.value for m in metrics]
    mu = _mean(values)
    sigma = _std(values)
    results = []
    for m in metrics:
        norm = 0.0 if sigma == 0 else (m.value - mu) / sigma
        results.append(
            NormalizedMetric(
                pipeline=m.pipeline,
                metric_name=m.name,
                original_value=m.value,
                normalized_value=round(norm, 6),
                strategy="zscore",
            )
        )
    return results


def normalize_metrics(
    metrics: List[PipelineMetric], strategy: str = "minmax"
) -> List[NormalizedMetric]:
    """Dispatch to the requested normalization strategy."""
    if strategy == "minmax":
        return normalize_minmax(metrics)
    if strategy == "zscore":
        return normalize_zscore(metrics)
    raise ValueError(f"Unknown normalization strategy: {strategy!r}")
