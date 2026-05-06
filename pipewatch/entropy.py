"""Entropy analysis for pipeline metric value distributions."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class EntropyResult:
    pipeline: str
    metric_name: str
    entropy: float
    max_entropy: float
    normalised: float  # 0.0 (fully predictable) – 1.0 (fully random)
    sample_count: int
    high_entropy: bool

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "entropy": round(self.entropy, 6),
            "max_entropy": round(self.max_entropy, 6),
            "normalised": round(self.normalised, 6),
            "sample_count": self.sample_count,
            "high_entropy": self.high_entropy,
        }


def _shannon_entropy(values: List[float], bins: int = 10) -> Optional[float]:
    if not values:
        return None
    lo, hi = min(values), max(values)
    if lo == hi:
        return 0.0
    width = (hi - lo) / bins
    counts: Dict[int, int] = {}
    for v in values:
        idx = min(int((v - lo) / width), bins - 1)
        counts[idx] = counts.get(idx, 0) + 1
    n = len(values)
    entropy = 0.0
    for c in counts.values():
        p = c / n
        entropy -= p * math.log2(p)
    return entropy


def analyze_entropy(
    metrics: List[PipelineMetric],
    threshold: float = 0.75,
    bins: int = 10,
) -> Optional[EntropyResult]:
    if len(metrics) < 2:
        return None
    pipeline = metrics[0].pipeline
    name = metrics[0].metric_name
    values = [m.value for m in metrics]
    entropy = _shannon_entropy(values, bins=bins)
    if entropy is None:
        return None
    max_entropy = math.log2(bins)
    normalised = entropy / max_entropy if max_entropy > 0 else 0.0
    return EntropyResult(
        pipeline=pipeline,
        metric_name=name,
        entropy=entropy,
        max_entropy=max_entropy,
        normalised=normalised,
        sample_count=len(values),
        high_entropy=normalised >= threshold,
    )


def analyze_all_entropies(
    history: Dict[str, List[PipelineMetric]],
    threshold: float = 0.75,
    bins: int = 10,
) -> List[EntropyResult]:
    results = []
    for metrics in history.values():
        result = analyze_entropy(metrics, threshold=threshold, bins=bins)
        if result is not None:
            results.append(result)
    return results
