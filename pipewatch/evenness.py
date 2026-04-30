"""Evenness analysis: measures how uniformly metric values are distributed over time."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric


@dataclass
class EvennessResult:
    pipeline: str
    metric_name: str
    sample_count: int
    entropy: float          # normalised Shannon entropy in [0, 1]
    is_uneven: bool
    entropy_threshold: float

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "sample_count": self.sample_count,
            "entropy": round(self.entropy, 6),
            "is_uneven": self.is_uneven,
            "entropy_threshold": self.entropy_threshold,
        }


def _bin_values(values: List[float], bins: int = 10) -> List[int]:
    """Bucket continuous values into *bins* equal-width bins."""
    lo, hi = min(values), max(values)
    if hi == lo:
        return [len(values)] + [0] * (bins - 1)
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        idx = min(int((v - lo) / width), bins - 1)
        counts[idx] += 1
    return counts


def _normalised_entropy(counts: List[int]) -> float:
    """Shannon entropy normalised to [0, 1] by log2(k)."""
    import math
    total = sum(counts)
    if total == 0:
        return 0.0
    k = sum(1 for c in counts if c > 0)
    if k <= 1:
        return 0.0
    entropy = -sum((c / total) * math.log2(c / total) for c in counts if c > 0)
    return entropy / math.log2(k)


def analyze_evenness(
    metrics: List[PipelineMetric],
    entropy_threshold: float = 0.5,
    bins: int = 10,
    min_samples: int = 5,
) -> Optional[EvennessResult]:
    if len(metrics) < min_samples:
        return None
    values = [m.value for m in metrics]
    counts = _bin_values(values, bins=bins)
    entropy = _normalised_entropy(counts)
    return EvennessResult(
        pipeline=metrics[0].pipeline,
        metric_name=metrics[0].name,
        sample_count=len(metrics),
        entropy=entropy,
        is_uneven=entropy < entropy_threshold,
        entropy_threshold=entropy_threshold,
    )


def analyze_all_evenness(
    collector: MetricCollector,
    entropy_threshold: float = 0.5,
    bins: int = 10,
    min_samples: int = 5,
) -> Dict[str, EvennessResult]:
    results: Dict[str, EvennessResult] = {}
    for key, history in collector.get_history().items():
        result = analyze_evenness(
            history,
            entropy_threshold=entropy_threshold,
            bins=bins,
            min_samples=min_samples,
        )
        if result is not None:
            results[key] = result
    return results
