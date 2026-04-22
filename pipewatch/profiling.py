"""Pipeline metric profiling: compute statistical profiles from historical data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MetricProfile:
    pipeline: str
    metric_name: str
    count: int
    mean: float
    std: float
    min_val: float
    max_val: float
    p50: float
    p95: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "count": self.count,
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
            "min": round(self.min_val, 4),
            "max": round(self.max_val, 4),
            "p50": round(self.p50, 4),
            "p95": round(self.p95, 4),
        }


def _percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * pct / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _std(vals: List[float], mean: float) -> float:
    if len(vals) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    return variance ** 0.5


def profile_metric(metrics: List[PipelineMetric]) -> Optional[MetricProfile]:
    """Build a statistical profile from a list of metrics sharing pipeline+name."""
    if not metrics:
        return None
    vals = sorted(m.value for m in metrics)
    mu = _mean(vals)
    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    return MetricProfile(
        pipeline=pipeline,
        metric_name=metric_name,
        count=len(vals),
        mean=mu,
        std=_std(vals, mu),
        min_val=vals[0],
        max_val=vals[-1],
        p50=_percentile(vals, 50),
        p95=_percentile(vals, 95),
    )


def profile_all(history: Dict[str, List[PipelineMetric]]) -> List[MetricProfile]:
    """Build profiles for every (pipeline, metric_name) group in collector history."""
    results: List[MetricProfile] = []
    for metrics in history.values():
        if not metrics:
            continue
        profile = profile_metric(metrics)
        if profile is not None:
            results.append(profile)
    return results
