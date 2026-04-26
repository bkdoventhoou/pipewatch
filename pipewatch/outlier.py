"""Outlier detection for pipeline metrics using IQR-based method."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class OutlierResult:
    pipeline: str
    metric_name: str
    value: float
    q1: float
    q3: float
    iqr: float
    lower_fence: float
    upper_fence: float
    is_outlier: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "q1": self.q1,
            "q3": self.q3,
            "iqr": self.iqr,
            "lower_fence": self.lower_fence,
            "upper_fence": self.upper_fence,
            "is_outlier": self.is_outlier,
        }


def _quartiles(values: List[float]):
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    lower_half = sorted_vals[:mid]
    upper_half = sorted_vals[mid:] if n % 2 == 0 else sorted_vals[mid + 1:]
    q1 = sorted_vals[len(lower_half) // 2] if lower_half else sorted_vals[0]
    q3 = upper_half[len(upper_half) // 2] if upper_half else sorted_vals[-1]
    return q1, q3


def detect_outlier(
    metrics: List[PipelineMetric],
    multiplier: float = 1.5,
) -> Optional[OutlierResult]:
    """Detect if the most recent metric value is an IQR outlier."""
    if len(metrics) < 4:
        return None
    values = [m.value for m in metrics]
    q1, q3 = _quartiles(values)
    iqr = q3 - q1
    if iqr == 0.0:
        return None
    lower_fence = q1 - multiplier * iqr
    upper_fence = q3 + multiplier * iqr
    latest = metrics[-1]
    is_outlier = latest.value < lower_fence or latest.value > upper_fence
    return OutlierResult(
        pipeline=latest.pipeline,
        metric_name=latest.name,
        value=latest.value,
        q1=q1,
        q3=q3,
        iqr=iqr,
        lower_fence=lower_fence,
        upper_fence=upper_fence,
        is_outlier=is_outlier,
    )


def detect_all_outliers(
    history: dict,
    multiplier: float = 1.5,
) -> List[OutlierResult]:
    """Run outlier detection across all pipeline/metric series."""
    results = []
    for metrics in history.values():
        result = detect_outlier(metrics, multiplier=multiplier)
        if result is not None:
            results.append(result)
    return results
