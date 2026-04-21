"""Rollup: aggregate metrics over time windows into summarized buckets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class RollupBucket:
    pipeline: str
    metric_name: str
    window_start: float
    window_end: float
    count: int = 0
    total: float = 0.0
    min_value: float = float("inf")
    max_value: float = float("-inf")
    ok_count: int = 0
    warning_count: int = 0
    critical_count: int = 0

    @property
    def average(self) -> Optional[float]:
        return self.total / self.count if self.count > 0 else None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "average": self.average,
            "min": self.min_value if self.count > 0 else None,
            "max": self.max_value if self.count > 0 else None,
            "ok_count": self.ok_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
        }


def rollup_metrics(
    metrics: List[PipelineMetric],
    window_seconds: float = 60.0,
    now: Optional[float] = None,
) -> List[RollupBucket]:
    """Group metrics into fixed time windows and compute per-bucket stats."""
    if not metrics:
        return []

    if now is None:
        now = time.time()

    buckets: Dict[tuple, RollupBucket] = {}

    for m in metrics:
        ts = m.timestamp if m.timestamp is not None else now
        bucket_start = (ts // window_seconds) * window_seconds
        bucket_end = bucket_start + window_seconds
        key = (m.pipeline, m.name, bucket_start)

        if key not in buckets:
            buckets[key] = RollupBucket(
                pipeline=m.pipeline,
                metric_name=m.name,
                window_start=bucket_start,
                window_end=bucket_end,
            )

        b = buckets[key]
        b.count += 1
        b.total += m.value
        b.min_value = min(b.min_value, m.value)
        b.max_value = max(b.max_value, m.value)

        if m.status == MetricStatus.OK:
            b.ok_count += 1
        elif m.status == MetricStatus.WARNING:
            b.warning_count += 1
        elif m.status == MetricStatus.CRITICAL:
            b.critical_count += 1

    return sorted(buckets.values(), key=lambda b: (b.pipeline, b.metric_name, b.window_start))
