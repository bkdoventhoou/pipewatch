"""Alert suppression windows: silence alerts for a pipeline/metric during a defined time window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pipewatch.metrics import PipelineMetric


@dataclass
class SuppressionWindow:
    pipeline: str
    metric_name: Optional[str]  # None means suppress all metrics for pipeline
    start_ts: float
    end_ts: float
    reason: str = ""

    def is_active(self, ts: Optional[float] = None) -> bool:
        now = ts if ts is not None else time.time()
        return self.start_ts <= now <= self.end_ts

    def matches(self, metric: PipelineMetric) -> bool:
        if metric.pipeline != self.pipeline:
            return False
        if self.metric_name is not None and metric.name != self.metric_name:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "reason": self.reason,
            "active": self.is_active(),
        }


class SuppressionRegistry:
    def __init__(self) -> None:
        self._windows: List[SuppressionWindow] = []

    def add(self, window: SuppressionWindow) -> None:
        self._windows.append(window)

    def is_suppressed(self, metric: PipelineMetric, ts: Optional[float] = None) -> bool:
        return any(w.is_active(ts) and w.matches(metric) for w in self._windows)

    def active_windows(self, ts: Optional[float] = None) -> List[SuppressionWindow]:
        return [w for w in self._windows if w.is_active(ts)]

    def prune_expired(self, ts: Optional[float] = None) -> int:
        now = ts if ts is not None else time.time()
        before = len(self._windows)
        self._windows = [w for w in self._windows if w.end_ts >= now]
        return before - len(self._windows)

    def all_windows(self) -> List[SuppressionWindow]:
        return list(self._windows)
