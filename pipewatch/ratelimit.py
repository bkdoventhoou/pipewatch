"""Rate limiting for alert dispatch — caps alerts per pipeline per time window."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class RateLimitEntry:
    pipeline: str
    metric_name: str
    window_seconds: int
    max_alerts: int
    timestamps: List[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "window_seconds": self.window_seconds,
            "max_alerts": self.max_alerts,
            "alert_count_in_window": self._count_in_window(),
        }

    def _count_in_window(self) -> int:
        cutoff = time.time() - self.window_seconds
        return sum(1 for t in self.timestamps if t >= cutoff)

    def _prune(self) -> None:
        cutoff = time.time() - self.window_seconds
        self.timestamps = [t for t in self.timestamps if t >= cutoff]


class AlertRateLimiter:
    """Tracks alert frequency and suppresses bursts exceeding max_alerts per window."""

    def __init__(self, window_seconds: int = 300, max_alerts: int = 3) -> None:
        self.window_seconds = window_seconds
        self.max_alerts = max_alerts
        self._entries: Dict[str, RateLimitEntry] = {}

    def _key(self, metric: PipelineMetric) -> str:
        return f"{metric.pipeline_name}::{metric.metric_name}"

    def _get_or_create(self, metric: PipelineMetric) -> RateLimitEntry:
        key = self._key(metric)
        if key not in self._entries:
            self._entries[key] = RateLimitEntry(
                pipeline=metric.pipeline_name,
                metric_name=metric.metric_name,
                window_seconds=self.window_seconds,
                max_alerts=self.max_alerts,
            )
        return self._entries[key]

    def is_allowed(self, metric: PipelineMetric) -> bool:
        """Return True if the alert should be dispatched; False if rate-limited."""
        if metric.status == MetricStatus.OK:
            return False
        entry = self._get_or_create(metric)
        entry._prune()
        if entry._count_in_window() >= self.max_alerts:
            return False
        entry.timestamps.append(time.time())
        return True

    def status(self) -> List[dict]:
        return [e.to_dict() for e in self._entries.values()]

    def reset(self, pipeline: Optional[str] = None, metric_name: Optional[str] = None) -> None:
        if pipeline is None and metric_name is None:
            self._entries.clear()
            return
        keys_to_remove = [
            k for k, e in self._entries.items()
            if (pipeline is None or e.pipeline == pipeline)
            and (metric_name is None or e.metric_name == metric_name)
        ]
        for k in keys_to_remove:
            del self._entries[k]
