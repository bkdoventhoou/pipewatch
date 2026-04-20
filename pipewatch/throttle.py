"""Alert throttling: suppress repeated alerts within a cooldown window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class ThrottleEntry:
    last_fired: float
    fire_count: int = 1

    def to_dict(self) -> dict:
        return {
            "last_fired": self.last_fired,
            "fire_count": self.fire_count,
        }


class AlertThrottle:
    """Tracks per-metric alert history and suppresses alerts within cooldown_seconds."""

    def __init__(self, cooldown_seconds: float = 300.0) -> None:
        self.cooldown_seconds = cooldown_seconds
        self._registry: Dict[str, ThrottleEntry] = {}

    def _key(self, metric: PipelineMetric) -> str:
        return f"{metric.pipeline_name}::{metric.metric_name}::{metric.status.value}"

    def should_fire(self, metric: PipelineMetric) -> bool:
        """Return True if the alert should fire (not throttled)."""
        if metric.status == MetricStatus.OK:
            return False
        key = self._key(metric)
        now = time.monotonic()
        entry = self._registry.get(key)
        if entry is None:
            return True
        return (now - entry.last_fired) >= self.cooldown_seconds

    def record(self, metric: PipelineMetric) -> None:
        """Record that an alert was fired for this metric."""
        key = self._key(metric)
        now = time.monotonic()
        entry = self._registry.get(key)
        if entry is None:
            self._registry[key] = ThrottleEntry(last_fired=now)
        else:
            entry.last_fired = now
            entry.fire_count += 1

    def reset(self, metric: PipelineMetric) -> None:
        """Clear throttle state for a metric (e.g. when it returns to OK)."""
        key = self._key(metric)
        self._registry.pop(key, None)

    def get_entry(self, metric: PipelineMetric) -> Optional[ThrottleEntry]:
        return self._registry.get(self._key(metric))

    def stats(self) -> Dict[str, dict]:
        return {k: v.to_dict() for k, v in self._registry.items()}
