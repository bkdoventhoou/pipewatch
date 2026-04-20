"""Alert silencing: mute alerts for specific pipelines or metrics by name."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SilenceRule:
    pipeline: Optional[str]  # None means match all pipelines
    metric_name: Optional[str]  # None means match all metric names
    expires_at: Optional[float]  # Unix timestamp; None means never expires
    reason: str = ""

    def is_active(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return True
        t = now if now is not None else time.time()
        return t < self.expires_at

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline is not None and metric.pipeline != self.pipeline:
            return False
        if self.metric_name is not None and metric.name != self.metric_name:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "expires_at": self.expires_at,
            "reason": self.reason,
        }


class SilenceRegistry:
    def __init__(self) -> None:
        self._rules: list[SilenceRule] = []

    def add(self, rule: SilenceRule) -> None:
        self._rules.append(rule)

    def is_silenced(self, metric: PipelineMetric, now: Optional[float] = None) -> bool:
        t = now if now is not None else time.time()
        return any(r.is_active(t) and r.matches(metric) for r in self._rules)

    def prune_expired(self, now: Optional[float] = None) -> int:
        """Remove expired rules. Returns count removed."""
        t = now if now is not None else time.time()
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.is_active(t)]
        return before - len(self._rules)

    def active_rules(self, now: Optional[float] = None) -> list[SilenceRule]:
        t = now if now is not None else time.time()
        return [r for r in self._rules if r.is_active(t)]

    def __len__(self) -> int:
        return len(self._rules)
