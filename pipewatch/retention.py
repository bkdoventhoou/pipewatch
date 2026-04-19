"""Retention policy: prune old metrics from collector history."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pipewatch.collector import MetricCollector


@dataclass
class RetentionPolicy:
    max_age_seconds: Optional[float] = None
    max_entries: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "max_age_seconds": self.max_age_seconds,
            "max_entries": self.max_entries,
        }


def prune_by_age(collector: MetricCollector, max_age_seconds: float) -> int:
    """Remove metrics older than max_age_seconds. Returns count removed."""
    cutoff = datetime.utcnow() - timedelta(seconds=max_age_seconds)
    removed = 0
    for key in list(collector._history.keys()):
        before = len(collector._history[key])
        collector._history[key] = [
            m for m in collector._history[key] if m.timestamp >= cutoff
        ]
        removed += before - len(collector._history[key])
        if not collector._history[key]:
            del collector._history[key]
    return removed


def prune_by_count(collector: MetricCollector, max_entries: int) -> int:
    """Keep only the latest max_entries per key. Returns count removed."""
    removed = 0
    for key in list(collector._history.keys()):
        history = collector._history[key]
        if len(history) > max_entries:
            removed += len(history) - max_entries
            collector._history[key] = history[-max_entries:]
    return removed


def apply_retention(collector: MetricCollector, policy: RetentionPolicy) -> int:
    """Apply a RetentionPolicy to a collector. Returns total entries removed."""
    removed = 0
    if policy.max_age_seconds is not None:
        removed += prune_by_age(collector, policy.max_age_seconds)
    if policy.max_entries is not None:
        removed += prune_by_count(collector, policy.max_entries)
    return removed
