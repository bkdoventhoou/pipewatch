"""Deduplication module for suppressing repeated identical alerts."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class DedupeEntry:
    key: str
    status: MetricStatus
    first_seen: float
    last_seen: float
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "status": self.status.value,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "count": self.count,
        }


def _make_key(metric: PipelineMetric) -> str:
    """Create a stable deduplication key from pipeline, metric name, and status."""
    raw = f"{metric.pipeline}:{metric.name}:{metric.status.value}"
    return hashlib.sha1(raw.encode()).hexdigest()


class AlertDeduplicator:
    """Tracks seen alerts and suppresses duplicates within a time window."""

    def __init__(self, window_seconds: float = 300.0) -> None:
        self.window_seconds = window_seconds
        self._seen: Dict[str, DedupeEntry] = {}

    def is_duplicate(self, metric: PipelineMetric) -> bool:
        """Return True if an identical alert was already seen within the window."""
        if metric.status == MetricStatus.OK:
            return False
        key = _make_key(metric)
        now = time.time()
        if key in self._seen:
            entry = self._seen[key]
            if now - entry.last_seen < self.window_seconds:
                entry.last_seen = now
                entry.count += 1
                return True
        self._seen[key] = DedupeEntry(
            key=key,
            status=metric.status,
            first_seen=now,
            last_seen=now,
        )
        return False

    def get_entry(self, metric: PipelineMetric) -> Optional[DedupeEntry]:
        return self._seen.get(_make_key(metric))

    def clear(self) -> None:
        self._seen.clear()

    def evict_expired(self) -> int:
        """Remove entries older than the window. Returns number evicted."""
        now = time.time()
        expired = [
            k for k, v in self._seen.items()
            if now - v.last_seen >= self.window_seconds
        ]
        for k in expired:
            del self._seen[k]
        return len(expired)

    def stats(self) -> dict:
        return {
            "tracked": len(self._seen),
            "window_seconds": self.window_seconds,
        }
