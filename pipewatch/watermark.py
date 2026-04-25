"""High-watermark tracking for pipeline metric values."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class WatermarkEntry:
    pipeline: str
    metric_name: str
    high: float
    low: float
    high_ts: float
    low_ts: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "high": self.high,
            "low": self.low,
            "high_ts": self.high_ts,
            "low_ts": self.low_ts,
        }


class WatermarkTracker:
    """Tracks per-pipeline, per-metric high and low watermarks."""

    def __init__(self) -> None:
        self._entries: Dict[str, WatermarkEntry] = {}

    def _key(self, pipeline: str, metric_name: str) -> str:
        return f"{pipeline}::{metric_name}"

    def update(self, metric: PipelineMetric) -> WatermarkEntry:
        key = self._key(metric.pipeline, metric.name)
        ts = metric.timestamp
        v = metric.value
        if key not in self._entries:
            self._entries[key] = WatermarkEntry(
                pipeline=metric.pipeline,
                metric_name=metric.name,
                high=v,
                low=v,
                high_ts=ts,
                low_ts=ts,
            )
        else:
            e = self._entries[key]
            if v > e.high:
                e.high = v
                e.high_ts = ts
            if v < e.low:
                e.low = v
                e.low_ts = ts
        return self._entries[key]

    def get(self, pipeline: str, metric_name: str) -> Optional[WatermarkEntry]:
        return self._entries.get(self._key(pipeline, metric_name))

    def all_entries(self) -> List[WatermarkEntry]:
        return list(self._entries.values())

    def reset(self, pipeline: str, metric_name: str) -> None:
        key = self._key(pipeline, metric_name)
        self._entries.pop(key, None)


def track_watermarks(
    metrics: List[PipelineMetric],
    tracker: Optional[WatermarkTracker] = None,
) -> WatermarkTracker:
    """Convenience: feed a list of metrics into a tracker and return it."""
    if tracker is None:
        tracker = WatermarkTracker()
    for m in metrics:
        tracker.update(m)
    return tracker
