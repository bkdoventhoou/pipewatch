"""Windowing: slice metric history into time-based windows for analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MetricWindow:
    """A named time window containing a slice of metric history."""

    pipeline: str
    metric_name: str
    label: str          # e.g. "last_5m", "last_1h"
    start: datetime
    end: datetime
    metrics: List[PipelineMetric] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.metrics)

    @property
    def average(self) -> Optional[float]:
        if not self.metrics:
            return None
        return sum(m.value for m in self.metrics) / len(self.metrics)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "label": self.label,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "count": self.count,
            "average": self.average,
        }


def slice_window(
    metrics: List[PipelineMetric],
    seconds: int,
    label: str,
    now: Optional[datetime] = None,
) -> List[PipelineMetric]:
    """Return only metrics whose timestamp falls within the last *seconds* seconds."""
    if now is None:
        now = datetime.utcnow()
    cutoff = now - timedelta(seconds=seconds)
    return [m for m in metrics if m.timestamp >= cutoff]


def build_windows(
    history: Dict[str, List[PipelineMetric]],
    window_configs: List[Dict],
    now: Optional[datetime] = None,
) -> List[MetricWindow]:
    """Build MetricWindow objects for every (pipeline+metric, window) combination.

    *window_configs* is a list of dicts with keys:
        - label  (str)  e.g. "last_5m"
        - seconds (int) e.g. 300
    *history* keys are "pipeline:metric_name" strings (collector convention).
    """
    if now is None:
        now = datetime.utcnow()

    results: List[MetricWindow] = []
    for key, metrics in history.items():
        if ":" not in key:
            continue
        pipeline, metric_name = key.split(":", 1)
        for cfg in window_configs:
            label = cfg["label"]
            seconds = int(cfg["seconds"])
            sliced = slice_window(metrics, seconds, label, now=now)
            results.append(
                MetricWindow(
                    pipeline=pipeline,
                    metric_name=metric_name,
                    label=label,
                    start=now - timedelta(seconds=seconds),
                    end=now,
                    metrics=sliced,
                )
            )
    return results
