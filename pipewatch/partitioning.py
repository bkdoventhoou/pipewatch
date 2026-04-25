"""Metric partitioning: split metric history into time-based partitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MetricPartition:
    """A named time-bucket containing a slice of metric history."""

    label: str          # e.g. "2024-06-01T14" for hourly, "2024-06-01" for daily
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
            "label": self.label,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "count": self.count,
            "average": self.average,
        }


def _partition_label(ts: datetime, granularity: str) -> str:
    """Return a string bucket label for *ts* at the given granularity."""
    if granularity == "hourly":
        return ts.strftime("%Y-%m-%dT%H")
    if granularity == "daily":
        return ts.strftime("%Y-%m-%d")
    if granularity == "weekly":
        iso = ts.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    raise ValueError(f"Unknown granularity: {granularity!r}. Use 'hourly', 'daily', or 'weekly'.")


def partition_metrics(
    metrics: List[PipelineMetric],
    granularity: str = "hourly",
) -> Dict[str, MetricPartition]:
    """Group *metrics* into :class:`MetricPartition` buckets.

    Args:
        metrics: Flat list of :class:`PipelineMetric` instances.
        granularity: One of ``'hourly'``, ``'daily'``, or ``'weekly'``.

    Returns:
        Ordered dict mapping bucket label -> :class:`MetricPartition`.
    """
    buckets: Dict[str, MetricPartition] = {}

    for m in metrics:
        ts = datetime.fromtimestamp(m.timestamp, tz=timezone.utc)
        label = _partition_label(ts, granularity)

        if label not in buckets:
            # Compute bucket boundaries
            if granularity == "hourly":
                start = ts.replace(minute=0, second=0, microsecond=0)
                end = start.replace(hour=start.hour + 1) if start.hour < 23 else start.replace(day=start.day + 1, hour=0)
            elif granularity == "daily":
                start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                end = start.replace(day=start.day + 1)
            else:  # weekly
                day_of_week = ts.weekday()  # Monday=0
                start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                start = start.replace(day=start.day - day_of_week)
                end = start.replace(day=start.day + 7)
            buckets[label] = MetricPartition(label=label, start=start, end=end)

        buckets[label].metrics.append(m)

    return buckets
