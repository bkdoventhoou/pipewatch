"""Audit log for pipeline metric state transitions."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import MetricStatus, PipelineMetric


@dataclass
class AuditEvent:
    pipeline: str
    metric_name: str
    previous_status: Optional[MetricStatus]
    current_status: MetricStatus
    value: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "previous_status": self.previous_status.value if self.previous_status else None,
            "current_status": self.current_status.value,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
        }


class AuditLog:
    def __init__(self) -> None:
        self._events: List[AuditEvent] = []
        self._last_status: dict[str, MetricStatus] = {}

    def _key(self, metric: PipelineMetric) -> str:
        return f"{metric.pipeline}:{metric.name}"

    def record(self, metric: PipelineMetric) -> Optional[AuditEvent]:
        """Record a metric; emit an AuditEvent only on status transitions."""
        key = self._key(metric)
        previous = self._last_status.get(key)
        current = metric.status
        self._last_status[key] = current
        if previous == current:
            return None
        event = AuditEvent(
            pipeline=metric.pipeline,
            metric_name=metric.name,
            previous_status=previous,
            current_status=current,
            value=metric.value,
        )
        self._events.append(event)
        return event

    def get_events(
        self,
        pipeline: Optional[str] = None,
        metric_name: Optional[str] = None,
    ) -> List[AuditEvent]:
        events = self._events
        if pipeline:
            events = [e for e in events if e.pipeline == pipeline]
        if metric_name:
            events = [e for e in events if e.metric_name == metric_name]
        return list(events)

    def clear(self) -> None:
        self._events.clear()
        self._last_status.clear()
