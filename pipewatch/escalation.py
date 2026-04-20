"""Alert escalation: track repeated alerts and escalate severity after N occurrences."""

from dataclasses import dataclass, field
from typing import Dict, Optional
from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class EscalationEntry:
    pipeline: str
    metric_name: str
    status: MetricStatus
    count: int = 0
    escalated: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "status": self.status.value,
            "count": self.count,
            "escalated": self.escalated,
        }


def _make_key(metric: PipelineMetric) -> str:
    return f"{metric.pipeline}::{metric.name}"


class AlertEscalator:
    """Escalates alert severity after repeated occurrences within a window."""

    def __init__(self, threshold: int = 3):
        """Args:
            threshold: number of consecutive non-OK alerts before escalation.
        """
        self.threshold = threshold
        self._entries: Dict[str, EscalationEntry] = {}

    def evaluate(self, metric: PipelineMetric) -> Optional[MetricStatus]:
        """Return the (possibly escalated) status, or None if metric is OK.

        - Resets the counter when status returns to OK.
        - Escalates WARNING to CRITICAL after `threshold` consecutive hits.
        - CRITICAL stays CRITICAL regardless.
        """
        key = _make_key(metric)

        if metric.status == MetricStatus.OK:
            self._entries.pop(key, None)
            return None

        entry = self._entries.get(key)
        if entry is None or entry.status != metric.status:
            entry = EscalationEntry(
                pipeline=metric.pipeline,
                metric_name=metric.name,
                status=metric.status,
                count=1,
            )
            self._entries[key] = entry
        else:
            entry.count += 1

        if metric.status == MetricStatus.WARNING and entry.count >= self.threshold:
            entry.escalated = True
            return MetricStatus.CRITICAL

        return metric.status

    def get_entry(self, metric: PipelineMetric) -> Optional[EscalationEntry]:
        return self._entries.get(_make_key(metric))

    def all_entries(self) -> list:
        return [e.to_dict() for e in self._entries.values()]
