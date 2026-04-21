"""SLA breach tracking and history for pipeline metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.sla import SLAResult


@dataclass
class SLABreachEvent:
    pipeline: str
    metric_name: str
    breached_at: str
    sla_target_pct: float
    actual_pct: float
    delta_pct: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "breached_at": self.breached_at,
            "sla_target_pct": self.sla_target_pct,
            "actual_pct": self.actual_pct,
            "delta_pct": self.delta_pct,
        }


@dataclass
class SLATracker:
    _breaches: Dict[str, List[SLABreachEvent]] = field(default_factory=dict)

    def _key(self, pipeline: str, metric_name: str) -> str:
        return f"{pipeline}::{metric_name}"

    def record(self, result: SLAResult) -> Optional[SLABreachEvent]:
        """Record an SLA result; return a breach event if SLA was violated."""
        if result.met:
            return None
        key = self._key(result.pipeline, result.metric_name)
        event = SLABreachEvent(
            pipeline=result.pipeline,
            metric_name=result.metric_name,
            breached_at=datetime.utcnow().isoformat(),
            sla_target_pct=result.target_pct,
            actual_pct=result.actual_pct,
            delta_pct=result.actual_pct - result.target_pct,
        )
        self._breaches.setdefault(key, []).append(event)
        return event

    def get_breaches(self, pipeline: str, metric_name: str) -> List[SLABreachEvent]:
        return self._breaches.get(self._key(pipeline, metric_name), [])

    def breach_count(self, pipeline: str, metric_name: str) -> int:
        return len(self.get_breaches(pipeline, metric_name))

    def all_breaches(self) -> List[SLABreachEvent]:
        result: List[SLABreachEvent] = []
        for events in self._breaches.values():
            result.extend(events)
        return result

    def clear(self, pipeline: str, metric_name: str) -> None:
        key = self._key(pipeline, metric_name)
        self._breaches.pop(key, None)
