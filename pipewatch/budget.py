"""Alert budget tracking — limits total alerts fired per pipeline per time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class BudgetEntry:
    pipeline: str
    metric_name: str
    fired_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "fired_at": self.fired_at.isoformat(),
        }


@dataclass
class AlertBudget:
    max_alerts: int = 10
    window_seconds: int = 3600
    _log: List[BudgetEntry] = field(default_factory=list)

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.window_seconds)
        self._log = [e for e in self._log if e.fired_at >= cutoff]

    def _key(self, metric: PipelineMetric) -> str:
        return metric.pipeline

    def used(self, pipeline: str, now: Optional[datetime] = None) -> int:
        now = now or datetime.utcnow()
        self._prune(now)
        return sum(1 for e in self._log if e.pipeline == pipeline)

    def remaining(self, pipeline: str, now: Optional[datetime] = None) -> int:
        return max(0, self.max_alerts - self.used(pipeline, now))

    def allow(self, metric: PipelineMetric, now: Optional[datetime] = None) -> bool:
        """Return True and record usage if budget allows; False otherwise."""
        if metric.status == MetricStatus.OK:
            return False
        now = now or datetime.utcnow()
        self._prune(now)
        pipeline = metric.pipeline
        if self.used(pipeline, now) >= self.max_alerts:
            return False
        self._log.append(BudgetEntry(
            pipeline=pipeline,
            metric_name=metric.name,
            fired_at=now,
        ))
        return True

    def summary(self, now: Optional[datetime] = None) -> Dict[str, dict]:
        now = now or datetime.utcnow()
        self._prune(now)
        pipelines = {e.pipeline for e in self._log}
        return {
            p: {"used": self.used(p, now), "remaining": self.remaining(p, now)}
            for p in pipelines
        }
