"""Recurrence detection: identify metrics that repeatedly breach thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import MetricStatus, PipelineMetric


@dataclass
class RecurrenceResult:
    pipeline: str
    metric_name: str
    breach_count: int
    total_count: int
    recurrence_rate: float  # 0.0 – 1.0
    is_recurring: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "breach_count": self.breach_count,
            "total_count": self.total_count,
            "recurrence_rate": round(self.recurrence_rate, 4),
            "is_recurring": self.is_recurring,
        }


def _is_breach(metric: PipelineMetric) -> bool:
    return metric.status in (MetricStatus.WARNING, MetricStatus.CRITICAL)


def detect_recurrence(
    metrics: List[PipelineMetric],
    threshold: float = 0.3,
    min_count: int = 3,
) -> Optional[RecurrenceResult]:
    """Return a RecurrenceResult for a single (pipeline, metric_name) series."""
    if len(metrics) < min_count:
        return None

    pipeline = metrics[0].pipeline
    metric_name = metrics[0].metric_name
    total = len(metrics)
    breaches = sum(1 for m in metrics if _is_breach(m))
    rate = breaches / total

    return RecurrenceResult(
        pipeline=pipeline,
        metric_name=metric_name,
        breach_count=breaches,
        total_count=total,
        recurrence_rate=rate,
        is_recurring=rate >= threshold,
    )


def detect_all_recurrences(
    history: Dict[str, List[PipelineMetric]],
    threshold: float = 0.3,
    min_count: int = 3,
) -> List[RecurrenceResult]:
    """Run recurrence detection over all collector history entries."""
    results: List[RecurrenceResult] = []
    for metrics in history.values():
        if not metrics:
            continue
        result = detect_recurrence(metrics, threshold=threshold, min_count=min_count)
        if result is not None:
            results.append(result)
    return results
