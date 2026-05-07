"""Compaction: reduce a metric history by merging consecutive same-status runs."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class CompactedRun:
    pipeline: str
    metric_name: str
    status: MetricStatus
    start_ts: float
    end_ts: float
    count: int
    avg_value: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "status": self.status.value,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "count": self.count,
            "avg_value": round(self.avg_value, 6),
        }


def compact_series(metrics: List[PipelineMetric]) -> List[CompactedRun]:
    """Merge consecutive metrics with the same status into CompactedRun entries."""
    if not metrics:
        return []

    runs: List[CompactedRun] = []
    current = metrics[0]
    run_values = [current.value]
    run_start = current.timestamp

    for m in metrics[1:]:
        if m.status == current.status:
            run_values.append(m.value)
        else:
            runs.append(CompactedRun(
                pipeline=current.pipeline,
                metric_name=current.name,
                status=current.status,
                start_ts=run_start,
                end_ts=current.timestamp,
                count=len(run_values),
                avg_value=sum(run_values) / len(run_values),
            ))
            current = m
            run_values = [m.value]
            run_start = m.timestamp

    runs.append(CompactedRun(
        pipeline=current.pipeline,
        metric_name=current.name,
        status=current.status,
        start_ts=run_start,
        end_ts=current.timestamp,
        count=len(run_values),
        avg_value=sum(run_values) / len(run_values),
    ))
    return runs


def compact_all(history: dict) -> dict:
    """Apply compact_series to every (pipeline, metric_name) key in a collector history."""
    return {
        key: compact_series(metrics)
        for key, metrics in history.items()
    }
