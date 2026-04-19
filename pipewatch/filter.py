"""Filtering utilities for pipeline metrics and report entries."""

from typing import List, Optional
from pipewatch.metrics import PipelineMetric, MetricStatus


def filter_by_status(
    metrics: List[PipelineMetric],
    statuses: List[MetricStatus],
) -> List[PipelineMetric]:
    """Return only metrics whose status is in the given list."""
    return [m for m in metrics if m.status in statuses]


def filter_by_pipeline(
    metrics: List[PipelineMetric],
    pipeline: str,
) -> List[PipelineMetric]:
    """Return only metrics belonging to the given pipeline."""
    return [m for m in metrics if m.pipeline == pipeline]


def filter_by_metric_name(
    metrics: List[PipelineMetric],
    name: str,
) -> List[PipelineMetric]:
    """Return only metrics with the given metric name."""
    return [m for m in metrics if m.name == name]


def apply_filters(
    metrics: List[PipelineMetric],
    pipeline: Optional[str] = None,
    name: Optional[str] = None,
    statuses: Optional[List[MetricStatus]] = None,
) -> List[PipelineMetric]:
    """Apply all provided filters in sequence."""
    result = metrics
    if pipeline is not None:
        result = filter_by_pipeline(result, pipeline)
    if name is not None:
        result = filter_by_metric_name(result, name)
    if statuses is not None:
        result = filter_by_status(result, statuses)
    return result
