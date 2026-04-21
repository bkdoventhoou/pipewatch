"""Metric sampling: downsample high-frequency metric histories."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SampledSeries:
    pipeline: str
    metric_name: str
    samples: List[PipelineMetric] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "sample_count": len(self.samples),
            "samples": [
                {"value": m.value, "timestamp": m.timestamp, "status": m.status.value}
                for m in self.samples
            ],
        }


def sample_every_nth(metrics: List[PipelineMetric], n: int) -> List[PipelineMetric]:
    """Keep every nth metric from the list."""
    if n < 1:
        raise ValueError("n must be >= 1")
    return metrics[::n]


def sample_by_count(metrics: List[PipelineMetric], max_count: int) -> List[PipelineMetric]:
    """Downsample to at most max_count evenly-spaced samples."""
    if max_count < 1:
        raise ValueError("max_count must be >= 1")
    total = len(metrics)
    if total <= max_count:
        return list(metrics)
    step = total / max_count
    return [metrics[int(i * step)] for i in range(max_count)]


def sample_series(
    metrics: List[PipelineMetric],
    max_count: Optional[int] = None,
    every_nth: Optional[int] = None,
) -> SampledSeries:
    """Build a SampledSeries from a list of metrics using one sampling strategy."""
    if not metrics:
        pipeline = ""
        name = ""
        return SampledSeries(pipeline=pipeline, metric_name=name, samples=[])

    pipeline = metrics[0].pipeline
    name = metrics[0].name

    if every_nth is not None:
        sampled = sample_every_nth(metrics, every_nth)
    elif max_count is not None:
        sampled = sample_by_count(metrics, max_count)
    else:
        sampled = list(metrics)

    return SampledSeries(pipeline=pipeline, metric_name=name, samples=sampled)


def sample_all(
    history: dict,
    max_count: Optional[int] = None,
    every_nth: Optional[int] = None,
) -> List[SampledSeries]:
    """Apply sampling to every (pipeline, metric_name) key in a collector history dict."""
    results = []
    for metrics in history.values():
        if not metrics:
            continue
        series = sample_series(metrics, max_count=max_count, every_nth=every_nth)
        results.append(series)
    return results
