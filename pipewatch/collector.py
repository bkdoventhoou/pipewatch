"""Metric collection and threshold evaluation."""
from typing import List, Dict
from pipewatch.metrics import PipelineMetric, ThresholdConfig, MetricStatus


class MetricCollector:
    """Collects pipeline metrics and evaluates them against thresholds."""

    def __init__(self, thresholds: List[ThresholdConfig] = None):
        self._thresholds: Dict[str, ThresholdConfig] = {}
        self._history: List[PipelineMetric] = []
        if thresholds:
            for t in thresholds:
                self.add_threshold(t)

    def add_threshold(self, threshold: ThresholdConfig) -> None:
        self._thresholds[threshold.metric_name] = threshold

    def record(self, metric: PipelineMetric) -> PipelineMetric:
        """Record a metric, evaluating its status against known thresholds."""
        if metric.metric_name in self._thresholds:
            metric.status = self._thresholds[metric.metric_name].evaluate(metric.value)
        else:
            metric.status = MetricStatus.UNKNOWN
        self._history.append(metric)
        return metric

    def get_history(self, pipeline_name: str = None) -> List[PipelineMetric]:
        if pipeline_name:
            return [m for m in self._history if m.pipeline_name == pipeline_name]
        return list(self._history)

    def latest(self, pipeline_name: str = None) -> List[PipelineMetric]:
        """Return only the most recent metric per (pipeline, metric_name) pair."""
        seen = {}
        for m in reversed(self._history):
            key = (m.pipeline_name, m.metric_name)
            if key not in seen:
                if pipeline_name is None or m.pipeline_name == pipeline_name:
                    seen[key] = m
        return list(seen.values())

    def clear(self) -> None:
        self._history.clear()
