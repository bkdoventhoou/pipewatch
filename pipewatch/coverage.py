"""Coverage analysis: measures what fraction of a pipeline's metrics are healthy."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class CoverageResult:
    pipeline: str
    total: int
    ok_count: int
    warning_count: int
    critical_count: int
    coverage_ratio: float  # fraction of metrics that are OK
    healthy: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total": self.total,
            "ok_count": self.ok_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "coverage_ratio": round(self.coverage_ratio, 4),
            "healthy": self.healthy,
        }


def analyze_coverage(
    pipeline: str,
    metrics: List[PipelineMetric],
    healthy_threshold: float = 1.0,
) -> Optional[CoverageResult]:
    """Return a CoverageResult for *pipeline* or None if there are no metrics.

    *healthy_threshold* is the minimum coverage_ratio (0.0–1.0) required for
    the pipeline to be considered healthy.  Defaults to 1.0 (all OK).
    """
    relevant = [m for m in metrics if m.pipeline == pipeline]
    if not relevant:
        return None

    total = len(relevant)
    ok_count = sum(1 for m in relevant if m.status == MetricStatus.OK)
    warning_count = sum(1 for m in relevant if m.status == MetricStatus.WARNING)
    critical_count = sum(1 for m in relevant if m.status == MetricStatus.CRITICAL)
    ratio = ok_count / total
    healthy = ratio >= healthy_threshold

    return CoverageResult(
        pipeline=pipeline,
        total=total,
        ok_count=ok_count,
        warning_count=warning_count,
        critical_count=critical_count,
        coverage_ratio=ratio,
        healthy=healthy,
    )


def analyze_all_coverages(
    metrics: List[PipelineMetric],
    healthy_threshold: float = 1.0,
) -> Dict[str, CoverageResult]:
    """Return a mapping of pipeline → CoverageResult for every pipeline seen."""
    pipelines = {m.pipeline for m in metrics}
    results: Dict[str, CoverageResult] = {}
    for pipeline in sorted(pipelines):
        result = analyze_coverage(pipeline, metrics, healthy_threshold)
        if result is not None:
            results[pipeline] = result
    return results
