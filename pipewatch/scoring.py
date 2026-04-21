"""Pipeline health scoring: compute a numeric health score for pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import MetricStatus, PipelineMetric

# Weights applied per status when computing a pipeline score (0-100)
_STATUS_WEIGHT: Dict[str, float] = {
    MetricStatus.OK: 1.0,
    MetricStatus.WARNING: 0.5,
    MetricStatus.CRITICAL: 0.0,
}


@dataclass
class PipelineScore:
    pipeline: str
    score: float          # 0.0 – 100.0
    total_metrics: int
    ok_count: int
    warning_count: int
    critical_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 2),
            "total_metrics": self.total_metrics,
            "ok_count": self.ok_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
        }

    @property
    def grade(self) -> str:
        if self.score >= 90:
            return "A"
        if self.score >= 75:
            return "B"
        if self.score >= 50:
            return "C"
        if self.score >= 25:
            return "D"
        return "F"


def score_pipeline(pipeline: str, metrics: List[PipelineMetric]) -> Optional[PipelineScore]:
    """Return a PipelineScore for *pipeline* computed from *metrics*."""
    relevant = [m for m in metrics if m.pipeline == pipeline]
    if not relevant:
        return None

    ok = sum(1 for m in relevant if m.status == MetricStatus.OK)
    warn = sum(1 for m in relevant if m.status == MetricStatus.WARNING)
    crit = sum(1 for m in relevant if m.status == MetricStatus.CRITICAL)
    total = len(relevant)

    weighted = sum(_STATUS_WEIGHT.get(m.status, 0.0) for m in relevant)
    score = (weighted / total) * 100.0

    return PipelineScore(
        pipeline=pipeline,
        score=score,
        total_metrics=total,
        ok_count=ok,
        warning_count=warn,
        critical_count=crit,
    )


def score_all(metrics: List[PipelineMetric]) -> List[PipelineScore]:
    """Return a PipelineScore for every distinct pipeline in *metrics*."""
    pipelines = dict.fromkeys(m.pipeline for m in metrics)
    results = []
    for pipeline in pipelines:
        result = score_pipeline(pipeline, metrics)
        if result is not None:
            results.append(result)
    return results
