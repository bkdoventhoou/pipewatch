"""Periodic digest summarizing pipeline health across all metrics."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.summary import PipelineSummary, summarize_by_pipeline


@dataclass
class DigestReport:
    generated_at: str
    total_pipelines: int
    healthy: int
    degraded: int
    critical: int
    summaries: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "total_pipelines": self.total_pipelines,
            "healthy": self.healthy,
            "degraded": self.degraded,
            "critical": self.critical,
            "summaries": self.summaries,
        }


def build_digest(metrics: List[PipelineMetric]) -> DigestReport:
    """Build a digest report from a flat list of metrics."""
    summaries = summarize_by_pipeline(metrics)

    healthy = sum(1 for s in summaries if s.health == "healthy")
    degraded = sum(1 for s in summaries if s.health == "degraded")
    critical = sum(1 for s in summaries if s.health == "critical")

    return DigestReport(
        generated_at=datetime.utcnow().isoformat(),
        total_pipelines=len(summaries),
        healthy=healthy,
        degraded=degraded,
        critical=critical,
        summaries=[s.to_dict() for s in summaries],
    )


def format_digest_text(digest: DigestReport) -> str:
    lines = [
        f"Digest Report — {digest.generated_at}",
        f"Pipelines: {digest.total_pipelines}  "
        f"Healthy: {digest.healthy}  "
        f"Degraded: {digest.degraded}  "
        f"Critical: {digest.critical}",
        "-" * 50,
    ]
    for s in digest.summaries:
        lines.append(
            f"  {s['pipeline']:30s}  health={s['health']}  "
            f"ok={s['ok']} warn={s['warning']} crit={s['critical']}"
        )
    return "\n".join(lines)
