"""Pipeline health report generation."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any

from pipewatch.metrics import PipelineMetric, MetricStatus, evaluate
from pipewatch.collector import MetricCollector


@dataclass
class ReportEntry:
    metric_name: str
    status: MetricStatus
    value: float
    threshold_warning: float
    threshold_critical: float
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric": self.metric_name,
            "status": self.status.value,
            "value": self.value,
            "threshold_warning": self.threshold_warning,
            "threshold_critical": self.threshold_critical,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class PipelineReport:
    generated_at: datetime = field(default_factory=datetime.utcnow)
    entries: List[ReportEntry] = field(default_factory=list)

    def add(self, entry: ReportEntry) -> None:
        self.entries.append(entry)

    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {s.value: 0 for s in MetricStatus}
        for e in self.entries:
            counts[e.status.value] += 1
        return counts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "summary": self.summary(),
            "entries": [e.to_dict() for e in self.entries],
        }


def build_report(collector: MetricCollector) -> PipelineReport:
    """Build a health report from the latest metric values in a collector."""
    report = PipelineReport()
    for name, threshold in collector.thresholds.items():
        history = collector.get_history(name)
        if not history:
            continue
        latest: PipelineMetric = history[-1]
        status = evaluate(latest, threshold)
        entry = ReportEntry(
            metric_name=name,
            status=status,
            value=latest.value,
            threshold_warning=threshold.warning,
            threshold_critical=threshold.critical,
            timestamp=latest.timestamp,
        )
        report.add(entry)
    return report
