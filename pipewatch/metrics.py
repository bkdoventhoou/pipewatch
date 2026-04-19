"""Core metric models for pipeline health tracking."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class MetricStatus(Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineMetric:
    """Represents a single pipeline health metric snapshot."""
    pipeline_name: str
    metric_name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    status: MetricStatus = MetricStatus.UNKNOWN
    unit: Optional[str] = None
    tags: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "metric_name": self.metric_name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "unit": self.unit,
            "tags": self.tags,
        }


@dataclass
class ThresholdConfig:
    """Warning and critical thresholds for a metric."""
    metric_name: str
    warning: float
    critical: float
    comparison: str = "gt"  # gt, lt, gte, lte

    def evaluate(self, value: float) -> MetricStatus:
        ops = {
            "gt": lambda a, b: a > b,
            "lt": lambda a, b: a < b,
            "gte": lambda a, b: a >= b,
            "lte": lambda a, b: a <= b,
        }
        compare = ops.get(self.comparison, ops["gt"])
        if compare(value, self.critical):
            return MetricStatus.CRITICAL
        if compare(value, self.warning):
            return MetricStatus.WARNING
        return MetricStatus.OK
