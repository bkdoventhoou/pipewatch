"""Alert dispatching for pipeline metric threshold violations."""

from dataclasses import dataclass, field
from typing import Callable, List, Optional
from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class Alert:
    pipeline: str
    metric_name: str
    status: MetricStatus
    value: float
    message: str

    def __str__(self) -> str:
        return (
            f"[{self.status.value.upper()}] {self.pipeline}/{self.metric_name} "
            f"= {self.value} — {self.message}"
        )


AlertHandler = Callable[[Alert], None]


@dataclass
class AlertDispatcher:
    handlers: List[AlertHandler] = field(default_factory=list)
    _last_statuses: dict = field(default_factory=dict, repr=False)

    def register(self, handler: AlertHandler) -> None:
        """Register a callable that receives Alert objects."""
        self.handlers.append(handler)

    def dispatch(self, metric: PipelineMetric) -> Optional[Alert]:
        """Dispatch an alert if metric status is WARNING or CRITICAL."""
        if metric.status == MetricStatus.OK:
            self._last_statuses[metric.name] = MetricStatus.OK
            return None

        prev = self._last_statuses.get(metric.name)
        self._last_statuses[metric.name] = metric.status

        # Suppress duplicate alerts for the same status
        if prev == metric.status:
            return None

        alert = Alert(
            pipeline=metric.pipeline,
            metric_name=metric.name,
            status=metric.status,
            value=metric.value,
            message=(
                f"Value {metric.value} exceeded "
                f"{metric.status.value} threshold."
            ),
        )
        for handler in self.handlers:
            handler(alert)
        return alert
