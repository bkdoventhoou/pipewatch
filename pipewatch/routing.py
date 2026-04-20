"""Alert routing: direct alerts to specific handlers based on rules."""

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.alerts import Alert


@dataclass
class RoutingRule:
    """A rule that matches alerts and routes them to a named handler."""
    handler_name: str
    pipeline: Optional[str] = None
    metric_name: Optional[str] = None
    min_status: MetricStatus = MetricStatus.WARNING

    def matches(self, alert: Alert) -> bool:
        if alert.metric.status.value < self.min_status.value:
            return False
        if self.pipeline and alert.metric.pipeline != self.pipeline:
            return False
        if self.metric_name and alert.metric.name != self.metric_name:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "handler_name": self.handler_name,
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "min_status": self.min_status.name,
        }


class AlertRouter:
    """Routes alerts to registered handlers based on matching rules."""

    def __init__(self) -> None:
        self._rules: List[RoutingRule] = []
        self._handlers: dict[str, Callable[[Alert], None]] = {}

    def register_handler(self, name: str, handler: Callable[[Alert], None]) -> None:
        self._handlers[name] = handler

    def add_rule(self, rule: RoutingRule) -> None:
        self._rules.append(rule)

    def route(self, alert: Alert) -> List[str]:
        """Dispatch alert to all matching handlers. Returns list of handler names used."""
        dispatched: List[str] = []
        for rule in self._rules:
            if rule.matches(alert):
                handler = self._handlers.get(rule.handler_name)
                if handler:
                    handler(alert)
                    dispatched.append(rule.handler_name)
        return dispatched

    def rules(self) -> List[RoutingRule]:
        return list(self._rules)


def build_router_from_config(config: dict) -> AlertRouter:
    """Build an AlertRouter from a config dict (e.g. loaded from pipewatch.yaml)."""
    router = AlertRouter()
    for entry in config.get("routing", []):
        status_str = entry.get("min_status", "WARNING").upper()
        rule = RoutingRule(
            handler_name=entry["handler"],
            pipeline=entry.get("pipeline"),
            metric_name=entry.get("metric_name"),
            min_status=MetricStatus[status_str],
        )
        router.add_rule(rule)
    return router
