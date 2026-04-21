"""Notification channel abstraction for pipewatch alerts."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class NotificationChannel:
    """Represents a named output channel for alert notifications."""

    name: str
    handler: Callable[[Alert], None]
    min_status: str = "warning"  # "warning" or "critical"
    pipelines: Optional[List[str]] = None  # None means all pipelines

    def accepts(self, alert: Alert) -> bool:
        """Return True if this channel should handle the given alert."""
        status_rank = {"ok": 0, "warning": 1, "critical": 2}
        alert_rank = status_rank.get(alert.metric.status.value.lower(), 0)
        min_rank = status_rank.get(self.min_status.lower(), 1)
        if alert_rank < min_rank:
            return False
        if self.pipelines is not None:
            return alert.metric.pipeline in self.pipelines
        return True

    def send(self, alert: Alert) -> None:
        """Dispatch alert to the handler if accepted."""
        if self.accepts(alert):
            self.handler(alert)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "min_status": self.min_status,
            "pipelines": self.pipelines,
        }


class NotificationManager:
    """Manages multiple notification channels and routes alerts to them."""

    def __init__(self) -> None:
        self._channels: List[NotificationChannel] = []

    def register(self, channel: NotificationChannel) -> None:
        """Register a notification channel."""
        self._channels.append(channel)

    def notify(self, alert: Alert) -> List[str]:
        """Send alert to all accepting channels. Returns list of channel names notified."""
        notified: List[str] = []
        for channel in self._channels:
            if channel.accepts(alert):
                channel.send(alert)
                notified.append(channel.name)
        return notified

    def channel_names(self) -> List[str]:
        return [c.name for c in self._channels]

    def get_channel(self, name: str) -> Optional[NotificationChannel]:
        for c in self._channels:
            if c.name == name:
                return c
        return None
