"""Periodic watcher that polls metrics and dispatches alerts."""

import time
import logging
from typing import Callable, Optional

from pipewatch.collector import MetricCollector
from pipewatch.reporter import PipelineReport, build_report
from pipewatch.alerts import AlertDispatcher
from pipewatch.metrics import MetricStatus

logger = logging.getLogger(__name__)


class PipelineWatcher:
    """Polls a MetricCollector at a fixed interval and dispatches alerts."""

    def __init__(
        self,
        collector: MetricCollector,
        dispatcher: AlertDispatcher,
        interval: float = 60.0,
        on_report: Optional[Callable[[PipelineReport], None]] = None,
    ):
        self.collector = collector
        self.dispatcher = dispatcher
        self.interval = interval
        self.on_report = on_report
        self._running = False

    def _tick(self) -> PipelineReport:
        report = build_report(self.collector)
        for entry in report.entries:
            if entry.status != MetricStatus.OK:
                self.dispatcher.dispatch(entry.metric)
        if self.on_report:
            self.on_report(report)
        return report

    def run_once(self) -> PipelineReport:
        """Execute a single watch cycle."""
        logger.debug("Running single watch cycle")
        return self._tick()

    def start(self, max_ticks: Optional[int] = None) -> None:
        """Start the watch loop. Runs until stopped or max_ticks reached."""
        self._running = True
        ticks = 0
        logger.info("PipelineWatcher started (interval=%.1fs)", self.interval)
        while self._running:
            try:
                self._tick()
            except Exception as exc:  # pragma: no cover
                logger.error("Error during watch tick: %s", exc)
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                break
            time.sleep(self.interval)
        logger.info("PipelineWatcher stopped")

    def stop(self) -> None:
        """Signal the watch loop to stop."""
        self._running = False
