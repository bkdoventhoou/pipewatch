"""Simple interval-based scheduler for running PipelineWatcher on a cron-like schedule."""

import time
import threading
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class SchedulerStop(Exception):
    """Raised to signal the scheduler loop to stop."""


class PipelineScheduler:
    """Runs a callable at a fixed interval in a background thread."""

    def __init__(self, interval_seconds: float, task: Callable[[], None]):
        self.interval = interval_seconds
        self.task = task
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _loop(self) -> None:
        logger.info("Scheduler started (interval=%.1fs)", self.interval)
        while not self._stop_event.is_set():
            try:
                self.task()
            except Exception as exc:  # noqa: BLE001
                logger.error("Scheduler task raised an exception: %s", exc)
            self._stop_event.wait(timeout=self.interval)
        logger.info("Scheduler stopped")

    def start(self) -> None:
        """Start the scheduler in a daemon background thread."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Scheduler is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the scheduler to stop and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())
