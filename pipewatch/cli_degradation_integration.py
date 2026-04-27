"""Integration helpers: attach degradation detection to a PipelineWatcher."""

from __future__ import annotations

from typing import Callable, List, Optional

from pipewatch.degradation import DegradationResult, detect_all_degradations
from pipewatch.reporter import PipelineReport
from pipewatch.watcher import PipelineWatcher


def attach_degradation_to_watcher(
    watcher: PipelineWatcher,
    on_degradation: Callable[[DegradationResult], None],
    min_samples: int = 4,
    slope_threshold: float = 0.1,
    only_degrading: bool = True,
) -> None:
    """Register an on_report callback that runs degradation analysis after each tick."""

    def _callback(report: PipelineReport) -> None:
        history = watcher.collector.get_all_history()
        results = detect_all_degradations(
            history,
            min_samples=min_samples,
            slope_threshold=slope_threshold,
        )
        for r in results:
            if only_degrading and not r.degrading:
                continue
            on_degradation(r)

    watcher.on_report = _callback


def build_degradation_summary(
    results: List[DegradationResult],
) -> dict:
    """Return a summary dict suitable for logging or serialisation."""
    degrading = [r for r in results if r.degrading]
    return {
        "total_analysed": len(results),
        "degrading_count": len(degrading),
        "degrading_pipelines": [
            {"pipeline": r.pipeline, "metric": r.metric_name, "slope": r.score_slope}
            for r in degrading
        ],
    }
