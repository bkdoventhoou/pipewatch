"""Integration helper: attach momentum analysis as a watcher callback."""
from __future__ import annotations

from typing import Callable, List, Optional

from pipewatch.momentum import MomentumResult, analyze_all_momentum
from pipewatch.watcher import PipelineWatcher


def attach_momentum_to_watcher(
    watcher: PipelineWatcher,
    accel_threshold: float = 0.01,
    on_accelerating: Optional[Callable[[MomentumResult], None]] = None,
) -> None:
    """Register an on_report callback that runs momentum analysis after each tick.

    If *on_accelerating* is provided it is called for every result whose
    ``accelerating`` flag is True.
    """

    def _callback(report) -> None:  # report is a PipelineReport
        history = watcher.collector.get_history()
        results = analyze_all_momentum(history, accel_threshold=accel_threshold)
        if on_accelerating is None:
            return
        for r in results:
            if r.accelerating:
                on_accelerating(r)

    watcher.on_report = _callback


def build_momentum_summary(results: List[MomentumResult]) -> dict:
    """Return a lightweight summary dict suitable for logging or export."""
    total = len(results)
    accelerating = [r for r in results if r.accelerating]
    return {
        "total_analyzed": total,
        "accelerating_count": len(accelerating),
        "stable_count": total - len(accelerating),
        "accelerating_pipelines": [
            {"pipeline": r.pipeline, "metric": r.metric_name, "accel": round(r.second_derivative, 6)}
            for r in accelerating
        ],
    }
