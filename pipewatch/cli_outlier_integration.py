"""Integration helper: attach outlier detection to a PipelineWatcher run."""
from __future__ import annotations

from typing import Callable, List, Optional

from pipewatch.outlier import OutlierResult, detect_all_outliers
from pipewatch.reporter import PipelineReport
from pipewatch.watcher import PipelineWatcher


def attach_outlier_detection(
    watcher: PipelineWatcher,
    multiplier: float = 1.5,
    on_outlier: Optional[Callable[[OutlierResult], None]] = None,
) -> None:
    """Register a post-report callback that runs IQR outlier detection.

    Args:
        watcher:     The PipelineWatcher instance to hook into.
        multiplier:  IQR fence multiplier passed to detect_all_outliers.
        on_outlier:  Optional callback invoked for each detected outlier.
                     Defaults to printing a warning to stdout.
    """
    if on_outlier is None:
        def on_outlier(result: OutlierResult) -> None:  # type: ignore[misc]
            print(
                f"[OUTLIER] {result.pipeline}/{result.metric_name} "
                f"value={result.value:.4f} outside "
                f"[{result.lower_fence:.4f}, {result.upper_fence:.4f}]"
            )

    original_callback: Optional[Callable[[PipelineReport], None]] = getattr(
        watcher, "_on_report", None
    )

    def _combined_callback(report: PipelineReport) -> None:
        if original_callback is not None:
            original_callback(report)
        history = watcher.collector.get_history()
        outliers = detect_all_outliers(history, multiplier=multiplier)
        for result in outliers:
            if result.is_outlier:
                on_outlier(result)

    watcher._on_report = _combined_callback  # type: ignore[attr-defined]


def build_outlier_summary(history: dict, multiplier: float = 1.5) -> dict:
    """Return a JSON-serialisable summary of outlier detection results."""
    results = detect_all_outliers(history, multiplier=multiplier)
    return {
        "total_series": len(results),
        "outlier_count": sum(1 for r in results if r.is_outlier),
        "entries": [r.to_dict() for r in results],
    }
