"""Trend analysis for pipeline metrics over time."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TrendResult:
    pipeline: str
    metric_name: str
    values: List[float]
    direction: str  # 'up', 'down', 'stable'
    slope: float
    avg: float
    min: float
    max: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "direction": self.direction,
            "slope": round(self.slope, 4),
            "avg": round(self.avg, 4),
            "min": self.min,
            "max": self.max,
            "sample_count": len(self.values),
        }


def _compute_slope(values: List[float]) -> float:
    """Compute the least-squares linear regression slope for a sequence of values.

    Returns 0.0 if fewer than two values are provided or if the denominator
    is zero (i.e. all x values are identical, which cannot happen for n >= 2
    with integer indices, but is guarded for safety).
    """
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


def analyze_trend(metrics: List[PipelineMetric], threshold: float = 0.01) -> Optional[TrendResult]:
    """Analyse the trend of a single metric series.

    Args:
        metrics: Ordered list of PipelineMetric samples for one
                 (pipeline, metric_name) pair.
        threshold: Absolute slope value above which the trend is considered
                   'up' or 'down' rather than 'stable'.

    Returns:
        A TrendResult, or None if *metrics* is empty.
    """
    if not metrics:
        return None
    pipeline = metrics[0].pipeline
    metric_name = metrics[0].name
    values = [m.value for m in metrics]
    slope = _compute_slope(values)
    if slope > threshold:
        direction = "up"
    elif slope < -threshold:
        direction = "down"
    else:
        direction = "stable"
    return TrendResult(
        pipeline=pipeline,
        metric_name=metric_name,
        values=values,
        direction=direction,
        slope=slope,
        avg=sum(values) / len(values),
        min=min(values),
        max=max(values),
    )


def analyze_all_trends(history: dict, threshold: float = 0.01) -> List[TrendResult]:
    """Analyse trends for every (pipeline, metric_name) series in *history*.

    Args:
        history: Mapping of ``(pipeline, metric_name)`` tuples to an ordered
                 list of :class:`PipelineMetric` samples.
        threshold: Passed through to :func:`analyze_trend`.

    Returns:
        List of :class:`TrendResult` objects, one per non-empty series.
    """
    results = []
    for metrics in history.values():
        result = analyze_trend(metrics, threshold)
        if result:
            results.append(result)
    return results


def filter_trends_by_direction(results: List[TrendResult], direction: str) -> List[TrendResult]:
    """Return only those trend results whose direction matches *direction*.

    Args:
        results: List of TrendResult objects to filter.
        direction: One of ``'up'``, ``'down'``, or ``'stable'``.

    Returns:
        Filtered list of TrendResult objects.

    Raises:
        ValueError: If *direction* is not a recognised value.
    """
    valid = {"up", "down", "stable"}
    if direction not in valid:
        raise ValueError(f"Invalid direction {direction!r}. Must be one of {valid}.")
    return [r for r in results if r.direction == direction]
