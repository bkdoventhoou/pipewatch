"""Simple linear forecast for pipeline metrics based on recent history."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class ForecastResult:
    pipeline: str
    metric_name: str
    horizon: int  # steps ahead
    predicted_value: float
    slope: float
    intercept: float
    based_on: int  # number of data points used

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "horizon": self.horizon,
            "predicted_value": round(self.predicted_value, 6),
            "slope": round(self.slope, 6),
            "intercept": round(self.intercept, 6),
            "based_on": self.based_on,
        }


def _linear_fit(values: List[float]):
    """Return (slope, intercept) for a simple OLS fit over index positions."""
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return 0.0, mean_y
    slope = num / den
    intercept = mean_y - slope * mean_x
    return slope, intercept


def forecast_metric(
    metrics: List[PipelineMetric],
    horizon: int = 1,
    min_points: int = 2,
) -> Optional[ForecastResult]:
    """Forecast a single metric's value `horizon` steps ahead.

    Returns None if there are fewer than `min_points` observations.
    """
    if len(metrics) < min_points:
        return None

    values = [m.value for m in metrics]
    slope, intercept = _linear_fit(values)
    next_x = len(values) - 1 + horizon
    predicted = slope * next_x + intercept

    sample = metrics[0]
    return ForecastResult(
        pipeline=sample.pipeline,
        metric_name=sample.name,
        horizon=horizon,
        predicted_value=predicted,
        slope=slope,
        intercept=intercept,
        based_on=len(values),
    )


def forecast_all(
    history: dict,
    horizon: int = 1,
    min_points: int = 2,
) -> List[ForecastResult]:
    """Run forecast_metric for every (pipeline, metric_name) key in a history dict."""
    results: List[ForecastResult] = []
    for metrics in history.values():
        result = forecast_metric(metrics, horizon=horizon, min_points=min_points)
        if result is not None:
            results.append(result)
    return results
