"""Tests for pipewatch.forecast."""

import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.forecast import ForecastResult, forecast_metric, forecast_all, _linear_fit


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=0.0,
    )


# ---------------------------------------------------------------------------
# _linear_fit
# ---------------------------------------------------------------------------

def test_linear_fit_flat_series():
    slope, intercept = _linear_fit([5.0, 5.0, 5.0])
    assert slope == pytest.approx(0.0)
    assert intercept == pytest.approx(5.0)


def test_linear_fit_increasing_series():
    slope, intercept = _linear_fit([1.0, 2.0, 3.0])
    assert slope == pytest.approx(1.0)
    assert intercept == pytest.approx(1.0)


def test_linear_fit_single_value_returns_zero_slope():
    slope, intercept = _linear_fit([7.0])
    assert slope == pytest.approx(0.0)
    assert intercept == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# forecast_metric
# ---------------------------------------------------------------------------

def test_forecast_returns_none_on_too_few_points():
    metrics = [make_metric("pipe", "latency", 1.0)]
    result = forecast_metric(metrics, horizon=1, min_points=2)
    assert result is None


def test_forecast_returns_result_with_correct_type():
    metrics = [
        make_metric("pipe", "latency", 1.0),
        make_metric("pipe", "latency", 2.0),
    ]
    result = forecast_metric(metrics, horizon=1)
    assert isinstance(result, ForecastResult)


def test_forecast_predicts_next_step_increasing():
    metrics = [make_metric("pipe", "latency", float(i)) for i in range(1, 6)]
    result = forecast_metric(metrics, horizon=1)
    assert result is not None
    assert result.predicted_value == pytest.approx(6.0, abs=0.01)


def test_forecast_horizon_two_steps():
    metrics = [make_metric("pipe", "latency", float(i)) for i in range(1, 6)]
    result = forecast_metric(metrics, horizon=2)
    assert result is not None
    assert result.predicted_value == pytest.approx(7.0, abs=0.01)


def test_forecast_flat_series_predicts_same_value():
    metrics = [make_metric("pipe", "rows", 100.0) for _ in range(4)]
    result = forecast_metric(metrics, horizon=3)
    assert result is not None
    assert result.predicted_value == pytest.approx(100.0)


def test_forecast_based_on_count():
    metrics = [make_metric("pipe", "latency", float(i)) for i in range(5)]
    result = forecast_metric(metrics, horizon=1)
    assert result.based_on == 5


def test_forecast_to_dict_keys():
    metrics = [
        make_metric("pipe", "latency", 1.0),
        make_metric("pipe", "latency", 2.0),
    ]
    result = forecast_metric(metrics, horizon=1)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "horizon",
        "predicted_value", "slope", "intercept", "based_on",
    }


# ---------------------------------------------------------------------------
# forecast_all
# ---------------------------------------------------------------------------

def test_forecast_all_returns_one_per_key():
    history = {
        ("pipe_a", "latency"): [make_metric("pipe_a", "latency", float(i)) for i in range(3)],
        ("pipe_b", "rows"): [make_metric("pipe_b", "rows", float(i * 10)) for i in range(3)],
    }
    results = forecast_all(history, horizon=1)
    assert len(results) == 2


def test_forecast_all_skips_insufficient_data():
    history = {
        ("pipe_a", "latency"): [make_metric("pipe_a", "latency", 1.0)],
        ("pipe_b", "rows"): [make_metric("pipe_b", "rows", float(i)) for i in range(3)],
    }
    results = forecast_all(history, horizon=1, min_points=2)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_b"
