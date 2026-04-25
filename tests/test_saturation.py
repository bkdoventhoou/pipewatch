"""Tests for pipewatch.saturation."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.saturation import (
    SaturationResult,
    compute_saturation,
    analyze_saturation,
)


def make_metric(pipeline: str, name: str, value: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        timestamp=datetime.now(timezone.utc),
        status=MetricStatus.OK,
    )


# ---------------------------------------------------------------------------
# compute_saturation
# ---------------------------------------------------------------------------

def test_compute_saturation_returns_none_for_zero_threshold():
    m = make_metric("pipe", "rows", 50.0)
    assert compute_saturation(m, 0.0) is None


def test_compute_saturation_ok_below_75_pct():
    m = make_metric("pipe", "rows", 50.0)
    result = compute_saturation(m, 100.0)
    assert result is not None
    assert result.saturation_pct == pytest.approx(50.0)
    assert result.status == MetricStatus.OK


def test_compute_saturation_warning_at_75_pct():
    m = make_metric("pipe", "rows", 75.0)
    result = compute_saturation(m, 100.0)
    assert result.status == MetricStatus.WARNING
    assert result.saturation_pct == pytest.approx(75.0)


def test_compute_saturation_critical_at_100_pct():
    m = make_metric("pipe", "rows", 100.0)
    result = compute_saturation(m, 100.0)
    assert result.status == MetricStatus.CRITICAL
    assert result.saturation_pct == pytest.approx(100.0)


def test_compute_saturation_critical_above_100_pct():
    m = make_metric("pipe", "rows", 150.0)
    result = compute_saturation(m, 100.0)
    assert result.status == MetricStatus.CRITICAL
    assert result.saturation_pct == pytest.approx(150.0)


def test_compute_saturation_fields_populated():
    m = make_metric("etl", "lag", 40.0)
    result = compute_saturation(m, 200.0)
    assert result.pipeline == "etl"
    assert result.metric_name == "lag"
    assert result.current_value == 40.0
    assert result.critical_threshold == 200.0


# ---------------------------------------------------------------------------
# analyze_saturation
# ---------------------------------------------------------------------------

def _mock_collector(history_map):
    collector = MagicMock()
    collector.get_history.side_effect = lambda key: history_map.get(key, [])
    return collector


def test_analyze_saturation_returns_results_for_known_keys():
    m = make_metric("pipe", "rows", 80.0)
    collector = _mock_collector({"pipe.rows": [m]})
    results = analyze_saturation(collector, {"pipe.rows": 100.0})
    assert len(results) == 1
    assert results[0].saturation_pct == pytest.approx(80.0)


def test_analyze_saturation_skips_missing_history():
    collector = _mock_collector({})
    results = analyze_saturation(collector, {"pipe.rows": 100.0})
    assert results == []


def test_analyze_saturation_uses_latest_value():
    old = make_metric("pipe", "rows", 10.0)
    new = make_metric("pipe", "rows", 90.0)
    collector = _mock_collector({"pipe.rows": [old, new]})
    results = analyze_saturation(collector, {"pipe.rows": 100.0})
    assert results[0].current_value == 90.0


def test_saturation_to_dict_keys():
    m = make_metric("etl", "lag", 50.0)
    result = compute_saturation(m, 100.0)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "current_value",
        "critical_threshold", "saturation_pct", "status",
    }
