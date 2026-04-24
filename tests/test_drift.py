"""Tests for pipewatch.drift module."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.drift import detect_drift, detect_all_drifts, DriftResult


def make_metric(value: float, pipeline: str = "pipe", name: str = "latency") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime.now(timezone.utc),
    )


def make_series(values: List[float], **kwargs) -> List[PipelineMetric]:
    return [make_metric(v, **kwargs) for v in values]


# ---------------------------------------------------------------------------
# detect_drift
# ---------------------------------------------------------------------------

def test_detect_drift_returns_none_on_too_few_points():
    metrics = make_series([1.0] * 10)  # need 25 by default
    assert detect_drift(metrics) is None


def test_detect_drift_no_drift_when_stable():
    metrics = make_series([10.0] * 25)
    result = detect_drift(metrics, baseline_n=20, recent_n=5, threshold_pct=20.0)
    assert result is not None
    assert result.drifted is False
    assert abs(result.drift_pct) < 1e-6


def test_detect_drift_detects_increase():
    baseline = [10.0] * 20
    recent = [15.0] * 5        # 50 % increase
    metrics = make_series(baseline + recent)
    result = detect_drift(metrics, baseline_n=20, recent_n=5, threshold_pct=20.0)
    assert result is not None
    assert result.drifted is True
    assert pytest.approx(result.drift_pct, rel=1e-3) == 50.0


def test_detect_drift_detects_decrease():
    baseline = [10.0] * 20
    recent = [7.0] * 5         # -30 % change
    metrics = make_series(baseline + recent)
    result = detect_drift(metrics, baseline_n=20, recent_n=5, threshold_pct=20.0)
    assert result is not None
    assert result.drifted is True
    assert result.drift_pct < 0


def test_detect_drift_not_triggered_below_threshold():
    baseline = [10.0] * 20
    recent = [10.5] * 5        # only 5 % change
    metrics = make_series(baseline + recent)
    result = detect_drift(metrics, baseline_n=20, recent_n=5, threshold_pct=20.0)
    assert result is not None
    assert result.drifted is False


def test_detect_drift_result_fields():
    metrics = make_series([5.0] * 20 + [10.0] * 5, pipeline="etl", name="rows")
    result = detect_drift(metrics, baseline_n=20, recent_n=5, threshold_pct=10.0)
    assert result.pipeline == "etl"
    assert result.metric_name == "rows"
    assert result.baseline_avg == pytest.approx(5.0)
    assert result.recent_avg == pytest.approx(10.0)


def test_detect_drift_to_dict_keys():
    metrics = make_series([1.0] * 25)
    result = detect_drift(metrics)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "metric_name", "baseline_avg",
        "recent_avg", "drift_pct", "drifted", "threshold_pct",
    }


# ---------------------------------------------------------------------------
# detect_all_drifts
# ---------------------------------------------------------------------------

def test_detect_all_drifts_skips_short_series():
    history = {
        ("pipe", "latency"): make_series([1.0] * 5),
    }
    results = detect_all_drifts(history)
    assert results == []


def test_detect_all_drifts_returns_result_per_key():
    history = {
        ("pipe", "latency"): make_series([10.0] * 20 + [20.0] * 5),
        ("pipe", "rows"): make_series([100.0] * 20 + [100.0] * 5),
    }
    results = detect_all_drifts(history, baseline_n=20, recent_n=5, threshold_pct=20.0)
    assert len(results) == 2
    drifted = [r for r in results if r.drifted]
    assert len(drifted) == 1
    assert drifted[0].metric_name == "latency"
