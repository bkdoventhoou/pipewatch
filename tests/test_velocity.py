"""Tests for pipewatch.velocity."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.velocity import (
    VelocityResult,
    compute_all_velocities,
    compute_velocity,
)

_BASE = datetime(2024, 1, 1, 12, 0, 0)


def make_metric(
    value: float,
    offset_seconds: float = 0,
    pipeline: str = "pipe_a",
    name: str = "row_count",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=_BASE + timedelta(seconds=offset_seconds),
    )


def test_compute_velocity_returns_none_on_single_metric():
    result = compute_velocity([make_metric(10.0)])
    assert result is None


def test_compute_velocity_returns_none_on_empty():
    result = compute_velocity([])
    assert result is None


def test_compute_velocity_returns_none_on_zero_span():
    m = make_metric(5.0)
    result = compute_velocity([m, m])
    assert result is None


def test_compute_velocity_rising():
    metrics = [make_metric(0.0, 0), make_metric(100.0, 10)]
    result = compute_velocity(metrics)
    assert result is not None
    assert result.velocity == pytest.approx(10.0)
    assert result.direction == "rising"
    assert result.sample_count == 2
    assert result.span_seconds == pytest.approx(10.0)


def test_compute_velocity_falling():
    metrics = [make_metric(200.0, 0), make_metric(100.0, 50)]
    result = compute_velocity(metrics)
    assert result is not None
    assert result.velocity == pytest.approx(-2.0)
    assert result.direction == "falling"


def test_compute_velocity_stable():
    metrics = [make_metric(42.0, 0), make_metric(42.0, 60)]
    result = compute_velocity(metrics)
    assert result is not None
    assert result.velocity == pytest.approx(0.0)
    assert result.direction == "stable"


def test_compute_velocity_unsorted_input_sorted_internally():
    metrics = [make_metric(100.0, 10), make_metric(0.0, 0)]
    result = compute_velocity(metrics)
    assert result is not None
    assert result.velocity == pytest.approx(10.0)
    assert result.direction == "rising"


def test_compute_all_velocities_aggregates_keys():
    history = {
        "pipe_a:row_count": [make_metric(0.0, 0), make_metric(60.0, 60)],
        "pipe_b:error_rate": [
            make_metric(1.0, 0, pipeline="pipe_b", name="error_rate"),
            make_metric(1.0, 30, pipeline="pipe_b", name="error_rate"),
        ],
    }
    results = compute_all_velocities(history)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert "pipe_a" in pipelines
    assert "pipe_b" in pipelines


def test_to_dict_keys():
    metrics = [make_metric(0.0, 0), make_metric(50.0, 5)]
    result = compute_velocity(metrics)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline",
        "metric_name",
        "velocity",
        "direction",
        "sample_count",
        "span_seconds",
    }
