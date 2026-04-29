"""Tests for pipewatch.cycle."""
from __future__ import annotations

import math
from typing import List
from unittest.mock import MagicMock

import pytest

from pipewatch.cycle import (
    CycleResult,
    _autocorrelation,
    detect_all_cycles,
    detect_cycle,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, ts: float = 0.0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        metric_name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=ts,
    )


def make_series(values: List[float], pipeline="p", name="m") -> List[PipelineMetric]:
    return [make_metric(pipeline, name, v, float(i)) for i, v in enumerate(values)]


# --- _autocorrelation ---

def test_autocorrelation_perfect_lag1():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    r = _autocorrelation(values, 1)
    assert r is not None
    assert abs(r - 1.0) < 1e-9


def test_autocorrelation_lag_too_large_returns_none():
    values = [1.0, 2.0]
    assert _autocorrelation(values, 5) is None


def test_autocorrelation_zero_variance_returns_none():
    values = [3.0, 3.0, 3.0, 3.0]
    assert _autocorrelation(values, 1) is None


# --- detect_cycle ---

def test_detect_cycle_returns_none_on_too_few():
    series = make_series([1.0, 2.0, 3.0])
    result = detect_cycle(series, min_samples=8)
    assert result is None


def test_detect_cycle_periodic_signal_detected():
    # sin-like repeating pattern with period 4
    period = 4
    raw = [math.sin(2 * math.pi * i / period) for i in range(32)]
    series = make_series(raw)
    result = detect_cycle(series, min_samples=8, confidence_threshold=0.7)
    assert result is not None
    assert result.is_cyclic is True
    assert result.period == period
    assert result.confidence >= 0.7


def test_detect_cycle_random_walk_not_cyclic():
    # Strictly increasing series — no periodicity
    series = make_series(list(range(20)))
    result = detect_cycle(series, min_samples=8, confidence_threshold=0.9)
    assert result is not None
    assert result.is_cyclic is False


def test_detect_cycle_to_dict_keys():
    series = make_series([1.0] * 10)
    result = detect_cycle(series, min_samples=8)
    assert result is not None
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "metric_name", "period", "confidence", "is_cyclic"}


# --- detect_all_cycles ---

def test_detect_all_cycles_skips_too_short():
    collector = MagicMock()
    collector.get_history.return_value = {
        "p:m": make_series([1.0, 2.0])  # too few
    }
    results = detect_all_cycles(collector, min_samples=8)
    assert results == {}


def test_detect_all_cycles_returns_result_for_valid_series():
    period = 4
    raw = [math.sin(2 * math.pi * i / period) for i in range(32)]
    collector = MagicMock()
    collector.get_history.return_value = {"p:m": make_series(raw)}
    results = detect_all_cycles(collector, min_samples=8, confidence_threshold=0.7)
    assert "p:m" in results
    assert isinstance(results["p:m"], CycleResult)
