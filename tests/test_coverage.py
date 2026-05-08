"""Tests for pipewatch.coverage."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.coverage import (
    CoverageResult,
    analyze_coverage,
    analyze_all_coverages,
)


def make_metric(
    pipeline: str = "pipe",
    name: str = "metric",
    value: float = 1.0,
    status: MetricStatus = MetricStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value, status=status, timestamp=0.0
    )


# ---------------------------------------------------------------------------
# analyze_coverage
# ---------------------------------------------------------------------------

def test_analyze_coverage_returns_none_on_empty():
    assert analyze_coverage("pipe", []) is None


def test_analyze_coverage_all_ok():
    metrics = [make_metric(status=MetricStatus.OK) for _ in range(4)]
    result = analyze_coverage("pipe", metrics)
    assert result is not None
    assert result.total == 4
    assert result.ok_count == 4
    assert result.coverage_ratio == 1.0
    assert result.healthy is True


def test_analyze_coverage_mixed_statuses():
    metrics = [
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.WARNING),
        make_metric(status=MetricStatus.CRITICAL),
        make_metric(status=MetricStatus.OK),
    ]
    result = analyze_coverage("pipe", metrics)
    assert result.ok_count == 2
    assert result.warning_count == 1
    assert result.critical_count == 1
    assert result.coverage_ratio == pytest.approx(0.5)
    assert result.healthy is False


def test_analyze_coverage_respects_threshold():
    metrics = [
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.OK),
        make_metric(status=MetricStatus.WARNING),
    ]
    # 2/3 ≈ 0.667; threshold 0.5 → healthy
    result = analyze_coverage("pipe", metrics, healthy_threshold=0.5)
    assert result.healthy is True


def test_analyze_coverage_filters_by_pipeline():
    metrics = [
        make_metric(pipeline="pipe", status=MetricStatus.OK),
        make_metric(pipeline="other", status=MetricStatus.CRITICAL),
    ]
    result = analyze_coverage("pipe", metrics)
    assert result.total == 1
    assert result.ok_count == 1


def test_analyze_coverage_to_dict_keys():
    metrics = [make_metric(status=MetricStatus.OK)]
    d = analyze_coverage("pipe", metrics).to_dict()
    for key in ("pipeline", "total", "ok_count", "warning_count",
                "critical_count", "coverage_ratio", "healthy"):
        assert key in d


# ---------------------------------------------------------------------------
# analyze_all_coverages
# ---------------------------------------------------------------------------

def test_analyze_all_coverages_empty_returns_empty():
    assert analyze_all_coverages([]) == {}


def test_analyze_all_coverages_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", status=MetricStatus.OK),
        make_metric(pipeline="b", status=MetricStatus.CRITICAL),
        make_metric(pipeline="a", status=MetricStatus.WARNING),
    ]
    results = analyze_all_coverages(metrics)
    assert set(results.keys()) == {"a", "b"}
    assert results["a"].total == 2
    assert results["b"].total == 1


def test_analyze_all_coverages_unhealthy_pipeline():
    metrics = [
        make_metric(pipeline="p", status=MetricStatus.CRITICAL),
    ]
    results = analyze_all_coverages(metrics)
    assert results["p"].healthy is False
