"""Tests for pipewatch.baseline."""
import json
import pytest
from pathlib import Path

from pipewatch.baseline import (
    BaselineEntry,
    BaselineComparison,
    load_baseline,
    save_baseline,
    compare_to_baseline,
    build_baseline_from_metrics,
)
from pipewatch.metrics import PipelineMetric, MetricStatus
from datetime import datetime


def make_metric(pipeline, name, value):
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=MetricStatus.OK,
        timestamp=datetime(2024, 1, 1),
    )


def test_build_baseline_from_metrics():
    metrics = [make_metric("etl", "rows", 100.0), make_metric("etl", "errors", 0.0)]
    entries = build_baseline_from_metrics(metrics)
    assert len(entries) == 2
    assert any(e.metric_name == "rows" and e.baseline_value == 100.0 for e in entries)


def test_save_and_load_baseline(tmp_path):
    path = str(tmp_path / "baseline.json")
    entries = [BaselineEntry(pipeline="etl", metric_name="rows", baseline_value=200.0)]
    save_baseline(entries, path)
    loaded = load_baseline(path)
    assert ("etl", "rows") in loaded
    assert loaded[("etl", "rows")].baseline_value == 200.0


def test_compare_to_baseline_delta():
    baseline = {("etl", "rows"): BaselineEntry("etl", "rows", 100.0)}
    metrics = [make_metric("etl", "rows", 120.0)]
    result = compare_to_baseline(metrics, baseline)
    assert len(result) == 1
    assert result[0].delta == pytest.approx(20.0)
    assert result[0].pct_change == pytest.approx(20.0)


def test_compare_to_baseline_negative_delta():
    baseline = {("etl", "rows"): BaselineEntry("etl", "rows", 200.0)}
    metrics = [make_metric("etl", "rows", 150.0)]
    result = compare_to_baseline(metrics, baseline)
    assert result[0].delta == pytest.approx(-50.0)
    assert result[0].pct_change == pytest.approx(-25.0)


def test_compare_skips_missing_baseline():
    baseline = {}
    metrics = [make_metric("etl", "rows", 100.0)]
    result = compare_to_baseline(metrics, baseline)
    assert result == []


def test_compare_zero_baseline_pct_none():
    baseline = {("etl", "rows"): BaselineEntry("etl", "rows", 0.0)}
    metrics = [make_metric("etl", "rows", 5.0)]
    result = compare_to_baseline(metrics, baseline)
    assert result[0].pct_change is None
    assert result[0].delta == pytest.approx(5.0)


def test_to_dict_fields():
    c = BaselineComparison(
        pipeline="p", metric_name="m", current_value=1.0,
        baseline_value=2.0, delta=-1.0, pct_change=-50.0
    )
    d = c.to_dict()
    assert d["pipeline"] == "p"
    assert d["delta"] == -1.0
