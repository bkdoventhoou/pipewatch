"""Tests for pipewatch.heatmap."""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from pipewatch.heatmap import build_heatmap, HeatmapCell, _hour_bucket
from pipewatch.metrics import PipelineMetric, MetricStatus


BASE_TS = datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp()
NEXT_HOUR_TS = datetime(2024, 1, 15, 15, 5, 0, tzinfo=timezone.utc).timestamp()


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus, ts: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=ts,
    )


def test_hour_bucket_formats_correctly():
    bucket = _hour_bucket(BASE_TS)
    assert bucket == "2024-01-15T14"


def test_build_heatmap_empty_history():
    cells = build_heatmap({})
    assert cells == []


def test_build_heatmap_single_ok_metric():
    history = {
        "etl": [make_metric("etl", "rows", 100.0, MetricStatus.OK, BASE_TS)]
    }
    cells = build_heatmap(history)
    assert len(cells) == 1
    assert cells[0].pipeline == "etl"
    assert cells[0].ok == 1
    assert cells[0].warning == 0
    assert cells[0].critical == 0
    assert cells[0].dominant_status == "ok"


def test_build_heatmap_counts_statuses():
    history = {
        "etl": [
            make_metric("etl", "rows", 100.0, MetricStatus.OK, BASE_TS),
            make_metric("etl", "rows", 50.0, MetricStatus.WARNING, BASE_TS),
            make_metric("etl", "rows", 10.0, MetricStatus.CRITICAL, BASE_TS),
        ]
    }
    cells = build_heatmap(history)
    assert len(cells) == 1
    cell = cells[0]
    assert cell.ok == 1
    assert cell.warning == 1
    assert cell.critical == 1
    assert cell.total == 3
    assert cell.dominant_status == "critical"


def test_build_heatmap_two_hour_buckets():
    history = {
        "etl": [
            make_metric("etl", "rows", 100.0, MetricStatus.OK, BASE_TS),
            make_metric("etl", "rows", 90.0, MetricStatus.WARNING, NEXT_HOUR_TS),
        ]
    }
    cells = build_heatmap(history)
    assert len(cells) == 2
    buckets = [c.bucket for c in cells]
    assert "2024-01-15T14" in buckets
    assert "2024-01-15T15" in buckets


def test_build_heatmap_pipeline_filter():
    history = {
        "etl": [make_metric("etl", "rows", 100.0, MetricStatus.OK, BASE_TS)],
        "loader": [make_metric("loader", "lag", 5.0, MetricStatus.WARNING, BASE_TS)],
    }
    cells = build_heatmap(history, pipeline="etl")
    assert all(c.pipeline == "etl" for c in cells)
    assert len(cells) == 1


def test_heatmap_cell_to_dict_keys():
    cell = HeatmapCell(bucket="2024-01-15T14", pipeline="etl", ok=3, warning=1, critical=0)
    d = cell.to_dict()
    assert set(d.keys()) == {"bucket", "pipeline", "ok", "warning", "critical", "total", "dominant_status"}
    assert d["dominant_status"] == "warning"
    assert d["total"] == 4


def test_build_heatmap_sorted_output():
    history = {
        "z_pipe": [make_metric("z_pipe", "m", 1.0, MetricStatus.OK, BASE_TS)],
        "a_pipe": [make_metric("a_pipe", "m", 1.0, MetricStatus.OK, BASE_TS)],
    }
    cells = build_heatmap(history)
    pipelines = [c.pipeline for c in cells]
    assert pipelines == sorted(pipelines)
