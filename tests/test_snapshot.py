"""Tests for pipewatch.snapshot."""
import json
import tempfile
from pathlib import Path

import pytest

from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.snapshot import (
    PipelineSnapshot,
    SnapshotEntry,
    capture_snapshot,
    diff_snapshots,
    load_snapshot,
    save_snapshot,
)


def make_metric(pipeline="etl", name="row_count", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


def test_capture_snapshot_entries():
    metrics = [make_metric(), make_metric(name="latency", value=0.5)]
    snap = capture_snapshot(metrics)
    assert len(snap.entries) == 2
    assert snap.entries[0].pipeline == "etl"
    assert snap.entries[0].metric_name == "row_count"
    assert snap.entries[0].value == 100.0
    assert snap.entries[0].status == "ok"


def test_capture_snapshot_has_timestamp():
    snap = capture_snapshot([])
    assert snap.captured_at is not None
    assert "T" in snap.captured_at  # ISO format


def test_save_and_load_snapshot():
    metrics = [make_metric(value=42.0, status=MetricStatus.WARNING)]
    snap = capture_snapshot(metrics)
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    save_snapshot(snap, path)
    loaded = load_snapshot(path)
    assert loaded.captured_at == snap.captured_at
    assert len(loaded.entries) == 1
    assert loaded.entries[0].value == 42.0
    assert loaded.entries[0].status == "warning"


def test_diff_no_changes():
    m = [make_metric()]
    old = capture_snapshot(m)
    new = capture_snapshot(m)
    assert diff_snapshots(old, new) == []


def test_diff_detects_status_change():
    old = capture_snapshot([make_metric(value=100.0, status=MetricStatus.OK)])
    new = capture_snapshot([make_metric(value=5.0, status=MetricStatus.CRITICAL)])
    diffs = diff_snapshots(old, new)
    assert len(diffs) == 1
    assert diffs[0]["old_status"] == "ok"
    assert diffs[0]["new_status"] == "critical"
    assert diffs[0]["old_value"] == 100.0
    assert diffs[0]["new_value"] == 5.0


def test_diff_ignores_new_entries():
    old = capture_snapshot([make_metric(name="row_count")])
    new = capture_snapshot([
        make_metric(name="row_count"),
        make_metric(name="latency", status=MetricStatus.CRITICAL),
    ])
    diffs = diff_snapshots(old, new)
    assert diffs == []


def test_snapshot_to_dict():
    snap = capture_snapshot([make_metric()])
    d = snap.to_dict()
    assert "captured_at" in d
    assert isinstance(d["entries"], list)
    assert d["entries"][0]["metric_name"] == "row_count"
