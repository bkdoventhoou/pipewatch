"""Tests for pipewatch.persistence."""

import json
import os
import tempfile
import time

import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.persistence import (
    PersistenceRecord,
    save_history,
    load_history,
)


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=time.time(),
    )


def test_persistence_record_to_dict():
    rec = PersistenceRecord(
        pipeline="etl", metric_name="row_count", value=100.0, status="ok", timestamp=1.0
    )
    d = rec.to_dict()
    assert d["pipeline"] == "etl"
    assert d["metric_name"] == "row_count"
    assert d["value"] == 100.0
    assert d["status"] == "ok"
    assert d["timestamp"] == 1.0


def test_persistence_record_roundtrip():
    original = PersistenceRecord(
        pipeline="pipe", metric_name="latency", value=42.5, status="warning", timestamp=99.9
    )
    restored = PersistenceRecord.from_dict(original.to_dict())
    assert restored.pipeline == original.pipeline
    assert restored.metric_name == original.metric_name
    assert restored.value == original.value
    assert restored.status == original.status
    assert restored.timestamp == original.timestamp


def test_save_and_load_history():
    m1 = make_metric("etl", "row_count", 200.0, MetricStatus.OK)
    m2 = make_metric("etl", "row_count", 180.0, MetricStatus.WARNING)
    history = {"etl:row_count": [m1, m2]}

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "history.json")
        save_history(history, path)
        assert os.path.exists(path)

        loaded = load_history(path)
        assert "etl:row_count" in loaded
        assert len(loaded["etl:row_count"]) == 2
        values = [m.value for m in loaded["etl:row_count"]]
        assert 200.0 in values
        assert 180.0 in values


def test_load_history_returns_empty_when_file_missing():
    result = load_history("/nonexistent/path/history.json")
    assert result == {}


def test_save_history_multiple_pipelines():
    m1 = make_metric("pipe_a", "errors", 5.0, MetricStatus.CRITICAL)
    m2 = make_metric("pipe_b", "latency", 0.3, MetricStatus.OK)
    history = {"pipe_a:errors": [m1], "pipe_b:latency": [m2]}

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "multi.json")
        save_history(history, path)
        loaded = load_history(path)

    assert "pipe_a:errors" in loaded
    assert "pipe_b:latency" in loaded
    assert loaded["pipe_a:errors"][0].status == MetricStatus.CRITICAL
    assert loaded["pipe_b:latency"][0].status == MetricStatus.OK


def test_saved_file_is_valid_json():
    m = make_metric("p", "m", 1.0, MetricStatus.OK)
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "out.json")
        save_history({"p:m": [m]}, path)
        with open(path) as fh:
            data = json.load(fh)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "p"
