"""Tests for pipewatch.checkpoint module."""

from __future__ import annotations

import json
import os
import tempfile
import time

import pytest

from pipewatch.checkpoint import (
    CheckpointEntry,
    CheckpointStore,
    build_checkpoint,
    load_checkpoint_store,
    save_checkpoint_store,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline: str, name: str, value: float, status: MetricStatus) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status, timestamp=time.time())


def test_build_checkpoint_counts_statuses():
    metrics = [
        make_metric("etl", "rows", 100, MetricStatus.OK),
        make_metric("etl", "errors", 5, MetricStatus.WARNING),
        make_metric("etl", "lag", 99, MetricStatus.CRITICAL),
    ]
    entry = build_checkpoint("etl", "run-1", metrics)
    assert entry.pipeline == "etl"
    assert entry.run_id == "run-1"
    assert entry.metric_count == 3
    assert entry.ok_count == 1
    assert entry.warning_count == 1
    assert entry.critical_count == 1


def test_build_checkpoint_empty_metrics():
    entry = build_checkpoint("etl", "run-0", [])
    assert entry.metric_count == 0
    assert entry.ok_count == 0


def test_store_record_and_latest():
    store = CheckpointStore()
    e1 = CheckpointEntry("pipe", "r1", 1000.0, 2, 2, 0, 0)
    e2 = CheckpointEntry("pipe", "r2", 2000.0, 3, 1, 1, 1)
    store.record(e1)
    store.record(e2)
    assert store.latest("pipe") is e2


def test_store_latest_returns_none_for_unknown_pipeline():
    store = CheckpointStore()
    assert store.latest("nonexistent") is None


def test_store_history_returns_all_entries():
    store = CheckpointStore()
    for i in range(3):
        store.record(CheckpointEntry("p", f"r{i}", float(i), 1, 1, 0, 0))
    assert len(store.history("p")) == 3


def test_store_history_empty_for_unknown():
    store = CheckpointStore()
    assert store.history("missing") == []


def test_save_and_load_roundtrip():
    store = CheckpointStore()
    store.record(CheckpointEntry("alpha", "r1", 12345.0, 5, 3, 1, 1))
    store.record(CheckpointEntry("beta", "r2", 99999.0, 2, 2, 0, 0))

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    try:
        save_checkpoint_store(store, path)
        loaded = load_checkpoint_store(path)
        assert "alpha" in loaded.entries
        assert "beta" in loaded.entries
        assert loaded.latest("alpha").run_id == "r1"
        assert loaded.latest("beta").ok_count == 2
    finally:
        os.unlink(path)


def test_checkpoint_to_dict_keys():
    entry = CheckpointEntry("p", "r", 0.0, 1, 1, 0, 0)
    d = entry.to_dict()
    assert set(d.keys()) == {
        "pipeline", "run_id", "timestamp",
        "metric_count", "ok_count", "warning_count", "critical_count",
    }
