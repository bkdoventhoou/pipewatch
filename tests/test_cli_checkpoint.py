"""Tests for pipewatch.cli_checkpoint."""

from __future__ import annotations

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.checkpoint import CheckpointEntry, CheckpointStore, save_checkpoint_store
from pipewatch.cli_checkpoint import main, parse_args
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(pipeline, name, value, status):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status, timestamp=time.time())


def _mock_collector(pipelines_metrics: dict):
    collector = MagicMock()
    collector.history = {p: v for p, v in pipelines_metrics.items()}
    collector.get_history = lambda p: pipelines_metrics.get(p, [])
    return collector


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.action == "show"
    assert args.fmt == "text"
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--action", "record", "--pipeline", "etl", "--run-id", "abc", "--format", "json"])
    assert args.action == "record"
    assert args.pipeline == "etl"
    assert args.run_id == "abc"
    assert args.fmt == "json"


def test_main_show_text_output(capsys):
    store = CheckpointStore()
    store.record(CheckpointEntry("pipe_a", "r1", 1000.0, 3, 2, 1, 0))

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        cp_path = f.name
    try:
        save_checkpoint_store(store, cp_path)
        with patch("pipewatch.cli_checkpoint.load_config"), \
             patch("pipewatch.cli_checkpoint.build_collector_from_config",
                   return_value=_mock_collector({})):
            main(["--checkpoint-file", cp_path, "--action", "show"])
        out = capsys.readouterr().out
        assert "pipe_a" in out
        assert "r1" in out
    finally:
        os.unlink(cp_path)


def test_main_show_json_output(capsys):
    store = CheckpointStore()
    store.record(CheckpointEntry("pipe_b", "r2", 2000.0, 2, 2, 0, 0))

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        cp_path = f.name
    try:
        save_checkpoint_store(store, cp_path)
        with patch("pipewatch.cli_checkpoint.load_config"), \
             patch("pipewatch.cli_checkpoint.build_collector_from_config",
                   return_value=_mock_collector({})):
            main(["--checkpoint-file", cp_path, "--action", "show", "--format", "json"])
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe_b"
    finally:
        os.unlink(cp_path)


def test_main_record_creates_entry(capsys):
    metrics = [make_metric("etl", "rows", 100, MetricStatus.OK)]
    collector = _mock_collector({"etl": metrics})

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        cp_path = f.name
    os.unlink(cp_path)  # ensure file does not exist so store starts fresh
    try:
        with patch("pipewatch.cli_checkpoint.load_config"), \
             patch("pipewatch.cli_checkpoint.build_collector_from_config", return_value=collector):
            main(["--checkpoint-file", cp_path, "--action", "record", "--run-id", "test-run"])
        out = capsys.readouterr().out
        assert "test-run" in out
        # verify file was written
        data = json.loads(open(cp_path).read())
        assert "etl" in data
    finally:
        if os.path.exists(cp_path):
            os.unlink(cp_path)
