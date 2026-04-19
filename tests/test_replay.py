import pytest
from unittest.mock import patch, MagicMock
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.snapshot import PipelineSnapshot, SnapshotEntry, capture_snapshot
from pipewatch.replay import ReplaySession, ReplayFrame, load_replay_session, replay_to_collector
from pipewatch.collector import MetricCollector
from pipewatch.metrics import ThresholdConfig
import time


def make_metric(pipeline="pipe", name="rows", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status, timestamp=time.time())


def make_snapshot(metrics):
    entries = [SnapshotEntry(metric=m) for m in metrics]
    snap = PipelineSnapshot(timestamp=time.time(), entries=entries)
    return snap


def test_replay_session_add_and_len():
    session = ReplaySession()
    snap = make_snapshot([make_metric()])
    session.add(snap)
    assert len(session) == 1


def test_replay_session_get_valid():
    session = ReplaySession()
    snap = make_snapshot([make_metric()])
    session.add(snap)
    frame = session.get(0)
    assert frame is not None
    assert frame.index == 0


def test_replay_session_get_out_of_bounds():
    session = ReplaySession()
    assert session.get(5) is None


def test_replay_frame_to_dict():
    snap = make_snapshot([make_metric()])
    frame = ReplayFrame(index=0, snapshot=snap)
    d = frame.to_dict()
    assert d["index"] == 0
    assert "snapshot" in d


def test_replay_to_collector_injects_metrics():
    session = ReplaySession()
    m = make_metric(value=42.0)
    snap = make_snapshot([m])
    session.add(snap)

    collector = MetricCollector()
    collector.add_threshold("pipe", "rows", ThresholdConfig(warning=50, critical=10))
    replay_to_collector(session, 0, collector)
    history = collector.get_history("pipe", "rows")
    assert len(history) == 1
    assert history[0].value == 42.0


def test_replay_to_collector_invalid_frame():
    session = ReplaySession()
    collector = MetricCollector()
    with pytest.raises(IndexError):
        replay_to_collector(session, 0, collector)


def test_load_replay_session_calls_load_snapshot():
    snap = make_snapshot([make_metric()])
    with patch("pipewatch.replay.load_snapshot", return_value=snap) as mock_load:
        session = load_replay_session(["a.json", "b.json"])
        assert mock_load.call_count == 2
        assert len(session) == 2
