"""Tests for pipewatch.retention."""
from datetime import datetime, timedelta
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.collector import MetricCollector
from pipewatch.retention import RetentionPolicy, prune_by_age, prune_by_count, apply_retention


def make_metric(pipeline: str, name: str, value: float, age_seconds: float = 0) -> PipelineMetric:
    ts = datetime.utcnow() - timedelta(seconds=age_seconds)
    return PipelineMetric(pipeline=pipeline, name=name, value=value,
                          status=MetricStatus.OK, timestamp=ts)


def make_collector(*metrics: PipelineMetric) -> MetricCollector:
    c = MetricCollector()
    for m in metrics:
        key = (m.pipeline, m.name)
        c._history.setdefault(key, []).append(m)
    return c


def test_prune_by_age_removes_old():
    old = make_metric("etl", "rows", 10, age_seconds=120)
    new = make_metric("etl", "rows", 20, age_seconds=5)
    c = make_collector(old, new)
    removed = prune_by_age(c, max_age_seconds=60)
    assert removed == 1
    history = c._history[("etl", "rows")]
    assert len(history) == 1
    assert history[0].value == 20


def test_prune_by_age_removes_key_when_empty():
    old = make_metric("etl", "rows", 10, age_seconds=200)
    c = make_collector(old)
    prune_by_age(c, max_age_seconds=60)
    assert ("etl", "rows") not in c._history


def test_prune_by_age_keeps_all_when_fresh():
    m1 = make_metric("etl", "rows", 1, age_seconds=10)
    m2 = make_metric("etl", "rows", 2, age_seconds=20)
    c = make_collector(m1, m2)
    removed = prune_by_age(c, max_age_seconds=60)
    assert removed == 0
    assert len(c._history[("etl", "rows")]) == 2


def test_prune_by_count_keeps_latest():
    metrics = [make_metric("etl", "rows", float(i)) for i in range(5)]
    c = make_collector(*metrics)
    removed = prune_by_count(c, max_entries=3)
    assert removed == 2
    history = c._history[("etl", "rows")]
    assert len(history) == 3
    assert history[-1].value == 4.0


def test_prune_by_count_no_op_when_within_limit():
    metrics = [make_metric("etl", "rows", float(i)) for i in range(3)]
    c = make_collector(*metrics)
    removed = prune_by_count(c, max_entries=5)
    assert removed == 0


def test_apply_retention_combines_both():
    old = make_metric("etl", "rows", 1, age_seconds=200)
    recent = [make_metric("etl", "rows", float(i), age_seconds=1) for i in range(5)]
    c = make_collector(old, *recent)
    policy = RetentionPolicy(max_age_seconds=60, max_entries=3)
    removed = apply_retention(c, policy)
    assert removed == 3  # 1 by age, 2 by count
    assert len(c._history[("etl", "rows")]) == 3


def test_retention_policy_to_dict():
    p = RetentionPolicy(max_age_seconds=300, max_entries=10)
    d = p.to_dict()
    assert d["max_age_seconds"] == 300
    assert d["max_entries"] == 10
