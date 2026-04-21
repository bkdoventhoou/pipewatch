"""Tests for pipewatch.labeling."""

from __future__ import annotations

import pytest

from pipewatch.labeling import LabeledMetric, LabelRegistry
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    pipeline: str = "etl",
    name: str = "row_count",
    value: float = 100.0,
    status: MetricStatus = MetricStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


# ---------------------------------------------------------------------------
# LabeledMetric
# ---------------------------------------------------------------------------

def test_has_label_key_only():
    lm = LabeledMetric(metric=make_metric(), labels={"env": "prod"})
    assert lm.has_label("env") is True


def test_has_label_key_value_match():
    lm = LabeledMetric(metric=make_metric(), labels={"env": "prod"})
    assert lm.has_label("env", "prod") is True


def test_has_label_key_value_mismatch():
    lm = LabeledMetric(metric=make_metric(), labels={"env": "prod"})
    assert lm.has_label("env", "staging") is False


def test_has_label_missing_key():
    lm = LabeledMetric(metric=make_metric(), labels={})
    assert lm.has_label("team") is False


def test_get_returns_value():
    lm = LabeledMetric(metric=make_metric(), labels={"team": "data"})
    assert lm.get("team") == "data"


def test_get_returns_none_for_missing():
    lm = LabeledMetric(metric=make_metric(), labels={})
    assert lm.get("team") is None


def test_to_dict_includes_labels():
    lm = LabeledMetric(metric=make_metric(), labels={"env": "prod"})
    d = lm.to_dict()
    assert d["labels"] == {"env": "prod"}
    assert d["pipeline"] == "etl"
    assert d["name"] == "row_count"


# ---------------------------------------------------------------------------
# LabelRegistry
# ---------------------------------------------------------------------------

def test_registry_label_and_all():
    reg = LabelRegistry()
    m = make_metric()
    lm = reg.label(m, env="prod")
    assert lm in reg.all()


def test_registry_query_by_key():
    reg = LabelRegistry()
    reg.label(make_metric(pipeline="a"), env="prod")
    reg.label(make_metric(pipeline="b"), env="staging")
    results = reg.query("env")
    assert len(results) == 2


def test_registry_query_by_key_value():
    reg = LabelRegistry()
    reg.label(make_metric(pipeline="a"), env="prod")
    reg.label(make_metric(pipeline="b"), env="staging")
    results = reg.query("env", "prod")
    assert len(results) == 1
    assert results[0].metric.pipeline == "a"


def test_registry_query_no_match():
    reg = LabelRegistry()
    reg.label(make_metric(), env="prod")
    assert reg.query("team") == []


def test_registry_clear():
    reg = LabelRegistry()
    reg.label(make_metric(), env="prod")
    reg.clear()
    assert reg.all() == []
