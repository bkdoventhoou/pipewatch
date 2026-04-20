import pytest
from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.tagging import TaggedMetric, TagRegistry
from datetime import datetime


def make_metric(pipeline="etl", name="row_count", value=100.0, status=MetricStatus.OK):
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status, timestamp=datetime.utcnow())


def test_tagged_metric_has_tag_key_only():
    m = make_metric()
    t = TaggedMetric(metric=m, tags={"env": "prod"})
    assert t.has_tag("env") is True
    assert t.has_tag("team") is False


def test_tagged_metric_has_tag_key_value():
    m = make_metric()
    t = TaggedMetric(metric=m, tags={"env": "prod"})
    assert t.has_tag("env", "prod") is True
    assert t.has_tag("env", "staging") is False


def test_tagged_metric_to_dict_includes_tags():
    m = make_metric()
    t = TaggedMetric(metric=m, tags={"owner": "alice"})
    d = t.to_dict()
    assert "tags" in d
    assert d["tags"]["owner"] == "alice"


def test_registry_tag_and_get():
    reg = TagRegistry()
    reg.tag("etl", "row_count", {"env": "prod"})
    tags = reg.get_tags("etl", "row_count")
    assert tags["env"] == "prod"


def test_registry_get_missing_returns_empty():
    reg = TagRegistry()
    assert reg.get_tags("x", "y") == {}


def test_registry_apply_attaches_tags():
    reg = TagRegistry()
    reg.tag("etl", "row_count", {"team": "data"})
    metrics = [make_metric(pipeline="etl", name="row_count")]
    tagged = reg.apply(metrics)
    assert len(tagged) == 1
    assert tagged[0].tags["team"] == "data"


def test_registry_apply_no_tags_empty_dict():
    reg = TagRegistry()
    metrics = [make_metric()]
    tagged = reg.apply(metrics)
    assert tagged[0].tags == {}


def test_registry_filter_by_tag_key():
    reg = TagRegistry()
    reg.tag("etl", "row_count", {"env": "prod"})
    metrics = [make_metric(pipeline="etl", name="row_count"), make_metric(pipeline="etl", name="error_rate")]
    tagged = reg.apply(metrics)
    filtered = reg.filter_by_tag(tagged, "env")
    assert len(filtered) == 1
    assert filtered[0].metric.name == "row_count"


def test_registry_filter_by_tag_key_value():
    reg = TagRegistry()
    reg.tag("etl", "row_count", {"env": "prod"})
    reg.tag("etl", "error_rate", {"env": "staging"})
    metrics = [make_metric(pipeline="etl", name="row_count"), make_metric(pipeline="etl", name="error_rate")]
    tagged = reg.apply(metrics)
    filtered = reg.filter_by_tag(tagged, "env", "prod")
    assert len(filtered) == 1
    assert filtered[0].metric.name == "row_count"
