"""Tests for pipewatch.silencing."""

import time
import pytest

from pipewatch.metrics import PipelineMetric, MetricStatus
from pipewatch.silencing import SilenceRule, SilenceRegistry


def make_metric(pipeline="etl_main", name="row_count", value=100.0,
                status=MetricStatus.OK) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        name=name,
        value=value,
        status=status,
        timestamp=time.time(),
    )


def test_rule_never_expires_is_always_active():
    rule = SilenceRule(pipeline=None, metric_name=None, expires_at=None)
    assert rule.is_active() is True
    assert rule.is_active(now=time.time() + 9999) is True


def test_rule_expires_in_past_is_inactive():
    rule = SilenceRule(pipeline=None, metric_name=None,
                       expires_at=time.time() - 1)
    assert rule.is_active() is False


def test_rule_matches_all_when_fields_none():
    rule = SilenceRule(pipeline=None, metric_name=None, expires_at=None)
    assert rule.matches(make_metric(pipeline="any", name="any_metric")) is True


def test_rule_matches_specific_pipeline():
    rule = SilenceRule(pipeline="etl_main", metric_name=None, expires_at=None)
    assert rule.matches(make_metric(pipeline="etl_main")) is True
    assert rule.matches(make_metric(pipeline="other")) is False


def test_rule_matches_specific_metric_name():
    rule = SilenceRule(pipeline=None, metric_name="row_count", expires_at=None)
    assert rule.matches(make_metric(name="row_count")) is True
    assert rule.matches(make_metric(name="latency")) is False


def test_rule_matches_both_pipeline_and_metric():
    rule = SilenceRule(pipeline="etl_main", metric_name="row_count", expires_at=None)
    assert rule.matches(make_metric(pipeline="etl_main", name="row_count")) is True
    assert rule.matches(make_metric(pipeline="etl_main", name="latency")) is False
    assert rule.matches(make_metric(pipeline="other", name="row_count")) is False


def test_registry_silences_metric_with_active_rule():
    registry = SilenceRegistry()
    rule = SilenceRule(pipeline="etl_main", metric_name=None, expires_at=None)
    registry.add(rule)
    assert registry.is_silenced(make_metric(pipeline="etl_main")) is True


def test_registry_does_not_silence_unmatched_metric():
    registry = SilenceRegistry()
    rule = SilenceRule(pipeline="etl_main", metric_name=None, expires_at=None)
    registry.add(rule)
    assert registry.is_silenced(make_metric(pipeline="other")) is False


def test_registry_expired_rule_does_not_silence():
    registry = SilenceRegistry()
    rule = SilenceRule(pipeline=None, metric_name=None,
                       expires_at=time.time() - 1)
    registry.add(rule)
    assert registry.is_silenced(make_metric()) is False


def test_registry_prune_removes_expired():
    registry = SilenceRegistry()
    expired = SilenceRule(pipeline=None, metric_name=None,
                          expires_at=time.time() - 10)
    active = SilenceRule(pipeline=None, metric_name=None, expires_at=None)
    registry.add(expired)
    registry.add(active)
    removed = registry.prune_expired()
    assert removed == 1
    assert len(registry) == 1


def test_registry_active_rules_excludes_expired():
    registry = SilenceRegistry()
    registry.add(SilenceRule(pipeline=None, metric_name=None,
                              expires_at=time.time() - 5))
    registry.add(SilenceRule(pipeline="p", metric_name=None, expires_at=None))
    active = registry.active_rules()
    assert len(active) == 1
    assert active[0].pipeline == "p"


def test_rule_to_dict():
    rule = SilenceRule(pipeline="etl", metric_name="rows",
                       expires_at=1234567890.0, reason="maintenance")
    d = rule.to_dict()
    assert d["pipeline"] == "etl"
    assert d["metric_name"] == "rows"
    assert d["expires_at"] == 1234567890.0
    assert d["reason"] == "maintenance"
