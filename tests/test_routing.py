"""Tests for pipewatch.routing."""

import pytest
from pipewatch.metrics import MetricStatus, PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.routing import AlertRouter, RoutingRule, build_router_from_config


def make_metric(
    pipeline="pipe_a",
    name="row_count",
    value=10.0,
    status=MetricStatus.WARNING,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline, name=name, value=value, status=status
    )


def make_alert(metric: PipelineMetric) -> Alert:
    return Alert(metric=metric, message=f"{metric.status.name}: {metric.name}")


# --- RoutingRule.matches ---

def test_rule_matches_any_pipeline_when_unset():
    rule = RoutingRule(handler_name="h", min_status=MetricStatus.WARNING)
    alert = make_alert(make_metric(status=MetricStatus.WARNING))
    assert rule.matches(alert)


def test_rule_filters_by_pipeline():
    rule = RoutingRule(handler_name="h", pipeline="pipe_b")
    alert = make_alert(make_metric(pipeline="pipe_a", status=MetricStatus.WARNING))
    assert not rule.matches(alert)


def test_rule_filters_by_metric_name():
    rule = RoutingRule(handler_name="h", metric_name="latency")
    alert = make_alert(make_metric(name="row_count", status=MetricStatus.WARNING))
    assert not rule.matches(alert)


def test_rule_min_status_blocks_ok():
    rule = RoutingRule(handler_name="h", min_status=MetricStatus.WARNING)
    alert = make_alert(make_metric(status=MetricStatus.OK))
    assert not rule.matches(alert)


def test_rule_min_status_critical_blocks_warning():
    rule = RoutingRule(handler_name="h", min_status=MetricStatus.CRITICAL)
    alert = make_alert(make_metric(status=MetricStatus.WARNING))
    assert not rule.matches(alert)


def test_rule_min_status_critical_allows_critical():
    rule = RoutingRule(handler_name="h", min_status=MetricStatus.CRITICAL)
    alert = make_alert(make_metric(status=MetricStatus.CRITICAL))
    assert rule.matches(alert)


# --- AlertRouter ---

def test_router_dispatches_to_matching_handler():
    received = []
    router = AlertRouter()
    router.register_handler("slack", lambda a: received.append(a))
    router.add_rule(RoutingRule(handler_name="slack"))
    alert = make_alert(make_metric(status=MetricStatus.WARNING))
    dispatched = router.route(alert)
    assert dispatched == ["slack"]
    assert len(received) == 1


def test_router_skips_unregistered_handler():
    router = AlertRouter()
    router.add_rule(RoutingRule(handler_name="missing"))
    alert = make_alert(make_metric(status=MetricStatus.WARNING))
    dispatched = router.route(alert)
    assert dispatched == []


def test_router_multiple_rules_multiple_handlers():
    calls = {"a": [], "b": []}
    router = AlertRouter()
    router.register_handler("a", lambda alert: calls["a"].append(alert))
    router.register_handler("b", lambda alert: calls["b"].append(alert))
    router.add_rule(RoutingRule(handler_name="a"))
    router.add_rule(RoutingRule(handler_name="b", min_status=MetricStatus.CRITICAL))
    warning_alert = make_alert(make_metric(status=MetricStatus.WARNING))
    router.route(warning_alert)
    assert len(calls["a"]) == 1
    assert len(calls["b"]) == 0


# --- build_router_from_config ---

def test_build_router_from_config_creates_rules():
    config = {
        "routing": [
            {"handler": "console", "min_status": "WARNING"},
            {"handler": "file", "pipeline": "pipe_a", "min_status": "CRITICAL"},
        ]
    }
    router = build_router_from_config(config)
    assert len(router.rules()) == 2


def test_build_router_from_config_empty():
    router = build_router_from_config({})
    assert router.rules() == []


def test_rule_to_dict():
    rule = RoutingRule(
        handler_name="pagerduty",
        pipeline="etl",
        metric_name="latency",
        min_status=MetricStatus.CRITICAL,
    )
    d = rule.to_dict()
    assert d["handler_name"] == "pagerduty"
    assert d["pipeline"] == "etl"
    assert d["min_status"] == "CRITICAL"
