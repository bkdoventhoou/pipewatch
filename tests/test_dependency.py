"""Tests for pipewatch.dependency."""
import pytest
from pipewatch.dependency import (
    DependencyGraph,
    DependencyNode,
    PropagatedStatus,
    propagate_status,
)
from pipewatch.metrics import MetricStatus


def test_graph_add_and_get():
    g = DependencyGraph()
    g.add("etl_a", ["etl_b"])
    node = g.get("etl_a")
    assert node is not None
    assert node.pipeline == "etl_a"
    assert node.depends_on == ["etl_b"]


def test_graph_get_unknown_returns_none():
    g = DependencyGraph()
    assert g.get("missing") is None


def test_graph_all_pipelines():
    g = DependencyGraph()
    g.add("a")
    g.add("b")
    assert set(g.all_pipelines()) == {"a", "b"}


def test_graph_to_dict():
    g = DependencyGraph()
    g.add("a", ["b"])
    d = g.to_dict()
    assert d["a"]["depends_on"] == ["b"]


def test_propagate_no_deps_own_status_preserved():
    g = DependencyGraph()
    g.add("a", [])
    statuses = {"a": MetricStatus.WARNING}
    result = propagate_status(g, statuses)
    assert result["a"].own_status == MetricStatus.WARNING
    assert result["a"].propagated_status == MetricStatus.WARNING
    assert result["a"].blocking_pipelines == []


def test_propagate_critical_upstream_escalates():
    g = DependencyGraph()
    g.add("upstream", [])
    g.add("downstream", ["upstream"])
    statuses = {"upstream": MetricStatus.CRITICAL, "downstream": MetricStatus.OK}
    result = propagate_status(g, statuses)
    assert result["downstream"].propagated_status == MetricStatus.CRITICAL
    assert "upstream" in result["downstream"].blocking_pipelines


def test_propagate_ok_upstream_does_not_change_ok_downstream():
    g = DependencyGraph()
    g.add("upstream", [])
    g.add("downstream", ["upstream"])
    statuses = {"upstream": MetricStatus.OK, "downstream": MetricStatus.OK}
    result = propagate_status(g, statuses)
    assert result["downstream"].propagated_status == MetricStatus.OK
    assert result["downstream"].blocking_pipelines == []


def test_propagate_own_critical_beats_warning_upstream():
    g = DependencyGraph()
    g.add("up", [])
    g.add("down", ["up"])
    statuses = {"up": MetricStatus.WARNING, "down": MetricStatus.CRITICAL}
    result = propagate_status(g, statuses)
    assert result["down"].propagated_status == MetricStatus.CRITICAL


def test_propagate_missing_dep_status_defaults_to_ok():
    g = DependencyGraph()
    g.add("a", ["ghost"])
    statuses = {"a": MetricStatus.OK}
    result = propagate_status(g, statuses)
    assert result["a"].propagated_status == MetricStatus.OK
    assert result["a"].blocking_pipelines == []


def test_propagated_status_to_dict():
    ps = PropagatedStatus(
        pipeline="p",
        own_status=MetricStatus.OK,
        propagated_status=MetricStatus.WARNING,
        blocking_pipelines=["q"],
    )
    d = ps.to_dict()
    assert d["pipeline"] == "p"
    assert d["own_status"] == "ok"
    assert d["propagated_status"] == "warning"
    assert d["blocking_pipelines"] == ["q"]
