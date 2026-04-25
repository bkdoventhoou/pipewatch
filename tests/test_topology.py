"""Tests for pipewatch.topology and pipewatch.cli_topology."""
from __future__ import annotations
import json
from unittest.mock import patch
import pytest
from pipewatch.topology import TopologyGraph, TopologyNode, build_topology_from_config
from pipewatch.cli_topology import main, parse_args


# ---------------------------------------------------------------------------
# TopologyGraph unit tests
# ---------------------------------------------------------------------------

def test_add_edge_creates_nodes():
    g = TopologyGraph()
    g.add_edge("ingest", "transform")
    assert "ingest" in g.all_pipelines()
    assert "transform" in g.all_pipelines()


def test_add_edge_sets_upstream_downstream():
    g = TopologyGraph()
    g.add_edge("ingest", "transform")
    assert "transform" in g.get("ingest").downstream
    assert "ingest" in g.get("transform").upstream


def test_no_cycle_in_dag():
    g = TopologyGraph()
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    assert g.has_cycle() is False


def test_cycle_detected():
    g = TopologyGraph()
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    g.add_edge("c", "a")
    assert g.has_cycle() is True


def test_get_unknown_returns_none():
    g = TopologyGraph()
    assert g.get("missing") is None


def test_to_dict_structure():
    g = TopologyGraph()
    g.add_edge("x", "y")
    d = g.to_dict()
    assert "x" in d
    assert d["x"]["downstream"] == ["y"]
    assert d["y"]["upstream"] == ["x"]


def test_build_topology_from_config():
    cfg = {"edges": [{"upstream": "src", "downstream": "dst"}]}
    g = build_topology_from_config(cfg)
    assert "src" in g.all_pipelines()
    assert "dst" in g.all_pipelines()


def test_build_topology_empty_config():
    g = build_topology_from_config({})
    assert g.all_pipelines() == []
    assert g.has_cycle() is False


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

def _mock_config(edges):
    return {"topology": {"edges": edges}}


def test_parse_args_defaults():
    args = parse_args([])
    assert args.config == "pipewatch.yaml"
    assert args.format == "text"
    assert args.pipeline is None


def test_parse_args_custom():
    args = parse_args(["--format", "json", "--pipeline", "ingest"])
    assert args.format == "json"
    assert args.pipeline == "ingest"


def test_main_text_output(capsys):
    cfg = _mock_config([{"upstream": "a", "downstream": "b"}])
    with patch("pipewatch.cli_topology.load_config", return_value=cfg):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "a" in out
    assert "b" in out
    assert "No cycles" in out


def test_main_json_output(capsys):
    cfg = _mock_config([{"upstream": "p1", "downstream": "p2"}])
    with patch("pipewatch.cli_topology.load_config", return_value=cfg):
        main(["--format", "json"])
    data = json.loads(capsys.readouterr().out)
    assert "cycle_detected" in data
    assert "nodes" in data
    assert data["cycle_detected"] is False


def test_main_cycle_warning(capsys):
    cfg = _mock_config([
        {"upstream": "a", "downstream": "b"},
        {"upstream": "b", "downstream": "a"},
    ])
    with patch("pipewatch.cli_topology.load_config", return_value=cfg):
        main(["--format", "text"])
    out = capsys.readouterr().out
    assert "WARNING" in out
