"""Tests for pipewatch.enrichment."""
from __future__ import annotations

import pytest

from pipewatch.enrichment import (
    EnrichmentRule,
    EnrichedMetric,
    MetricEnricher,
    build_enricher_from_config,
)
from pipewatch.metrics import MetricStatus, PipelineMetric


def make_metric(
    pipeline: str = "ingest",
    name: str = "row_count",
    value: float = 100.0,
    status: MetricStatus = MetricStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(pipeline=pipeline, name=name, value=value, status=status)


# ---------------------------------------------------------------------------
# EnrichmentRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_all_when_pipeline_is_none():
    rule = EnrichmentRule(pipeline=None, context={"env": "prod"})
    assert rule.matches(make_metric(pipeline="ingest"))
    assert rule.matches(make_metric(pipeline="transform"))


def test_rule_matches_specific_pipeline():
    rule = EnrichmentRule(pipeline="ingest", context={"team": "data"})
    assert rule.matches(make_metric(pipeline="ingest"))
    assert not rule.matches(make_metric(pipeline="transform"))


# ---------------------------------------------------------------------------
# MetricEnricher.enrich
# ---------------------------------------------------------------------------

def test_enrich_no_rules_returns_empty_context():
    enricher = MetricEnricher()
    result = enricher.enrich(make_metric())
    assert isinstance(result, EnrichedMetric)
    assert result.context == {}


def test_enrich_wildcard_rule_applies_to_all():
    enricher = MetricEnricher()
    enricher.add_rule(EnrichmentRule(pipeline=None, context={"env": "staging"}))
    result = enricher.enrich(make_metric(pipeline="transform"))
    assert result.get("env") == "staging"


def test_enrich_pipeline_specific_rule():
    enricher = MetricEnricher()
    enricher.add_rule(EnrichmentRule(pipeline="ingest", context={"owner": "alice"}))
    enricher.add_rule(EnrichmentRule(pipeline="transform", context={"owner": "bob"}))

    ingest_result = enricher.enrich(make_metric(pipeline="ingest"))
    transform_result = enricher.enrich(make_metric(pipeline="transform"))

    assert ingest_result.get("owner") == "alice"
    assert transform_result.get("owner") == "bob"


def test_enrich_later_rules_override_earlier():
    enricher = MetricEnricher()
    enricher.add_rule(EnrichmentRule(pipeline=None, context={"env": "prod"}))
    enricher.add_rule(EnrichmentRule(pipeline="ingest", context={"env": "staging"}))
    result = enricher.enrich(make_metric(pipeline="ingest"))
    assert result.get("env") == "staging"


def test_enrich_all_returns_list():
    enricher = MetricEnricher()
    enricher.add_rule(EnrichmentRule(pipeline=None, context={"x": "1"}))
    metrics = [make_metric(pipeline="a"), make_metric(pipeline="b")]
    results = enricher.enrich_all(metrics)
    assert len(results) == 2
    assert all(r.get("x") == "1" for r in results)


# ---------------------------------------------------------------------------
# EnrichedMetric.to_dict
# ---------------------------------------------------------------------------

def test_to_dict_includes_context():
    enricher = MetricEnricher()
    enricher.add_rule(EnrichmentRule(pipeline=None, context={"env": "prod"}))
    result = enricher.enrich(make_metric())
    d = result.to_dict()
    assert "context" in d
    assert d["context"]["env"] == "prod"
    assert "pipeline" in d
    assert "name" in d


# ---------------------------------------------------------------------------
# build_enricher_from_config
# ---------------------------------------------------------------------------

def test_build_enricher_from_config_empty():
    enricher = build_enricher_from_config({})
    result = enricher.enrich(make_metric())
    assert result.context == {}


def test_build_enricher_from_config_with_rules():
    config = {
        "enrichment": {
            "rules": [
                {"pipeline": "ingest", "context": {"team": "data-eng", "env": "prod"}},
                {"context": {"region": "us-east-1"}},
            ]
        }
    }
    enricher = build_enricher_from_config(config)
    result = enricher.enrich(make_metric(pipeline="ingest"))
    assert result.get("team") == "data-eng"
    assert result.get("region") == "us-east-1"
