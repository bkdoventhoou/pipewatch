"""Metric enrichment: attach contextual metadata to PipelineMetric instances."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class EnrichedMetric:
    """A PipelineMetric decorated with extra contextual fields."""

    metric: PipelineMetric
    context: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        base = self.metric.to_dict()
        base["context"] = dict(self.context)
        return base

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.context.get(key, default)


@dataclass
class EnrichmentRule:
    """Maps a pipeline name pattern to a set of context key/value pairs."""

    pipeline: Optional[str]  # None means match all
    context: Dict[str, str] = field(default_factory=dict)

    def matches(self, metric: PipelineMetric) -> bool:
        if self.pipeline is None:
            return True
        return metric.pipeline == self.pipeline


class MetricEnricher:
    """Applies enrichment rules to metrics and returns EnrichedMetric objects."""

    def __init__(self) -> None:
        self._rules: List[EnrichmentRule] = []

    def add_rule(self, rule: EnrichmentRule) -> None:
        self._rules.append(rule)

    def enrich(self, metric: PipelineMetric) -> EnrichedMetric:
        merged: Dict[str, str] = {}
        for rule in self._rules:
            if rule.matches(metric):
                merged.update(rule.context)
        return EnrichedMetric(metric=metric, context=merged)

    def enrich_all(self, metrics: List[PipelineMetric]) -> List[EnrichedMetric]:
        return [self.enrich(m) for m in metrics]


def build_enricher_from_config(config: dict) -> MetricEnricher:
    """Construct a MetricEnricher from a config dict.

    Expected shape::

        enrichment:
          rules:
            - pipeline: ingest          # omit for wildcard
              context:
                team: data-eng
                env: production
    """
    enricher = MetricEnricher()
    rules_cfg = config.get("enrichment", {}).get("rules", [])
    for entry in rules_cfg:
        rule = EnrichmentRule(
            pipeline=entry.get("pipeline"),
            context=entry.get("context", {}),
        )
        enricher.add_rule(rule)
    return enricher
