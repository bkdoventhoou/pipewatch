"""Persistence layer for saving and loading metric history to/from disk."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.metrics import PipelineMetric, MetricStatus


@dataclass
class PersistenceRecord:
    pipeline: str
    metric_name: str
    value: float
    status: str
    timestamp: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "status": self.status,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def from_dict(data: dict) -> "PersistenceRecord":
        return PersistenceRecord(
            pipeline=data["pipeline"],
            metric_name=data["metric_name"],
            value=data["value"],
            status=data["status"],
            timestamp=data["timestamp"],
        )


def save_history(history: Dict[str, List[PipelineMetric]], path: str) -> None:
    """Serialize metric history to a JSON file."""
    records: List[dict] = []
    for key, metrics in history.items():
        for m in metrics:
            records.append(
                PersistenceRecord(
                    pipeline=m.pipeline,
                    metric_name=m.name,
                    value=m.value,
                    status=m.status.value,
                    timestamp=m.timestamp,
                ).to_dict()
            )
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w") as fh:
        json.dump(records, fh, indent=2)


def load_history(path: str) -> Dict[str, List[PipelineMetric]]:
    """Deserialize metric history from a JSON file."""
    if not os.path.exists(path):
        return {}
    with open(path) as fh:
        raw = json.load(fh)
    history: Dict[str, List[PipelineMetric]] = {}
    for item in raw:
        rec = PersistenceRecord.from_dict(item)
        key = f"{rec.pipeline}:{rec.metric_name}"
        metric = PipelineMetric(
            pipeline=rec.pipeline,
            name=rec.metric_name,
            value=rec.value,
            status=MetricStatus(rec.status),
            timestamp=rec.timestamp,
        )
        history.setdefault(key, []).append(metric)
    return history
