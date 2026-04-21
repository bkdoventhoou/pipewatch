"""Checkpoint tracking for pipeline runs — record and compare pipeline execution state."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class CheckpointEntry:
    pipeline: str
    run_id: str
    timestamp: float
    metric_count: int
    ok_count: int
    warning_count: int
    critical_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "metric_count": self.metric_count,
            "ok_count": self.ok_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
        }


@dataclass
class CheckpointStore:
    entries: Dict[str, List[CheckpointEntry]] = field(default_factory=dict)

    def record(self, entry: CheckpointEntry) -> None:
        self.entries.setdefault(entry.pipeline, []).append(entry)

    def latest(self, pipeline: str) -> Optional[CheckpointEntry]:
        history = self.entries.get(pipeline, [])
        return history[-1] if history else None

    def history(self, pipeline: str) -> List[CheckpointEntry]:
        return list(self.entries.get(pipeline, []))


def build_checkpoint(pipeline: str, run_id: str, metrics: list) -> CheckpointEntry:
    from pipewatch.metrics import MetricStatus

    ok = sum(1 for m in metrics if m.status == MetricStatus.OK)
    warning = sum(1 for m in metrics if m.status == MetricStatus.WARNING)
    critical = sum(1 for m in metrics if m.status == MetricStatus.CRITICAL)
    return CheckpointEntry(
        pipeline=pipeline,
        run_id=run_id,
        timestamp=time.time(),
        metric_count=len(metrics),
        ok_count=ok,
        warning_count=warning,
        critical_count=critical,
    )


def save_checkpoint_store(store: CheckpointStore, path: str) -> None:
    data = {
        pipeline: [e.to_dict() for e in entries]
        for pipeline, entries in store.entries.items()
    }
    Path(path).write_text(json.dumps(data, indent=2))


def load_checkpoint_store(path: str) -> CheckpointStore:
    raw = json.loads(Path(path).read_text())
    store = CheckpointStore()
    for pipeline, entries in raw.items():
        for e in entries:
            store.record(CheckpointEntry(**e))
    return store
