"""Snapshot: capture and compare pipeline metric states at a point in time."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SnapshotEntry:
    pipeline: str
    metric_name: str
    value: float
    status: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric_name": self.metric_name,
            "value": self.value,
            "status": self.status,
        }


@dataclass
class PipelineSnapshot:
    captured_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    entries: List[SnapshotEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "captured_at": self.captured_at,
            "entries": [e.to_dict() for e in self.entries],
        }


def capture_snapshot(metrics: List[PipelineMetric]) -> PipelineSnapshot:
    snap = PipelineSnapshot()
    for m in metrics:
        snap.entries.append(
            SnapshotEntry(
                pipeline=m.pipeline,
                metric_name=m.name,
                value=m.value,
                status=m.status.value,
            )
        )
    return snap


def save_snapshot(snap: PipelineSnapshot, path: str) -> None:
    Path(path).write_text(json.dumps(snap.to_dict(), indent=2))


def load_snapshot(path: str) -> PipelineSnapshot:
    data = json.loads(Path(path).read_text())
    snap = PipelineSnapshot(captured_at=data["captured_at"])
    for e in data["entries"]:
        snap.entries.append(SnapshotEntry(**e))
    return snap


def diff_snapshots(old: PipelineSnapshot, new: PipelineSnapshot) -> List[Dict]:
    """Return entries where status changed between snapshots."""
    old_map = {(e.pipeline, e.metric_name): e for e in old.entries}
    diffs = []
    for e in new.entries:
        key = (e.pipeline, e.metric_name)
        prev = old_map.get(key)
        if prev and prev.status != e.status:
            diffs.append({
                "pipeline": e.pipeline,
                "metric_name": e.metric_name,
                "old_status": prev.status,
                "new_status": e.status,
                "old_value": prev.value,
                "new_value": e.value,
            })
    return diffs
