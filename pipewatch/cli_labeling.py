"""CLI entry-point: pipewatch label — display metrics with their labels."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.config import load_config
from pipewatch.labeling import LabelRegistry
from pipewatch.metrics import MetricStatus


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pipewatch-label",
        description="Display pipeline metrics annotated with labels.",
    )
    p.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    p.add_argument("--label", metavar="KEY=VALUE", action="append", default=[],
                   help="Filter by label (may be repeated)")
    p.add_argument("--format", choices=["text", "json"], default="text")
    return p.parse_args(argv)


def _parse_label_filter(raw: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in raw:
        if "=" in item:
            k, v = item.split("=", 1)
            result[k.strip()] = v.strip()
        else:
            result[item.strip()] = ""
    return result


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)

    collector = MetricCollector()
    for pipeline, metrics_cfg in config.get("pipelines", {}).items():
        for name, cfg in metrics_cfg.items():
            collector.record(pipeline, name, cfg.get("value", 0.0))

    registry = LabelRegistry()
    label_defs = config.get("labels", {})
    for metric in collector.all_latest():
        key = f"{metric.pipeline}.{metric.name}"
        labels = label_defs.get(key, {})
        registry.label(metric, **labels)

    filters = _parse_label_filter(args.label)
    results = registry.all()
    for k, v in filters.items():
        results = [lm for lm in results if lm.has_label(k, v or None)]

    if args.format == "json":
        print(json.dumps([lm.to_dict() for lm in results], indent=2))
    else:
        for lm in results:
            label_str = ", ".join(f"{k}={v}" for k, v in lm.labels.items()) or "(none)"
            status = lm.metric.status.value.upper()
            print(f"[{status}] {lm.metric.pipeline}/{lm.metric.name} "
                  f"= {lm.metric.value}  labels: {label_str}")


if __name__ == "__main__":  # pragma: no cover
    main()
