"""CLI entry point for aggregating pipeline metric history."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.config import load_config
from pipewatch.cli_report import build_collector_from_config
from pipewatch.aggregator import aggregate_metrics


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Aggregate pipeline metric history from collector."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument("--metric", default=None, help="Filter by metric name")
    parser.add_argument("--format", choices=["json", "text"], default="text", dest="fmt")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)

    all_metrics = []
    for pipeline_name, history in collector._history.items():
        for entry in history:
            all_metrics.append(entry)

    if args.pipeline:
        all_metrics = [m for m in all_metrics if m.pipeline == args.pipeline]
    if args.metric:
        all_metrics = [m for m in all_metrics if m.name == args.metric]

    aggregated = aggregate_metrics(all_metrics)

    if args.fmt == "json":
        print(json.dumps([a.to_dict() for a in aggregated], indent=2))
    else:
        if not aggregated:
            print("No metrics to aggregate.")
            return
        for a in aggregated:
            print(f"[{a.pipeline}] {a.metric_name}: count={a.count} "
                  f"min={a.min_value} max={a.max_value} "
                  f"mean={a.mean_value} median={a.median_value} "
                  f"statuses={a.statuses}")


if __name__ == "__main__":
    main()
