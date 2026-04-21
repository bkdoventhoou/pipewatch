"""CLI for displaying SLA breach history from a live collector."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.sla import SLATarget, evaluate_sla
from pipewatch.sla_tracker import SLATracker


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show SLA breach history for monitored pipelines."
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    parser.add_argument(
        "--pipeline", default=None, help="Filter output to a specific pipeline"
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)
    collector = build_collector_from_config(config)
    tracker = SLATracker()

    sla_configs = config.get("sla", [])
    targets = [
        SLATarget(
            pipeline=s["pipeline"],
            metric_name=s["metric_name"],
            target_pct=s["target_pct"],
            window_seconds=s.get("window_seconds", 3600),
        )
        for s in sla_configs
    ]

    for target in targets:
        history = collector.get_history(target.pipeline, target.metric_name)
        if not history:
            continue
        result = evaluate_sla(target, history)
        tracker.record(result)

    breaches = tracker.all_breaches()
    if args.pipeline:
        breaches = [b for b in breaches if b.pipeline == args.pipeline]

    if args.format == "json":
        print(json.dumps([b.to_dict() for b in breaches], indent=2))
    else:
        if not breaches:
            print("No SLA breaches recorded.")
            return
        for b in breaches:
            delta = f"{b.delta_pct:+.1f}%"
            print(
                f"[BREACH] {b.pipeline}/{b.metric_name} "
                f"target={b.sla_target_pct:.1f}% actual={b.actual_pct:.1f}% "
                f"delta={delta} at={b.breached_at}"
            )


if __name__ == "__main__":
    main()
