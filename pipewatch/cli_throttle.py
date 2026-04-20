"""CLI command: show current alert throttle state."""

from __future__ import annotations

import argparse
import json

from pipewatch.config import load_config
from pipewatch.collector import MetricCollector
from pipewatch.throttle import AlertThrottle
from pipewatch.metrics import MetricStatus


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch throttle",
        description="Display alert throttle statistics.",
    )
    parser.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=None,
        help="Cooldown window in seconds (overrides config).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="fmt",
        help="Output format.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    cooldown = args.cooldown if args.cooldown is not None else cfg.get("throttle_cooldown", 300.0)
    throttle = AlertThrottle(cooldown_seconds=float(cooldown))

    # Simulate populating throttle from collector history
    collector = MetricCollector()
    from pipewatch.cli_report import build_collector_from_config
    collector = build_collector_from_config(cfg)

    for key, history in collector.get_all_history().items():
        for metric in history:
            if metric.status != MetricStatus.OK:
                if throttle.should_fire(metric):
                    throttle.record(metric)

    stats = throttle.stats()

    if args.fmt == "json":
        print(json.dumps(stats, indent=2))
    else:
        if not stats:
            print("No active throttle entries.")
            return
        print(f"{'Key':<55} {'Fires':>6}  {'Last Fired (mono s)':>20}")
        print("-" * 85)
        for key, entry in stats.items():
            print(f"{key:<55} {entry['fire_count']:>6}  {entry['last_fired']:>20.2f}")
