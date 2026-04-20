"""CLI for inspecting and resetting alert rate-limit state."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.config import load_config
from pipewatch.collector import MetricCollector
from pipewatch.ratelimit import AlertRateLimiter
from pipewatch.metrics import MetricStatus


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-ratelimit",
        description="Inspect or reset alert rate-limit counters.",
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument("--window", type=int, default=300, help="Rate-limit window in seconds")
    parser.add_argument("--max-alerts", type=int, default=3, help="Max alerts per window")
    parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    parser.add_argument("--metric", default=None, help="Filter by metric name")
    parser.add_argument(
        "--reset", action="store_true", help="Reset matching rate-limit counters"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    config = load_config(args.config)

    window = config.get("rate_limit", {}).get("window_seconds", args.window)
    max_alerts = config.get("rate_limit", {}).get("max_alerts", args.max_alerts)

    limiter = AlertRateLimiter(window_seconds=window, max_alerts=max_alerts)

    # Simulate current metrics to populate state
    collector = MetricCollector()
    for name, cfg in config.get("thresholds", {}).items():
        collector.add_threshold(name, cfg.get("warning"), cfg.get("critical"))

    if args.reset:
        limiter.reset(pipeline=args.pipeline, metric_name=args.metric)
        print("Rate-limit counters reset.", file=sys.stderr)
        return

    entries = limiter.status()
    if args.pipeline:
        entries = [e for e in entries if e["pipeline"] == args.pipeline]
    if args.metric:
        entries = [e for e in entries if e["metric_name"] == args.metric]

    if args.format == "json":
        print(json.dumps(entries, indent=2))
    else:
        if not entries:
            print("No rate-limit entries found.")
            return
        header = f"{'PIPELINE':<20} {'METRIC':<20} {'IN WINDOW':>10} {'MAX':>5} {'WINDOW(s)':>10}"
        print(header)
        print("-" * len(header))
        for e in entries:
            print(
                f"{e['pipeline']:<20} {e['metric_name']:<20}"
                f" {e['alert_count_in_window']:>10} {e['max_alerts']:>5}"
                f" {e['window_seconds']:>10}"
            )


if __name__ == "__main__":
    main()
