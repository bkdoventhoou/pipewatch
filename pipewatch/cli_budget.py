"""CLI for inspecting alert budget usage per pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from pipewatch.budget import AlertBudget
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipewatch-budget",
        description="Show alert budget usage per pipeline.",
    )
    parser.add_argument("--config", default="pipewatch.yaml", help="Config file path")
    parser.add_argument(
        "--max-alerts", type=int, default=None,
        help="Override max alerts per window (default from config or 10)",
    )
    parser.add_argument(
        "--window", type=int, default=None,
        help="Override window in seconds (default from config or 3600)",
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    budget_cfg = cfg.get("budget", {})
    max_alerts = args.max_alerts or budget_cfg.get("max_alerts", 10)
    window_seconds = args.window or budget_cfg.get("window_seconds", 3600)

    collector = build_collector_from_config(args.config)
    budget = AlertBudget(max_alerts=max_alerts, window_seconds=window_seconds)

    now = datetime.utcnow()
    for key, history in collector.get_history().items():
        for metric in history:
            budget.allow(metric, now=metric.timestamp if hasattr(metric, "timestamp") else now)

    summary = budget.summary(now=now)

    if not summary:
        print("No alert budget usage recorded.")
        return

    if args.fmt == "json":
        print(json.dumps(summary, indent=2))
    else:
        print(f"Alert Budget  (max={max_alerts} / {window_seconds}s window)")
        print(f"  {'Pipeline':<30} {'Used':>6} {'Remaining':>10}")
        print("  " + "-" * 50)
        for pipeline, info in sorted(summary.items()):
            print(f"  {pipeline:<30} {info['used']:>6} {info['remaining']:>10}")


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
