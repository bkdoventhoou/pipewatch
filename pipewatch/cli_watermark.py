"""CLI entry point for high/low watermark reporting."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config
from pipewatch.watermark import track_watermarks


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Show high/low watermarks for pipeline metrics."
    )
    p.add_argument(
        "--config", default="pipewatch.yaml", help="Path to config file."
    )
    p.add_argument(
        "--pipeline", default=None, help="Filter to a specific pipeline."
    )
    p.add_argument(
        "--metric", default=None, help="Filter to a specific metric name."
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)

    all_metrics = [
        m
        for metrics in collector.get_history().values()
        for m in metrics
    ]

    tracker = track_watermarks(all_metrics)
    entries = tracker.all_entries()

    if args.pipeline:
        entries = [e for e in entries if e.pipeline == args.pipeline]
    if args.metric:
        entries = [e for e in entries if e.metric_name == args.metric]

    if not entries:
        print("No watermark data available.")
        sys.exit(0)

    if args.format == "json":
        print(json.dumps([e.to_dict() for e in entries], indent=2))
    else:
        for e in entries:
            print(
                f"[{e.pipeline}] {e.metric_name}: "
                f"high={e.high:.4f} (ts={e.high_ts:.0f})  "
                f"low={e.low:.4f} (ts={e.low_ts:.0f})"
            )


if __name__ == "__main__":
    main()
