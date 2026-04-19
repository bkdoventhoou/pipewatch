"""CLI for baseline comparison commands."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.baseline import (
    load_baseline,
    save_baseline,
    compare_to_baseline,
    build_baseline_from_metrics,
)
from pipewatch.cli_report import build_collector_from_config
from pipewatch.config import load_config


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Baseline comparison for pipeline metrics")
    sub = parser.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="Capture current metrics as baseline")
    cap.add_argument("--config", default="pipewatch.yaml")
    cap.add_argument("--output", default="baseline.json")

    cmp = sub.add_parser("compare", help="Compare current metrics to baseline")
    cmp.add_argument("--config", default="pipewatch.yaml")
    cmp.add_argument("--baseline", default="baseline.json")
    cmp.add_argument("--format", choices=["text", "json"], default="text")

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(args.config)
    collector = build_collector_from_config(cfg)
    metrics = collector.get_latest()

    if args.command == "capture":
        entries = build_baseline_from_metrics(metrics)
        save_baseline(entries, args.output)
        print(f"Baseline saved to {args.output} ({len(entries)} entries)")

    elif args.command == "compare":
        baseline = load_baseline(args.baseline)
        comparisons = compare_to_baseline(metrics, baseline)
        if not comparisons:
            print("No baseline matches found.", file=sys.stderr)
            return
        if args.format == "json":
            print(json.dumps([c.to_dict() for c in comparisons], indent=2))
        else:
            for c in comparisons:
                sign = "+" if c.delta >= 0 else ""
                pct_str = f" ({sign}{c.pct_change}%)" if c.pct_change is not None else ""
                print(
                    f"{c.pipeline}/{c.metric_name}: "
                    f"current={c.current_value} baseline={c.baseline_value} "
                    f"delta={sign}{c.delta}{pct_str}"
                )


if __name__ == "__main__":
    main()
